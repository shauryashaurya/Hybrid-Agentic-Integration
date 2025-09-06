# module_2_csv_introspection_v2.py
# Module 2: Building a Production CSV Introspection Agent
# Version 2: Supports multiple LLM backends (Ollama, OpenAI, Anthropic)

import os
import csv
import json
import re
import chardet
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import pandas as pd
import numpy as np
from collections import Counter

# Import our flexible LLM interface
from llm_providers import llm_interface, simple_llm_call, llm_json_call

# Part 1: Advanced CSV Detection and Reading
# Handle different encodings, delimiters, and formats


class CSVIntrospector:
    # Comprehensive CSV analysis agent

    def __init__(self):
        self.encoding = None
        self.delimiter = None
        self.headers = []
        self.sample_data = []
        self.schema = {}
        self.issues = []
        self.metadata = {}

    def detect_encoding(self, file_path: str) -> str:
        # Detect file encoding automatically
        with open(file_path, 'rb') as file:
            raw_data = file.read(10000)  # Read first 10KB
            result = chardet.detect(raw_data)
            confidence = result['confidence']
            encoding = result['encoding']

            # Store metadata
            self.metadata['encoding_confidence'] = confidence

            if confidence < 0.7:
                self.issues.append(
                    f"Low encoding confidence: {confidence:.2f}")

            return encoding or 'utf-8'

    def detect_delimiter(self, file_path: str, encoding: str) -> str:
        # Intelligently detect CSV delimiter
        with open(file_path, 'r', encoding=encoding, errors='ignore') as file:
            sample = file.read(5000)

            # Use csv.Sniffer for initial detection
            try:
                sniffer = csv.Sniffer()
                delimiter = sniffer.sniff(sample).delimiter
            except:
                # Fallback to frequency analysis
                delimiters = [',', '\t', '|', ';']
                delimiter_counts = {d: sample.count(d) for d in delimiters}
                delimiter = max(delimiter_counts, key=delimiter_counts.get)

            return delimiter

    def read_csv_robust(self, file_path: str) -> pd.DataFrame:
        # Robust CSV reading with multiple fallback strategies

        # Step 1: Detect encoding
        self.encoding = self.detect_encoding(file_path)
        print(
            f"  Detected encoding: {self.encoding} (confidence: {self.metadata['encoding_confidence']:.2f})")

        # Step 2: Detect delimiter
        self.delimiter = self.detect_delimiter(file_path, self.encoding)
        delimiter_display = 'tab' if self.delimiter == '\t' else self.delimiter
        print(f"  Detected delimiter: '{delimiter_display}'")

        # Step 3: Try reading with detected parameters
        try:
            df = pd.read_csv(
                file_path,
                encoding=self.encoding,
                delimiter=self.delimiter,
                on_bad_lines='warn',
                engine='python'
            )

            # Store headers and samples
            self.headers = list(df.columns)
            self.sample_data = df.head(100)

            return df

        except Exception as e:
            self.issues.append(f"Error reading CSV: {str(e)}")

            # Try alternative approaches
            try:
                # Try with different encoding
                df = pd.read_csv(file_path, encoding='latin-1',
                                 delimiter=self.delimiter)
                self.encoding = 'latin-1'
                self.issues.append("Fallback to latin-1 encoding")
                return df
            except:
                # Last resort - read as text and parse manually
                return self.manual_parse(file_path)

    def manual_parse(self, file_path: str) -> pd.DataFrame:
        # Manual parsing for problematic files
        rows = []
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            for i, line in enumerate(file):
                if i == 0:
                    self.headers = line.strip().split(self.delimiter)
                else:
                    rows.append(line.strip().split(self.delimiter))
                if i > 100:  # Limit for samples
                    break

        return pd.DataFrame(rows, columns=self.headers)

# Part 2: Header Analysis with AI


class HeaderAnalyzer:
    # Analyze and standardize CSV headers using AI

    def __init__(self):
        self.original_headers = []
        self.standardized_headers = []
        self.header_mappings = {}
        self.header_types = {}
        self.use_ai = True  # Flag to enable/disable AI features

    def analyze_header_patterns(self, headers: List[str]) -> Dict[str, Any]:
        # Detect naming conventions and patterns in headers

        patterns = {
            'snake_case': 0,
            'camelCase': 0,
            'PascalCase': 0,
            'UPPER_CASE': 0,
            'mixed': 0
        }

        for header in headers:
            if header.islower() and '_' in header:
                patterns['snake_case'] += 1
            elif header[0].islower() and any(c.isupper() for c in header[1:]):
                patterns['camelCase'] += 1
            elif header[0].isupper() and any(c.isupper() for c in header[1:]):
                patterns['PascalCase'] += 1
            elif header.isupper():
                patterns['UPPER_CASE'] += 1
            else:
                patterns['mixed'] += 1

        # Determine dominant pattern
        dominant = max(patterns, key=patterns.get)

        return {
            'patterns': patterns,
            'dominant': dominant,
            'consistency': patterns[dominant] / len(headers) if headers else 0
        }

    def standardize_header(self, header: str, target_format: str = 'snake_case') -> str:
        # Convert header to target format

        # Remove special characters
        cleaned = re.sub(r'[^\w\s]', '', header)

        # Split into words
        words = re.findall(r'[A-Z]?[a-z]+|[A-Z]+(?=[A-Z][a-z]|\b)', cleaned)
        if not words:
            words = cleaned.split()

        # Convert to target format
        if target_format == 'snake_case':
            return '_'.join(words).lower()
        elif target_format == 'camelCase':
            return words[0].lower() + ''.join(w.capitalize() for w in words[1:])
        elif target_format == 'PascalCase':
            return ''.join(w.capitalize() for w in words)
        else:
            return '_'.join(words).lower()  # Default to snake_case

    def infer_semantic_type_with_ai(self, header: str, samples: List[Any]) -> str:
        # Use AI to understand semantic meaning

        if not self.use_ai:
            return self.infer_semantic_type_rule_based(header, samples)

        samples_str = ', '.join([str(s) for s in samples[:5]])

        prompt = f"""
        Analyze this database column:
        Header: {header}
        Samples: {samples_str}
        
        Return a JSON with:
        - semantic_type: one of [identifier, timestamp, monetary, quantity, status, text, email, phone, location, general]
        - confidence: 0-1 score
        
        JSON only:"""

        result = llm_json_call(prompt, temperature=0.1)

        if result and 'semantic_type' in result:
            return result['semantic_type']
        else:
            # Fallback to rule-based
            return self.infer_semantic_type_rule_based(header, samples)

    def infer_semantic_type_rule_based(self, header: str, samples: List[Any]) -> str:
        # Rule-based semantic type inference

        header_lower = header.lower()

        # Check header keywords
        if any(keyword in header_lower for keyword in ['id', 'key', 'code', 'number']):
            return 'identifier'
        elif any(keyword in header_lower for keyword in ['date', 'time', 'created', 'updated']):
            return 'timestamp'
        elif any(keyword in header_lower for keyword in ['amount', 'price', 'cost', 'total']):
            return 'monetary'
        elif any(keyword in header_lower for keyword in ['quantity', 'qty', 'count']):
            return 'quantity'
        elif any(keyword in header_lower for keyword in ['status', 'state', 'flag']):
            return 'status'
        elif any(keyword in header_lower for keyword in ['email', 'mail']):
            return 'email'
        elif any(keyword in header_lower for keyword in ['phone', 'tel']):
            return 'phone'
        elif any(keyword in header_lower for keyword in ['address', 'city', 'country']):
            return 'location'

        return 'general'

    def create_header_mapping(self, headers: List[str], sample_data: pd.DataFrame) -> Dict[str, Any]:
        # Create comprehensive header mapping with metadata

        self.original_headers = headers
        pattern_analysis = self.analyze_header_patterns(headers)

        mappings = []
        for header in headers:
            samples = sample_data[header].dropna().head(
                5).tolist() if header in sample_data.columns else []

            # Try AI-powered understanding
            semantic_type = self.infer_semantic_type_with_ai(header, samples)

            mapping = {
                'original': header,
                'standardized': self.standardize_header(header),
                'semantic_type': semantic_type,
                'samples': samples[:3]
            }
            mappings.append(mapping)

            self.header_mappings[header] = mapping['standardized']
            self.header_types[header] = semantic_type

        return {
            'pattern_analysis': pattern_analysis,
            'mappings': mappings,
            'total_headers': len(headers)
        }

# Part 3: Enhanced Schema Extraction with AI


class SchemaExtractor:
    # Extract and document complete schema from CSV

    def __init__(self, use_ai: bool = True):
        self.schema = {}
        self.relationships = []
        self.constraints = []
        self.quality_issues = []
        self.use_ai = use_ai

    def extract_schema_with_ai(self, df: pd.DataFrame) -> Dict[str, Any]:
        # Use AI to understand the overall schema

        if not self.use_ai:
            return {}

        # Prepare data summary for AI
        column_info = []
        for col in df.columns[:10]:  # Limit to first 10 columns for prompt size
            samples = df[col].dropna().head(3).tolist()
            column_info.append(f"{col}: {samples}")

        prompt = f"""
        Analyze this dataset schema:
        
        Columns and samples:
        {chr(10).join(column_info)}
        
        Row count: {len(df)}
        
        Return JSON with:
        - dataset_type: what kind of business data this is
        - potential_primary_key: most likely primary key column
        - foreign_key_candidates: columns that might be foreign keys (as list)
        - data_quality_concerns: potential issues (as list)
        - normalization_suggestions: improvements (as list)
        
        JSON only:"""

        result = llm_json_call(prompt, temperature=0.1)

        return result if result else {}

    def detect_potential_keys(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        # Identify potential primary and foreign keys

        potential_keys = []

        for col in df.columns:
            unique_ratio = df[col].nunique() / len(df) if len(df) > 0 else 0

            # Check if could be primary key
            if unique_ratio > 0.95:
                potential_keys.append({
                    'column': col,
                    'type': 'primary_key_candidate',
                    'uniqueness': unique_ratio,
                    'reasoning': 'High uniqueness ratio'
                })

            # Check if could be foreign key
            elif 'id' in col.lower() or 'key' in col.lower() or 'ref' in col.lower():
                potential_keys.append({
                    'column': col,
                    'type': 'foreign_key_candidate',
                    'uniqueness': unique_ratio,
                    'reasoning': 'Column name suggests reference'
                })

        return potential_keys

    def extract_complete_schema(self, df: pd.DataFrame, header_analyzer: HeaderAnalyzer) -> Dict[str, Any]:
        # Complete schema extraction combining rule-based and AI

        print("\n  Extracting schema...")

        # Get header analysis
        header_mapping = header_analyzer.create_header_mapping(
            list(df.columns), df)

        # Get AI insights if available
        ai_insights = self.extract_schema_with_ai(df)

        # Detect keys
        potential_keys = self.detect_potential_keys(df)

        # Check for quality issues
        for col in df.columns:
            null_pct = df[col].isnull().sum() / len(df) * 100
            if null_pct > 50:
                self.quality_issues.append(
                    f"High null rate in '{col}': {null_pct:.1f}%")

        # Build schema
        self.schema = {
            'table_info': {
                'row_count': len(df),
                'column_count': len(df.columns),
                'estimated_size_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
            },
            'header_analysis': header_mapping,
            'potential_keys': potential_keys,
            'ai_insights': ai_insights,
            'quality_issues': self.quality_issues,
            'llm_provider': llm_interface.client.__class__.__name__ if self.use_ai else 'none'
        }

        return self.schema

# Part 4: Complete CSV Introspection Pipeline


class CSVIntrospectionAgent:
    # Complete agent combining all introspection capabilities

    def __init__(self, use_ai: bool = True):
        self.introspector = CSVIntrospector()
        self.header_analyzer = HeaderAnalyzer()
        self.schema_extractor = SchemaExtractor(use_ai=use_ai)
        self.analysis_results = {}
        self.use_ai = use_ai

    def analyze_csv(self, file_path: str) -> Dict[str, Any]:
        # Complete CSV analysis pipeline

        print(f"\nAnalyzing CSV: {file_path}")
        print("="*60)

        # Step 1: Read CSV robustly
        print("\nStep 1: Reading CSV file")
        df = self.introspector.read_csv_robust(file_path)
        print(
            f"  Successfully loaded {len(df)} rows, {len(df.columns)} columns")

        # Step 2: Extract schema
        print("\nStep 2: Extracting schema")
        if self.use_ai:
            print(
                f"  Using AI provider: {llm_interface.client.__class__.__name__}")
        else:
            print("  Using rule-based analysis only")

        schema = self.schema_extractor.extract_complete_schema(
            df, self.header_analyzer)

        # Step 3: Generate insights
        print("\nStep 3: Generating insights")
        insights = self.generate_insights(df, schema)

        # Compile results
        self.analysis_results = {
            'file_path': file_path,
            'file_metadata': {
                'encoding': self.introspector.encoding,
                'delimiter': self.introspector.delimiter,
                'size_mb': os.path.getsize(file_path) / 1024 / 1024
            },
            'schema': schema,
            'insights': insights,
            'issues': self.introspector.issues + self.schema_extractor.quality_issues
        }

        return self.analysis_results

    def generate_insights(self, df: pd.DataFrame, schema: Dict[str, Any]) -> Dict[str, Any]:
        # Generate actionable insights from analysis

        insights = {
            'data_quality_score': self.calculate_quality_score(schema),
            'completeness': 100 - (df.isnull().sum().sum() / (len(df) * len(df.columns)) * 100),
            'standardization_needed': schema['header_analysis']['pattern_analysis']['consistency'] < 0.8,
            'key_findings': []
        }

        # Add AI insights if available
        if schema.get('ai_insights'):
            if schema['ai_insights'].get('dataset_type'):
                insights['dataset_type'] = schema['ai_insights']['dataset_type']
            if schema['ai_insights'].get('normalization_suggestions'):
                insights['recommendations'] = schema['ai_insights']['normalization_suggestions']

        # Key findings
        if schema['potential_keys']:
            insights['key_findings'].append(
                f"Found {len(schema['potential_keys'])} potential key columns")

        if schema['quality_issues']:
            insights['key_findings'].append(
                f"Identified {len(schema['quality_issues'])} data quality issues")

        return insights

    def calculate_quality_score(self, schema: Dict[str, Any]) -> float:
        # Calculate overall data quality score

        score = 100.0

        # Deduct for quality issues
        score -= len(schema['quality_issues']) * 5

        # Deduct for inconsistent headers
        header_consistency = schema['header_analysis']['pattern_analysis']['consistency']
        score -= (1 - header_consistency) * 20

        return max(0, min(100, score))

    def compare_schemas(self, file_paths: List[str]) -> Dict[str, Any]:
        # Compare schemas across multiple files

        schemas = {}
        for file_path in file_paths:
            if os.path.exists(file_path):
                results = self.analyze_csv(file_path)
                schemas[os.path.basename(file_path)] = results['schema']

        # Find common patterns
        comparison = {
            'files_analyzed': len(schemas),
            'column_variations': {},
            'common_issues': []
        }

        # Group columns by semantic type
        semantic_groups = {}
        for file_name, schema in schemas.items():
            for mapping in schema['header_analysis']['mappings']:
                semantic = mapping['semantic_type']
                if semantic not in semantic_groups:
                    semantic_groups[semantic] = {}
                semantic_groups[semantic][file_name] = mapping['original']

        comparison['semantic_groups'] = semantic_groups

        return comparison

# Example functions for testing


def example_basic_test():
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic CSV Analysis")
    print("="*60)

    agent = CSVIntrospectionAgent(use_ai=True)

    # Test with customer data
    if os.path.exists('workshop_data/customers_v1.csv'):
        results = agent.analyze_csv('workshop_data/customers_v1.csv')
        print(
            f"\nQuality Score: {results['insights']['data_quality_score']:.1f}/100")
        print(f"Completeness: {results['insights']['completeness']:.1f}%")


def example_supply_chain_test():
    print("\n" + "="*60)
    print("EXAMPLE 2: Supply Chain Data Analysis")
    print("="*60)

    agent = CSVIntrospectionAgent(use_ai=True)

    # Test with supply chain data
    files = [
        'supply_chain_data/plant_a_production.csv',
        'supply_chain_data/wms_inventory.csv'
    ]

    for file_path in files:
        if os.path.exists(file_path):
            results = agent.analyze_csv(file_path)
            print(f"\n{os.path.basename(file_path)}:")
            print(
                f"  Quality Score: {results['insights']['data_quality_score']:.1f}")
            if 'dataset_type' in results['insights']:
                print(f"  Dataset Type: {results['insights']['dataset_type']}")


def example_schema_comparison():
    print("\n" + "="*60)
    print("EXAMPLE 3: Schema Comparison")
    print("="*60)

    agent = CSVIntrospectionAgent(use_ai=True)

    # Compare customer file versions
    files = [
        'workshop_data/customers_v1.csv',
        'workshop_data/customers_v2.csv',
        'workshop_data/customers_v3.csv'
    ]

    comparison = agent.compare_schemas(files)

    print(f"\nCompared {comparison['files_analyzed']} files")
    print("\nSemantic groupings found:")
    for semantic_type, files in comparison['semantic_groups'].items():
        print(f"\n  {semantic_type}:")
        for file_name, column in files.items():
            print(f"    {file_name}: {column}")


def example_no_ai_fallback():
    print("\n" + "="*60)
    print("EXAMPLE 4: Rule-Based Analysis (No AI)")
    print("="*60)

    # Test without AI
    agent = CSVIntrospectionAgent(use_ai=False)

    if os.path.exists('workshop_data/orders.csv'):
        results = agent.analyze_csv('workshop_data/orders.csv')
        print(f"\nAnalysis completed using rule-based methods only")
        print(
            f"Quality Score: {results['insights']['data_quality_score']:.1f}/100")

# Main execution


def main():
    print("="*60)
    print("MODULE 2 (v2): CSV SOURCE INTROSPECTION")
    print("="*60)
    print("\nFlexible CSV analysis with multi-LLM support")

    # Show current LLM configuration
    print(f"\nCurrent LLM Provider: {llm_interface.client.__class__.__name__}")

    # Run examples
    example_basic_test()
    example_supply_chain_test()
    example_schema_comparison()
    example_no_ai_fallback()

    print("\n" + "="*60)
    print("MODULE 2 COMPLETE")
    print("="*60)
    print("\nCapabilities demonstrated:")
    print("- Robust CSV reading with encoding/delimiter detection")
    print("- AI-enhanced header analysis and semantic understanding")
    print("- Schema extraction with quality scoring")
    print("- Multi-file schema comparison")
    print("- Fallback to rule-based analysis when AI unavailable")
    print("- Support for Ollama, OpenAI, and other LLM providers")
    print("\nNext: Module 3 - Pattern Matching & Schema Alignment")


if __name__ == "__main__":
    main()
