# module_2_csv_introspection.py
# Module 2: Building a Production CSV Introspection Agent
# Deep analysis of CSV files with automatic schema extraction and standardization

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
from dotenv import load_dotenv
import openai

# Load configuration
load_dotenv()
openai.api_key = os.getenv('OPENAI_API_KEY')
MODEL = os.getenv('OPENAI_MODEL', 'gpt-3.5-turbo')

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
        print(f"  Detected delimiter: '{self.delimiter}' ({'tab' if self.delimiter == '\t' else self.delimiter})")

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

# Part 2: Header Analysis and Standardization


class HeaderAnalyzer:
    # Analyze and standardize CSV headers

    def __init__(self):
        self.original_headers = []
        self.standardized_headers = []
        self.header_mappings = {}
        self.header_types = {}

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

    def infer_semantic_type(self, header: str, samples: List[Any]) -> str:
        # Infer what type of data this column represents

        header_lower = header.lower()

        # Check header keywords
        if any(keyword in header_lower for keyword in ['id', 'key', 'code', 'number']):
            return 'identifier'
        elif any(keyword in header_lower for keyword in ['date', 'time', 'created', 'updated', 'modified']):
            return 'timestamp'
        elif any(keyword in header_lower for keyword in ['amount', 'price', 'cost', 'total', 'sum']):
            return 'monetary'
        elif any(keyword in header_lower for keyword in ['quantity', 'qty', 'count', 'number']):
            return 'quantity'
        elif any(keyword in header_lower for keyword in ['status', 'state', 'flag', 'active']):
            return 'status'
        elif any(keyword in header_lower for keyword in ['name', 'title', 'description']):
            return 'text'
        elif any(keyword in header_lower for keyword in ['email', 'mail']):
            return 'email'
        elif any(keyword in header_lower for keyword in ['phone', 'tel', 'mobile']):
            return 'phone'
        elif any(keyword in header_lower for keyword in ['address', 'location', 'city', 'country']):
            return 'location'

        # Check sample patterns if header doesn't give clear indication
        if samples:
            sample_str = str(samples[0])
            if '@' in sample_str:
                return 'email'
            elif re.match(r'^[A-Z]{2,}-?\d+', sample_str):
                return 'identifier'
            elif re.match(r'^\d{4}-\d{2}-\d{2}', sample_str):
                return 'timestamp'

        return 'general'

    def create_header_mapping(self, headers: List[str], sample_data: pd.DataFrame) -> Dict[str, Any]:
        # Create comprehensive header mapping with metadata

        self.original_headers = headers
        pattern_analysis = self.analyze_header_patterns(headers)

        mappings = []
        for header in headers:
            samples = sample_data[header].dropna().head(
                5).tolist() if header in sample_data.columns else []

            mapping = {
                'original': header,
                'standardized': self.standardize_header(header),
                'semantic_type': self.infer_semantic_type(header, samples),
                'samples': samples[:3]
            }
            mappings.append(mapping)

            self.header_mappings[header] = mapping['standardized']
            self.header_types[header] = mapping['semantic_type']

        return {
            'pattern_analysis': pattern_analysis,
            'mappings': mappings,
            'total_headers': len(headers)
        }

# Part 3: Data Type Inference with Pattern Recognition


class DataTypeInferencer:
    # Advanced type inference using patterns and samples

    def __init__(self):
        self.type_patterns = {
            'integer': [r'^-?\d+$'],
            'float': [r'^-?\d+\.\d+$', r'^-?\d+\.?\d*[eE][+-]?\d+$'],
            'date_iso': [r'^\d{4}-\d{2}-\d{2}$'],
            'date_us': [r'^\d{1,2}/\d{1,2}/\d{4}$'],
            'date_eu': [r'^\d{1,2}-\d{1,2}-\d{4}$'],
            'datetime_iso': [r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}'],
            'boolean': [r'^(true|false|yes|no|y|n|0|1|active|inactive)$'],
            'email': [r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'],
            'phone': [r'^[\+]?[(]?[0-9]{3}[)]?[-\s\.]?[0-9]{3}[-\s\.]?[0-9]{4,6}$'],
            'uuid': [r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'],
            'currency': [r'^[\$£€]\s*[\d,]+\.?\d*$', r'^[\d,]+\.?\d*\s*[\$£€]$']
        }

        self.null_values = {'', 'null', 'NULL', 'None',
                            'none', 'N/A', 'n/a', '#N/A', '-', '--'}

    def detect_pattern(self, value: str) -> List[str]:
        # Detect which patterns match this value
        if value in self.null_values:
            return ['null']

        matched_types = []
        for type_name, patterns in self.type_patterns.items():
            for pattern in patterns:
                if re.match(pattern, value, re.IGNORECASE):
                    matched_types.append(type_name)
                    break

        return matched_types if matched_types else ['string']

    def infer_column_type(self, column_data: pd.Series) -> Dict[str, Any]:
        # Comprehensive type inference for a column

        # Remove null values
        non_null = column_data.dropna()
        non_null_str = non_null.astype(str)

        # Count null values
        null_count = len(column_data) - len(non_null)
        null_percentage = null_count / \
            len(column_data) * 100 if len(column_data) > 0 else 0

        # Analyze patterns in non-null values
        type_counts = Counter()
        for value in non_null_str:
            types = self.detect_pattern(str(value))
            type_counts.update(types)

        # Determine primary type
        if type_counts:
            primary_type = type_counts.most_common(1)[0][0]
            consistency = type_counts[primary_type] / \
                len(non_null) if len(non_null) > 0 else 0
        else:
            primary_type = 'empty'
            consistency = 0

        # Get format details
        format_info = self.get_format_details(primary_type, non_null_str)

        # Calculate statistics
        stats = self.calculate_statistics(primary_type, non_null)

        return {
            'primary_type': primary_type,
            'consistency': consistency,
            'null_percentage': null_percentage,
            'type_distribution': dict(type_counts),
            'format': format_info,
            'statistics': stats,
            'sample_values': non_null.head(5).tolist()
        }

    def get_format_details(self, data_type: str, values: pd.Series) -> Dict[str, Any]:
        # Extract format details based on type

        if 'date' in data_type:
            # Analyze date formats
            formats = []
            for val in values.head(10):
                if 'iso' in data_type:
                    formats.append('%Y-%m-%d')
                elif 'us' in data_type:
                    formats.append('%m/%d/%Y')
                elif 'eu' in data_type:
                    formats.append('%d-%m-%Y')

            return {'date_format': formats[0] if formats else 'unknown'}

        elif data_type == 'currency':
            # Detect currency symbol
            symbols = []
            for val in values.head(10):
                if '$' in str(val):
                    symbols.append('USD')
                elif '€' in str(val):
                    symbols.append('EUR')
                elif '£' in str(val):
                    symbols.append('GBP')

            return {'currency': symbols[0] if symbols else 'unknown'}

        elif data_type == 'string':
            # Analyze string patterns
            lengths = values.str.len()
            return {
                'min_length': int(lengths.min()) if len(lengths) > 0 else 0,
                'max_length': int(lengths.max()) if len(lengths) > 0 else 0,
                'avg_length': float(lengths.mean()) if len(lengths) > 0 else 0
            }

        return {}

    def calculate_statistics(self, data_type: str, values: pd.Series) -> Dict[str, Any]:
        # Calculate relevant statistics based on type

        stats = {}

        if data_type in ['integer', 'float']:
            try:
                numeric_values = pd.to_numeric(
                    values, errors='coerce').dropna()
                if len(numeric_values) > 0:
                    stats = {
                        'min': float(numeric_values.min()),
                        'max': float(numeric_values.max()),
                        'mean': float(numeric_values.mean()),
                        'median': float(numeric_values.median()),
                        'std': float(numeric_values.std())
                    }
            except:
                pass

        elif data_type in ['string', 'email']:
            unique_count = values.nunique()
            total_count = len(values)
            stats = {
                'unique_values': unique_count,
                'uniqueness_ratio': unique_count / total_count if total_count > 0 else 0
            }

        return stats

# Part 4: Schema Extraction and Documentation


class SchemaExtractor:
    # Extract and document complete schema from CSV

    def __init__(self):
        self.schema = {}
        self.relationships = []
        self.constraints = []
        self.quality_issues = []

    def extract_schema(self, df: pd.DataFrame, header_analyzer: HeaderAnalyzer, type_inferencer: DataTypeInferencer) -> Dict[str, Any]:
        # Complete schema extraction

        print("\n  Extracting schema...")

        # Analyze headers
        header_mapping = header_analyzer.create_header_mapping(
            list(df.columns), df)

        # Analyze each column
        columns_schema = []
        for col in df.columns:
            print(f"    Analyzing column: {col}")

            # Get type information
            type_info = type_inferencer.infer_column_type(df[col])

            # Build column schema
            col_schema = {
                'name': col,
                'standardized_name': header_analyzer.header_mappings.get(col, col),
                'semantic_type': header_analyzer.header_types.get(col, 'unknown'),
                'data_type': type_info['primary_type'],
                'nullable': type_info['null_percentage'] > 0,
                'null_percentage': type_info['null_percentage'],
                'type_consistency': type_info['consistency'],
                'format': type_info.get('format', {}),
                'statistics': type_info.get('statistics', {}),
                'samples': type_info.get('sample_values', [])[:3]
            }

            columns_schema.append(col_schema)

            # Check for quality issues
            if type_info['consistency'] < 0.9:
                self.quality_issues.append(
                    f"Inconsistent data type in column '{col}': {type_info['consistency']:.2%} consistency")

            if type_info['null_percentage'] > 50:
                self.quality_issues.append(
                    f"High null rate in column '{col}': {type_info['null_percentage']:.1f}%")

        # Detect potential keys and relationships
        self.detect_keys_and_relationships(df, columns_schema)

        # Build complete schema
        self.schema = {
            'table_info': {
                'row_count': len(df),
                'column_count': len(df.columns),
                'estimated_size_mb': df.memory_usage(deep=True).sum() / 1024 / 1024
            },
            'header_analysis': header_mapping,
            'columns': columns_schema,
            'potential_keys': self.detect_potential_keys(df),
            'relationships': self.relationships,
            'quality_issues': self.quality_issues,
            'recommendations': self.generate_recommendations()
        }

        return self.schema

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

    def detect_keys_and_relationships(self, df: pd.DataFrame, columns_schema: List[Dict]) -> None:
        # Detect relationships between columns

        for col_schema in columns_schema:
            col = col_schema['name']

            # Check for ID patterns suggesting relationships
            if col_schema['semantic_type'] == 'identifier':
                # Look for similar columns in other tables (would need multiple tables)
                self.relationships.append({
                    'column': col,
                    'type': 'potential_foreign_key',
                    'target': 'unknown',  # Would need other tables to determine
                    'confidence': 0.7
                })

    def generate_recommendations(self) -> List[str]:
        # Generate improvement recommendations

        recommendations = []

        if self.quality_issues:
            recommendations.append(
                "Address data quality issues identified in the analysis")

        if any('inconsistent' in issue.lower() for issue in self.quality_issues):
            recommendations.append(
                "Standardize data types across columns for consistency")

        if not self.relationships:
            recommendations.append(
                "Consider adding explicit foreign key relationships")

        recommendations.append(
            "Implement data validation rules based on detected patterns")
        recommendations.append(
            "Consider normalizing column names to a consistent format")

        return recommendations

# Part 5: Complete CSV Introspection Pipeline


class CSVIntrospectionAgent:
    # Complete agent combining all introspection capabilities

    def __init__(self):
        self.introspector = CSVIntrospector()
        self.header_analyzer = HeaderAnalyzer()
        self.type_inferencer = DataTypeInferencer()
        self.schema_extractor = SchemaExtractor()
        self.analysis_results = {}

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
        schema = self.schema_extractor.extract_schema(
            df, self.header_analyzer, self.type_inferencer)

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
            'key_findings': [],
            'transformation_priority': []
        }

        # Key findings
        if schema['potential_keys']:
            insights['key_findings'].append(
                f"Found {len(schema['potential_keys'])} potential key columns")

        if schema['quality_issues']:
            insights['key_findings'].append(
                f"Identified {len(schema['quality_issues'])} data quality issues")

        # Transformation priorities
        for col in schema['columns']:
            if col['type_consistency'] < 0.9:
                insights['transformation_priority'].append({
                    'column': col['name'],
                    'issue': 'type_inconsistency',
                    'priority': 'high'
                })

            if col['null_percentage'] > 30:
                insights['transformation_priority'].append({
                    'column': col['name'],
                    'issue': 'high_null_rate',
                    'priority': 'medium'
                })

        return insights

    def calculate_quality_score(self, schema: Dict[str, Any]) -> float:
        # Calculate overall data quality score

        score = 100.0

        # Deduct for quality issues
        score -= len(schema['quality_issues']) * 5

        # Deduct for inconsistent headers
        header_consistency = schema['header_analysis']['pattern_analysis']['consistency']
        score -= (1 - header_consistency) * 20

        # Deduct for type inconsistencies
        for col in schema['columns']:
            if col['type_consistency'] < 0.9:
                score -= 5

        return max(0, min(100, score))

    def export_schema_documentation(self, output_path: str = 'output/schema_doc.json'):
        # Export analysis results as documentation

        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        with open(output_path, 'w') as f:
            json.dump(self.analysis_results, f, indent=2, default=str)

        print(f"\nSchema documentation exported to: {output_path}")

        # Also create a simplified summary
        summary = {
            'file': self.analysis_results['file_path'],
            'quality_score': self.analysis_results['insights']['data_quality_score'],
            'columns': len(self.analysis_results['schema']['columns']),
            'rows': self.analysis_results['schema']['table_info']['row_count'],
            'key_issues': self.analysis_results['issues'][:5]  # Top 5 issues
        }

        summary_path = output_path.replace('.json', '_summary.json')
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)

        print(f"Summary exported to: {summary_path}")

# Example 1: Basic CSV introspection


def example_basic_introspection():
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic CSV Introspection")
    print("="*60)

    introspector = CSVIntrospector()

    # Test with different customer CSV versions
    files = [
        'workshop_data/customers_v1.csv',
        'workshop_data/customers_v2.csv',
        'workshop_data/customers_v3.csv'
    ]

    for file_path in files:
        if os.path.exists(file_path):
            print(f"\nAnalyzing: {file_path}")
            df = introspector.read_csv_robust(file_path)
            print(f"  Shape: {df.shape}")
            print(f"  Headers: {list(df.columns)[:5]}")
            print(f"  Issues: {introspector.issues}")

# Example 2: Header analysis across versions


def example_header_analysis():
    print("\n" + "="*60)
    print("EXAMPLE 2: Header Pattern Analysis")
    print("="*60)

    analyzer = HeaderAnalyzer()

    # Compare headers from different versions
    headers_v1 = ['Customer_ID', 'Full_Name', 'Email_Address',
                  'Registration_Date', 'Account_Status']
    headers_v2 = ['CustID', 'CustomerName',
                  'ContactEmail', 'SignupDate', 'Status']
    headers_v3 = ['client_number', 'name',
                  'email_addr', 'created_at', 'active']

    print("\nVersion 1 Headers:")
    pattern = analyzer.analyze_header_patterns(headers_v1)
    print(
        f"  Pattern: {pattern['dominant']}, Consistency: {pattern['consistency']:.2%}")

    print("\nVersion 2 Headers:")
    pattern = analyzer.analyze_header_patterns(headers_v2)
    print(
        f"  Pattern: {pattern['dominant']}, Consistency: {pattern['consistency']:.2%}")

    print("\nVersion 3 Headers:")
    pattern = analyzer.analyze_header_patterns(headers_v3)
    print(
        f"  Pattern: {pattern['dominant']}, Consistency: {pattern['consistency']:.2%}")

    print("\nStandardizing all headers to snake_case:")
    all_headers = headers_v1 + headers_v2 + headers_v3
    for header in all_headers:
        standardized = analyzer.standardize_header(header)
        print(f"  {header:20} -> {standardized}")

# Example 3: Type inference on real data


def example_type_inference():
    print("\n" + "="*60)
    print("EXAMPLE 3: Advanced Type Inference")
    print("="*60)

    inferencer = DataTypeInferencer()

    # Load orders data with mixed formats
    df = pd.read_csv('workshop_data/orders.csv')

    print("\nAnalyzing orders.csv columns:")
    for col in df.columns:
        type_info = inferencer.infer_column_type(df[col])
        print(f"\n{col}:")
        print(f"  Type: {type_info['primary_type']}")
        print(f"  Consistency: {type_info['consistency']:.2%}")
        print(f"  Null rate: {type_info['null_percentage']:.1f}%")
        print(f"  Samples: {type_info['sample_values'][:3]}")

# Example 4: Complete pipeline on supply chain data


def example_supply_chain_analysis():
    print("\n" + "="*60)
    print("EXAMPLE 4: Supply Chain Data Analysis")
    print("="*60)

    agent = CSVIntrospectionAgent()

    # Analyze complex supply chain data
    supply_chain_files = [
        'supply_chain_data/plant_a_production.csv',
        'supply_chain_data/wms_inventory.csv',
        'supply_chain_data/carrier_shipments.tsv'
    ]

    for file_path in supply_chain_files:
        if os.path.exists(file_path):
            results = agent.analyze_csv(file_path)

            print(f"\n{file_path} Analysis Summary:")
            print(
                f"  Quality Score: {results['insights']['data_quality_score']:.1f}/100")
            print(
                f"  Completeness: {results['insights']['completeness']:.1f}%")
            print(f"  Key Issues: {len(results['issues'])}")

            # Export documentation
            output_name = os.path.basename(file_path).replace(
                '.csv', '').replace('.tsv', '')
            agent.export_schema_documentation(
                f'output/{output_name}_schema.json')

# Example 5: Comparing schemas across versions


def example_schema_comparison():
    print("\n" + "="*60)
    print("EXAMPLE 5: Schema Evolution Detection")
    print("="*60)

    agent = CSVIntrospectionAgent()

    # Analyze all customer versions
    schemas = {}
    for i in range(1, 4):
        file_path = f'workshop_data/customers_v{i}.csv'
        if os.path.exists(file_path):
            results = agent.analyze_csv(file_path)
            schemas[f'v{i}'] = results['schema']

    # Compare schemas
    print("\nSchema Evolution Analysis:")
    print("\nColumn name changes:")
    for i in range(1, 3):
        v1_cols = set([c['name'] for c in schemas[f'v{i}']['columns']])
        v2_cols = set([c['name'] for c in schemas[f'v{i+1}']['columns']])

        print(f"\n  v{i} -> v{i+1}:")
        removed = v1_cols - v2_cols
        added = v2_cols - v1_cols

        if removed:
            print(f"    Removed: {removed}")
        if added:
            print(f"    Added: {added}")

    print("\nData type consistency across versions:")
    # Map columns by semantic type
    semantic_groups = {}
    for version, schema in schemas.items():
        for col in schema['columns']:
            semantic = col['semantic_type']
            if semantic not in semantic_groups:
                semantic_groups[semantic] = {}
            semantic_groups[semantic][version] = col['name']

    for semantic_type, versions in semantic_groups.items():
        if len(versions) > 1:
            print(f"\n  {semantic_type}:")
            for version, col_name in versions.items():
                print(f"    {version}: {col_name}")

# Main execution


def main():
    print("="*60)
    print("MODULE 2: CSV SOURCE INTROSPECTION")
    print("="*60)
    print("\nThis module demonstrates deep CSV analysis and schema extraction")

    # Run examples
    example_basic_introspection()
    example_header_analysis()
    example_type_inference()
    example_supply_chain_analysis()
    example_schema_comparison()

    print("\n" + "="*60)
    print("MODULE 2 COMPLETE")
    print("="*60)
    print("\nKey capabilities demonstrated:")
    print("- Robust CSV reading with encoding/delimiter detection")
    print("- Header pattern analysis and standardization")
    print("- Advanced type inference with pattern matching")
    print("- Complete schema extraction and documentation")
    print("- Data quality scoring and issue detection")
    print("\nNext: Module 3 - Pattern Matching & Schema Alignment")


if __name__ == "__main__":
    main()
