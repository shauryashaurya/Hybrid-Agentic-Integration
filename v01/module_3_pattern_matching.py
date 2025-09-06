# module_3_pattern_matching.py
# Module 3: Pattern Matching and Schema Alignment
# Semantic understanding to align schemas across heterogeneous sources

import os
import json
import re
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
import numpy as np
from datetime import datetime
from collections import Counter
import itertools

# Import our LLM interface and previous modules
from llm_providers import llm_interface, simple_llm_call, llm_json_call
from module_2_csv_introspection_v2 import CSVIntrospectionAgent

# Part 1: Semantic Similarity Without External Dependencies
# We implement our own similarity measures to avoid heavy ML dependencies


class SemanticMatcher:
    # Match fields based on semantic meaning

    def __init__(self, use_ai: bool = True):
        self.use_ai = use_ai
        self.common_synonyms = self.load_synonym_dictionary()
        self.abbreviations = self.load_abbreviations()
        self.match_cache = {}  # Cache AI responses

    def load_synonym_dictionary(self) -> Dict[str, List[str]]:
        # Common field name synonyms in data integration
        return {
            'customer': ['client', 'buyer', 'purchaser', 'consumer', 'account'],
            'identifier': ['id', 'key', 'code', 'number', 'ref', 'reference'],
            'product': ['item', 'article', 'sku', 'merchandise', 'goods'],
            'quantity': ['qty', 'amount', 'count', 'units', 'volume'],
            'price': ['cost', 'rate', 'fee', 'charge', 'amount'],
            'date': ['time', 'timestamp', 'datetime', 'when', 'period'],
            'status': ['state', 'condition', 'flag', 'active', 'enabled'],
            'name': ['title', 'label', 'description', 'desc'],
            'address': ['location', 'place', 'site', 'destination'],
            'email': ['mail', 'contact', 'electronic_mail'],
            'phone': ['telephone', 'tel', 'mobile', 'cell', 'contact_number'],
            'order': ['purchase', 'transaction', 'sale', 'invoice'],
            'supplier': ['vendor', 'provider', 'manufacturer', 'source'],
            'warehouse': ['depot', 'storage', 'facility', 'distribution_center', 'dc'],
            'shipment': ['delivery', 'consignment', 'cargo', 'freight']
        }

    def load_abbreviations(self) -> Dict[str, str]:
        # Common abbreviations in business data
        return {
            'cust': 'customer',
            'qty': 'quantity',
            'amt': 'amount',
            'dt': 'date',
            'id': 'identifier',
            'ref': 'reference',
            'desc': 'description',
            'addr': 'address',
            'tel': 'telephone',
            'wh': 'warehouse',
            'dc': 'distribution_center',
            'sku': 'stock_keeping_unit',
            'po': 'purchase_order',
            'inv': 'inventory',
            'mfg': 'manufacturing',
            'prd': 'product',
            'sup': 'supplier',
            'shp': 'shipment',
            'dlv': 'delivery'
        }

    def tokenize_field_name(self, field_name: str) -> List[str]:
        # Break field name into meaningful tokens

        # Replace common separators with spaces
        cleaned = re.sub(r'[_\-\.]', ' ', field_name)

        # Split camelCase and PascalCase
        cleaned = re.sub(r'([a-z])([A-Z])', r'\1 \2', cleaned)
        cleaned = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1 \2', cleaned)

        # Convert to lowercase and split
        tokens = cleaned.lower().split()

        # Expand abbreviations
        expanded_tokens = []
        for token in tokens:
            if token in self.abbreviations:
                expanded_tokens.append(self.abbreviations[token])
            else:
                expanded_tokens.append(token)

        return expanded_tokens

    def calculate_token_similarity(self, tokens1: List[str], tokens2: List[str]) -> float:
        # Calculate similarity between two token lists

        if not tokens1 or not tokens2:
            return 0.0

        # Direct token matches
        common_tokens = set(tokens1) & set(tokens2)
        if common_tokens:
            return len(common_tokens) / max(len(tokens1), len(tokens2))

        # Check synonyms
        synonym_matches = 0
        for t1 in tokens1:
            for t2 in tokens2:
                # Check if tokens are synonyms
                for syn_group in self.common_synonyms.values():
                    if t1 in syn_group and t2 in syn_group:
                        synonym_matches += 1
                        break

        if synonym_matches > 0:
            # Slightly lower score for synonyms
            return synonym_matches / max(len(tokens1), len(tokens2)) * 0.8

        # Check substring matches
        substring_matches = 0
        for t1 in tokens1:
            for t2 in tokens2:
                if len(t1) > 3 and len(t2) > 3:  # Only for meaningful length
                    if t1 in t2 or t2 in t1:
                        substring_matches += 1

        if substring_matches > 0:
            return substring_matches / max(len(tokens1), len(tokens2)) * 0.6

        return 0.0

    def calculate_semantic_similarity_with_ai(self, field1: str, field2: str, samples1: List[Any] = None, samples2: List[Any] = None) -> float:
        # Use AI to determine semantic similarity

        if not self.use_ai:
            return 0.0

        # Check cache first
        cache_key = f"{field1}|{field2}"
        if cache_key in self.match_cache:
            return self.match_cache[cache_key]

        # Prepare sample data for context
        samples1_str = str(samples1[:3]) if samples1 else "no samples"
        samples2_str = str(samples2[:3]) if samples2 else "no samples"

        prompt = f"""
        Determine if these two database fields represent the same semantic concept:
        
        Field 1: {field1}
        Samples: {samples1_str}
        
        Field 2: {field2}
        Samples: {samples2_str}
        
        Return JSON with:
        - match: true/false
        - confidence: 0.0 to 1.0
        - reasoning: brief explanation
        
        JSON only:"""

        result = llm_json_call(prompt, temperature=0.1)

        if result and 'confidence' in result:
            score = float(result['confidence'])
            self.match_cache[cache_key] = score
            return score

        return 0.0

    def calculate_similarity(self, field1: str, field2: str, samples1: List[Any] = None, samples2: List[Any] = None) -> float:
        # Combined similarity calculation

        # Tokenize field names
        tokens1 = self.tokenize_field_name(field1)
        tokens2 = self.tokenize_field_name(field2)

        # Calculate token-based similarity
        token_similarity = self.calculate_token_similarity(tokens1, tokens2)

        # If high token similarity, that's probably enough
        if token_similarity > 0.8:
            return token_similarity

        # Try AI-based similarity if available
        if self.use_ai:
            ai_similarity = self.calculate_semantic_similarity_with_ai(
                field1, field2, samples1, samples2)
            # Weighted average of token and AI similarity
            return max(token_similarity, ai_similarity)

        return token_similarity

# Part 2: Schema Mapping Engine


class SchemaMappingEngine:
    # Engine to map fields between different schemas

    def __init__(self, use_ai: bool = True):
        self.semantic_matcher = SemanticMatcher(use_ai=use_ai)
        self.mappings = []
        self.confidence_threshold = 0.6

    def analyze_data_patterns(self, series1: pd.Series, series2: pd.Series) -> float:
        # Compare data patterns between two columns

        # Check data types
        dtype1 = str(series1.dtype)
        dtype2 = str(series2.dtype)

        if dtype1 != dtype2:
            # Different types, but might still be compatible
            if 'int' in dtype1 and 'float' in dtype2:
                return 0.8
            elif 'object' in dtype1 and 'object' in dtype2:
                return 0.9
            else:
                return 0.3

        # Check value distributions for numeric data
        if pd.api.types.is_numeric_dtype(series1) and pd.api.types.is_numeric_dtype(series2):
            # Compare ranges
            range1 = series1.max() - series1.min()
            range2 = series2.max() - series2.min()

            if range1 > 0 and range2 > 0:
                range_ratio = min(range1, range2) / max(range1, range2)
                return range_ratio

        # Check unique value overlap for categorical data
        if dtype1 == 'object':
            unique1 = set(series1.dropna().unique())
            unique2 = set(series2.dropna().unique())

            if unique1 and unique2:
                overlap = len(unique1 & unique2)
                total = len(unique1 | unique2)
                if total > 0:
                    return overlap / total

        return 0.5  # Default moderate confidence

    def find_best_mapping(self, source_field: str, target_fields: List[str], source_data: pd.DataFrame = None, target_data: pd.DataFrame = None) -> Tuple[str, float]:
        # Find best matching field from target schema

        best_match = None
        best_score = 0.0

        # Get samples from source field if available
        source_samples = None
        if source_data is not None and source_field in source_data.columns:
            source_samples = source_data[source_field].dropna().head(
                5).tolist()

        for target_field in target_fields:
            # Get samples from target field
            target_samples = None
            if target_data is not None and target_field in target_data.columns:
                target_samples = target_data[target_field].dropna().head(
                    5).tolist()

            # Calculate semantic similarity
            semantic_score = self.semantic_matcher.calculate_similarity(
                source_field, target_field, source_samples, target_samples
            )

            # Calculate data pattern similarity if data available
            pattern_score = 0.5  # Default
            if source_data is not None and target_data is not None:
                if source_field in source_data.columns and target_field in target_data.columns:
                    pattern_score = self.analyze_data_patterns(
                        source_data[source_field],
                        target_data[target_field]
                    )

            # Combined score (weighted average)
            combined_score = (semantic_score * 0.7) + (pattern_score * 0.3)

            if combined_score > best_score:
                best_score = combined_score
                best_match = target_field

        return best_match, best_score

    def map_schemas(self, source_schema: Dict[str, Any], target_schema: Dict[str, Any], source_data: pd.DataFrame = None, target_data: pd.DataFrame = None) -> List[Dict[str, Any]]:
        # Map fields between two schemas

        print("\nMapping schemas...")

        # Extract field lists
        source_fields = [col['name']
                         for col in source_schema.get('columns', [])]
        target_fields = [col['name']
                         for col in target_schema.get('columns', [])]

        mappings = []
        unmapped_source = []
        unmapped_target = set(target_fields)

        for source_field in source_fields:
            target_match, confidence = self.find_best_mapping(
                source_field, list(unmapped_target), source_data, target_data
            )

            if confidence >= self.confidence_threshold and target_match:
                mapping = {
                    'source_field': source_field,
                    'target_field': target_match,
                    'confidence': confidence,
                    'mapping_type': 'direct' if confidence > 0.9 else 'fuzzy'
                }
                mappings.append(mapping)
                unmapped_target.discard(target_match)

                print(
                    f"  {source_field:30} -> {target_match:30} (confidence: {confidence:.2f})")
            else:
                unmapped_source.append(source_field)
                print(f"  {source_field:30} -> [NO MATCH FOUND]")

        # Report unmapped fields
        if unmapped_source:
            print(f"\nUnmapped source fields: {unmapped_source}")
        if unmapped_target:
            print(f"Unmapped target fields: {list(unmapped_target)}")

        self.mappings = mappings
        return mappings

# Part 3: Unified Schema Builder


class UnifiedSchemaBuilder:
    # Build a unified schema from multiple sources

    def __init__(self):
        self.unified_schema = {}
        self.field_groups = {}
        self.canonical_names = {}

    def determine_canonical_field_name(self, field_variations: List[str]) -> str:
        # Determine the best canonical name for a field

        # Prefer snake_case
        snake_case_candidates = [
            f for f in field_variations if '_' in f and f.islower()]
        if snake_case_candidates:
            # Choose shortest snake_case name
            return min(snake_case_candidates, key=len)

        # Otherwise choose the most descriptive (longest) name
        return max(field_variations, key=len)

    def determine_canonical_data_type(self, type_variations: List[str]) -> str:
        # Determine the most appropriate data type

        # Priority order for data types
        type_priority = {
            'identifier': 1,
            'timestamp': 2,
            'datetime': 2,
            'date': 3,
            'float': 4,
            'integer': 5,
            'boolean': 6,
            'string': 7,
            'text': 8,
            'unknown': 9
        }

        # Find the highest priority type
        best_type = 'unknown'
        best_priority = 9

        for dtype in type_variations:
            priority = type_priority.get(dtype.lower(), 9)
            if priority < best_priority:
                best_priority = priority
                best_type = dtype

        return best_type

    def build_unified_schema(self, schema_mappings: List[Dict[str, Any]], schemas: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        # Build unified schema from multiple source schemas

        print("\nBuilding unified schema...")

        # Group related fields
        field_groups = {}

        # Process each schema
        for source_name, schema in schemas.items():
            for column in schema.get('columns', []):
                field_name = column['name']
                semantic_type = column.get('semantic_type', 'unknown')

                # Find or create group for this semantic type
                if semantic_type not in field_groups:
                    field_groups[semantic_type] = []

                field_groups[semantic_type].append({
                    'source': source_name,
                    'field_name': field_name,
                    'data_type': column.get('data_type', 'unknown'),
                    'standardized_name': column.get('standardized_name', field_name)
                })

        # Build unified fields
        unified_fields = []

        for semantic_type, fields in field_groups.items():
            # Get all field name variations
            field_names = [f['standardized_name'] for f in fields]
            canonical_name = self.determine_canonical_field_name(field_names)

            # Get all data type variations
            data_types = [f['data_type'] for f in fields]
            canonical_type = self.determine_canonical_data_type(data_types)

            # Create unified field definition
            unified_field = {
                'canonical_name': canonical_name,
                'semantic_type': semantic_type,
                'data_type': canonical_type,
                'source_mappings': {}
            }

            # Add source mappings
            for field in fields:
                unified_field['source_mappings'][field['source']
                                                 ] = field['field_name']

            unified_fields.append(unified_field)

            print(
                f"  {canonical_name:30} <- {list(unified_field['source_mappings'].values())}")

        # Build complete unified schema
        self.unified_schema = {
            'version': '1.0',
            'created_at': datetime.now().isoformat(),
            'fields': unified_fields,
            'sources': list(schemas.keys()),
            'total_fields': len(unified_fields)
        }

        return self.unified_schema

    def generate_sql_view(self, unified_schema: Dict[str, Any], source_table_names: Dict[str, str]) -> str:
        # Generate SQL view definition for unified access

        sql_views = []

        for source in unified_schema['sources']:
            table_name = source_table_names.get(source, source)

            select_clauses = []
            for field in unified_schema['fields']:
                canonical_name = field['canonical_name']
                source_field = field['source_mappings'].get(source)

                if source_field:
                    select_clauses.append(
                        f"    {source_field} AS {canonical_name}")
                else:
                    # Field doesn't exist in this source, use NULL
                    select_clauses.append(f"    NULL AS {canonical_name}")

            sql = f"SELECT\n{',\n'.join(select_clauses)}\nFROM {table_name}"
            sql_views.append(sql)

        # Combine with UNION ALL
        unified_sql = "\nUNION ALL\n".join(sql_views)

        return f"CREATE VIEW unified_data AS\n{unified_sql};"

# Part 4: Ambiguity Resolution


class AmbiguityResolver:
    # Handle ambiguous mappings that need human input or special rules

    def __init__(self):
        self.ambiguous_mappings = []
        self.resolution_rules = []

    def identify_ambiguities(self, mappings: List[Dict[str, Any]], threshold_difference: float = 0.1) -> List[Dict[str, Any]]:
        # Find mappings with similar confidence scores (ambiguous)

        ambiguous = []

        # Group mappings by source field
        source_groups = {}
        for mapping in mappings:
            source = mapping['source_field']
            if source not in source_groups:
                source_groups[source] = []
            source_groups[source].append(mapping)

        # Check for ambiguities
        for source, field_mappings in source_groups.items():
            if len(field_mappings) > 1:
                # Sort by confidence
                field_mappings.sort(
                    key=lambda x: x['confidence'], reverse=True)

                # Check if top scores are close
                if len(field_mappings) >= 2:
                    diff = field_mappings[0]['confidence'] - \
                        field_mappings[1]['confidence']
                    if diff < threshold_difference:
                        ambiguous.append({
                            'source_field': source,
                            # Top 3 candidates
                            'candidates': field_mappings[:3],
                            'ambiguity_score': 1.0 - diff
                        })

        self.ambiguous_mappings = ambiguous
        return ambiguous

    def apply_resolution_rules(self, ambiguous_mapping: Dict[str, Any]) -> Optional[str]:
        # Apply rules to resolve ambiguity

        source_field = ambiguous_mapping['source_field']
        candidates = ambiguous_mapping['candidates']

        # Rule 1: Prefer exact name matches
        for candidate in candidates:
            if candidate['source_field'].lower() == candidate['target_field'].lower():
                return candidate['target_field']

        # Rule 2: Prefer fields with similar semantic types
        # (would need semantic type information)

        # Rule 3: Choose highest confidence if difference is meaningful
        if candidates[0]['confidence'] > 0.7:
            return candidates[0]['target_field']

        return None

    def resolve_with_ai(self, ambiguous_mapping: Dict[str, Any]) -> Optional[str]:
        # Use AI to resolve ambiguity

        source_field = ambiguous_mapping['source_field']
        candidates = ambiguous_mapping['candidates']

        candidate_list = "\n".join([
            f"- {c['target_field']} (confidence: {c['confidence']:.2f})"
            for c in candidates
        ])

        prompt = f"""
        Choose the best mapping for this field:
        
        Source field: {source_field}
        
        Candidate mappings:
        {candidate_list}
        
        Return JSON with:
        - best_match: the chosen target field name
        - reasoning: why this is the best choice
        
        JSON only:"""

        result = llm_json_call(prompt, temperature=0.1)

        if result and 'best_match' in result:
            return result['best_match']

        return None

# Part 5: Complete Pattern Matching Pipeline


class PatternMatchingPipeline:
    # Complete pipeline for pattern matching and schema alignment

    def __init__(self, use_ai: bool = True):
        self.introspection_agent = CSVIntrospectionAgent(use_ai=use_ai)
        self.mapping_engine = SchemaMappingEngine(use_ai=use_ai)
        self.schema_builder = UnifiedSchemaBuilder()
        self.ambiguity_resolver = AmbiguityResolver()
        self.use_ai = use_ai

    def analyze_sources(self, file_paths: List[str]) -> Dict[str, Dict[str, Any]]:
        # Analyze all data sources

        schemas = {}
        data_frames = {}

        for file_path in file_paths:
            if os.path.exists(file_path):
                print(f"\nAnalyzing: {file_path}")
                results = self.introspection_agent.analyze_csv(file_path)

                # Store schema
                source_name = os.path.basename(file_path).replace(
                    '.csv', '').replace('.tsv', '')
                schemas[source_name] = results['schema']

                # Load data for pattern analysis
                df = pd.read_csv(file_path,
                                 encoding=results['file_metadata']['encoding'],
                                 delimiter=results['file_metadata']['delimiter'])
                data_frames[source_name] = df

        return schemas, data_frames

    def align_schemas(self, schemas: Dict[str, Dict[str, Any]], data_frames: Dict[str, pd.DataFrame] = None) -> Dict[str, Any]:
        # Align multiple schemas into unified schema

        print("\n" + "="*60)
        print("SCHEMA ALIGNMENT PROCESS")
        print("="*60)

        # If we have more than 2 schemas, we need to map them pairwise
        schema_names = list(schemas.keys())

        all_mappings = []

        if len(schema_names) >= 2:
            # Use first schema as reference
            reference_name = schema_names[0]
            reference_schema = schemas[reference_name]
            reference_data = data_frames.get(
                reference_name) if data_frames else None

            for other_name in schema_names[1:]:
                print(f"\nMapping {other_name} to {reference_name}:")

                other_schema = schemas[other_name]
                other_data = data_frames.get(
                    other_name) if data_frames else None

                mappings = self.mapping_engine.map_schemas(
                    other_schema, reference_schema,
                    other_data, reference_data
                )

                all_mappings.extend(mappings)

        # Build unified schema
        unified_schema = self.schema_builder.build_unified_schema(
            all_mappings, schemas)

        # Identify ambiguities
        ambiguities = self.ambiguity_resolver.identify_ambiguities(
            all_mappings)

        if ambiguities:
            print(f"\nFound {len(ambiguities)} ambiguous mappings")
            for amb in ambiguities:
                print(
                    f"  {amb['source_field']}: {len(amb['candidates'])} candidates")

                # Try to resolve
                resolved = self.ambiguity_resolver.apply_resolution_rules(amb)
                if not resolved and self.use_ai:
                    resolved = self.ambiguity_resolver.resolve_with_ai(amb)

                if resolved:
                    print(f"    Resolved to: {resolved}")

        return {
            'unified_schema': unified_schema,
            'mappings': all_mappings,
            'ambiguities': ambiguities
        }

# Example functions


def example_basic_pattern_matching():
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic Pattern Matching")
    print("="*60)

    matcher = SemanticMatcher(use_ai=True)

    # Test field pairs
    test_pairs = [
        ('Customer_ID', 'CustID'),
        ('Customer_ID', 'client_number'),
        ('Registration_Date', 'SignupDate'),
        ('Full_Name', 'CustomerName'),
        ('Email_Address', 'ContactEmail'),
        ('Account_Status', 'active')
    ]

    print("\nSemantic similarity scores:")
    for field1, field2 in test_pairs:
        similarity = matcher.calculate_similarity(field1, field2)
        print(f"  {field1:20} <-> {field2:20} : {similarity:.2f}")


def example_schema_alignment():
    print("\n" + "="*60)
    print("EXAMPLE 2: Customer Schema Alignment")
    print("="*60)

    pipeline = PatternMatchingPipeline(use_ai=True)

    # Align customer file versions
    files = [
        'workshop_data/customers_v1.csv',
        'workshop_data/customers_v2.csv',
        'workshop_data/customers_v3.csv'
    ]

    # Check files exist
    existing_files = [f for f in files if os.path.exists(f)]
    if len(existing_files) < 2:
        print("  Need at least 2 customer files. Run fake_data_generator.py first.")
        return

    # Analyze and align
    schemas, data_frames = pipeline.analyze_sources(existing_files)
    alignment_results = pipeline.align_schemas(schemas, data_frames)

    # Display unified schema
    print("\n" + "="*60)
    print("UNIFIED SCHEMA")
    print("="*60)

    unified = alignment_results['unified_schema']
    print(
        f"\nUnified {unified['total_fields']} fields from {len(unified['sources'])} sources:")

    for field in unified['fields'][:10]:  # Show first 10
        print(f"\n  {field['canonical_name']}:")
        print(f"    Type: {field['semantic_type']} ({field['data_type']})")
        print(f"    Sources: {field['source_mappings']}")


def example_supply_chain_alignment():
    print("\n" + "="*60)
    print("EXAMPLE 3: Supply Chain Schema Alignment")
    print("="*60)

    pipeline = PatternMatchingPipeline(use_ai=True)

    # Align supply chain data from different systems
    files = [
        'supply_chain_data/plant_a_production.csv',
        'supply_chain_data/plant_b_production.csv',
        'supply_chain_data/wms_inventory.csv'
    ]

    # Check files exist
    existing_files = [f for f in files if os.path.exists(f)]
    if not existing_files:
        print("  Supply chain data not found. Run supply_chain_data_generator.py first.")
        return

    # Analyze and align
    schemas, data_frames = pipeline.analyze_sources(existing_files)
    alignment_results = pipeline.align_schemas(schemas, data_frames)

    # Show mapping quality
    print("\n" + "="*60)
    print("MAPPING QUALITY REPORT")
    print("="*60)

    mappings = alignment_results['mappings']
    if mappings:
        avg_confidence = sum(m['confidence'] for m in mappings) / len(mappings)
        high_confidence = sum(1 for m in mappings if m['confidence'] > 0.8)

        print(f"\nTotal mappings: {len(mappings)}")
        print(f"Average confidence: {avg_confidence:.2f}")
        print(f"High confidence mappings: {high_confidence}/{len(mappings)}")

        # Show low confidence mappings
        low_confidence = [m for m in mappings if m['confidence'] < 0.6]
        if low_confidence:
            print("\nLow confidence mappings requiring review:")
            for m in low_confidence[:5]:
                print(
                    f"  {m['source_field']:30} -> {m['target_field']:30} ({m['confidence']:.2f})")


def example_sql_generation():
    print("\n" + "="*60)
    print("EXAMPLE 4: SQL View Generation")
    print("="*60)

    # Build a simple unified schema
    builder = UnifiedSchemaBuilder()

    # Mock unified schema
    unified_schema = {
        'sources': ['customers_v1', 'customers_v2'],
        'fields': [
            {
                'canonical_name': 'customer_id',
                'semantic_type': 'identifier',
                'data_type': 'string',
                'source_mappings': {
                    'customers_v1': 'Customer_ID',
                    'customers_v2': 'CustID'
                }
            },
            {
                'canonical_name': 'customer_name',
                'semantic_type': 'text',
                'data_type': 'string',
                'source_mappings': {
                    'customers_v1': 'Full_Name',
                    'customers_v2': 'CustomerName'
                }
            },
            {
                'canonical_name': 'email',
                'semantic_type': 'email',
                'data_type': 'string',
                'source_mappings': {
                    'customers_v1': 'Email_Address',
                    'customers_v2': 'ContactEmail'
                }
            }
        ]
    }

    # Generate SQL
    table_names = {
        'customers_v1': 'customer_data_v1',
        'customers_v2': 'customer_data_v2'
    }

    sql = builder.generate_sql_view(unified_schema, table_names)

    print("\nGenerated SQL View:")
    print("-" * 40)
    print(sql)


def example_ambiguity_resolution():
    print("\n" + "="*60)
    print("EXAMPLE 5: Ambiguity Resolution")
    print("="*60)

    resolver = AmbiguityResolver()

    # Mock ambiguous mappings
    mappings = [
        {'source_field': 'date', 'target_field': 'created_date', 'confidence': 0.75},
        {'source_field': 'date', 'target_field': 'modified_date', 'confidence': 0.72},
        {'source_field': 'date', 'target_field': 'order_date', 'confidence': 0.70},
        {'source_field': 'amount', 'target_field': 'total_amount', 'confidence': 0.85},
        {'source_field': 'status', 'target_field': 'order_status', 'confidence': 0.90}
    ]

    # Identify ambiguities
    ambiguities = resolver.identify_ambiguities(
        mappings, threshold_difference=0.1)

    print(f"\nFound {len(ambiguities)} ambiguous mappings:")

    for amb in ambiguities:
        print(f"\nSource field: {amb['source_field']}")
        print(f"Ambiguity score: {amb['ambiguity_score']:.2f}")
        print("Candidates:")
        for candidate in amb['candidates']:
            print(
                f"  - {candidate['target_field']} (confidence: {candidate['confidence']:.2f})")

        # Try resolution
        resolved = resolver.apply_resolution_rules(amb)
        if resolved:
            print(f"Rule-based resolution: {resolved}")
        else:
            print("Requires manual review or AI assistance")

# Main execution


def main():
    print("="*60)
    print("MODULE 3: PATTERN MATCHING & SCHEMA ALIGNMENT")
    print("="*60)
    print("\nSemantic understanding for schema alignment")

    # Show current configuration
    print(f"\nLLM Provider: {llm_interface.client.__class__.__name__}")

    # Run examples
    example_basic_pattern_matching()
    example_schema_alignment()
    example_supply_chain_alignment()
    example_sql_generation()
    example_ambiguity_resolution()

    print("\n" + "="*60)
    print("MODULE 3 COMPLETE")
    print("="*60)
    print("\nCapabilities demonstrated:")
    print("- Semantic field matching using tokens and AI")
    print("- Schema mapping with confidence scoring")
    print("- Unified schema construction from multiple sources")
    print("- Ambiguity detection and resolution")
    print("- SQL view generation for unified access")
    print("- Support for both simple and supply chain data")
    print("\nNext: Module 4 - Human-in-the-Loop Validation")


if __name__ == "__main__":
    main()
