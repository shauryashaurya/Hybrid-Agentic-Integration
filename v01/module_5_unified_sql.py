# module_5_unified_sql.py
# Module 5: Integration - Unified SQL Interface
# Query heterogeneous data sources through a unified SQL interface

import os
import json
import re
import sqlite3
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import pandas as pd
import duckdb

# Import all previous modules
from llm_providers import llm_interface, llm_json_call
from module_2_csv_introspection_v2 import CSVIntrospectionAgent
from module_3_pattern_matching import PatternMatchingPipeline, UnifiedSchemaBuilder
from module_4_human_validation import ValidationSession, ValidationPipeline

# Part 1: Data Source Registry


class DataSource:
    # Represents a single data source

    def __init__(self, source_id: str, source_type: str, location: str, schema: Dict[str, Any] = None):
        self.source_id = source_id
        self.source_type = source_type  # csv, json, xml, api
        self.location = location  # file path or URL
        self.schema = schema
        self.metadata = {
            'registered_at': datetime.now().isoformat(),
            'last_accessed': None,
            'row_count': None,
            'size_mb': None
        }
        self.field_mappings = {}  # canonical -> source field names

    def load_data(self) -> pd.DataFrame:
        # Load data based on source type

        if self.source_type == 'csv':
            # Use introspection agent for robust loading
            if 'delimiter' in self.metadata and 'encoding' in self.metadata:
                df = pd.read_csv(
                    self.location,
                    delimiter=self.metadata['delimiter'],
                    encoding=self.metadata['encoding']
                )
            else:
                df = pd.read_csv(self.location)

        elif self.source_type == 'tsv':
            df = pd.read_csv(self.location, delimiter='\t')

        elif self.source_type == 'json':
            with open(self.location, 'r') as f:
                data = json.load(f)
            # Handle nested JSON structures
            if isinstance(data, dict):
                # Look for data array in common patterns
                for key in ['data', 'records', 'items', 'products', 'warehouses']:
                    if key in data and isinstance(data[key], list):
                        df = pd.DataFrame(data[key])
                        break
                else:
                    df = pd.DataFrame([data])
            else:
                df = pd.DataFrame(data)

        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

        # Update metadata
        self.metadata['last_accessed'] = datetime.now().isoformat()
        self.metadata['row_count'] = len(df)
        self.metadata['size_mb'] = df.memory_usage(
            deep=True).sum() / 1024 / 1024

        return df

    def apply_field_mappings(self, df: pd.DataFrame) -> pd.DataFrame:
        # Rename columns to canonical names

        if self.field_mappings:
            # Reverse mapping: source -> canonical
            rename_map = {v: k for k, v in self.field_mappings.items()}
            df = df.rename(columns=rename_map)

        return df


class DataSourceRegistry:
    # Manages all registered data sources

    def __init__(self):
        self.sources = {}  # source_id -> DataSource
        self.unified_schema = None
        self.canonical_fields = set()

    def register_source(self, source_id: str, source_type: str, location: str) -> DataSource:
        # Register a new data source

        if source_id in self.sources:
            print(f"Source {source_id} already registered")
            return self.sources[source_id]

        # Create data source
        source = DataSource(source_id, source_type, location)

        # Analyze schema if CSV
        if source_type in ['csv', 'tsv']:
            agent = CSVIntrospectionAgent(use_ai=True)
            results = agent.analyze_csv(location)
            source.schema = results['schema']
            source.metadata.update({
                'encoding': results['file_metadata']['encoding'],
                'delimiter': results['file_metadata']['delimiter']
            })

        self.sources[source_id] = source
        print(f"Registered source: {source_id} ({source_type})")

        return source

    def build_unified_schema(self, validated_mappings: Dict[str, Any] = None):
        # Build unified schema from all sources

        if not self.sources:
            print("No sources registered")
            return None

        # Use schema alignment from Module 3
        pipeline = PatternMatchingPipeline(use_ai=True)

        # Collect schemas
        schemas = {}
        for source_id, source in self.sources.items():
            if source.schema:
                schemas[source_id] = source.schema

        if not schemas:
            print("No schemas available")
            return None

        # Align schemas
        alignment_results = pipeline.align_schemas(schemas)
        self.unified_schema = alignment_results['unified_schema']

        # Apply validated mappings if provided
        if validated_mappings:
            self.apply_validated_mappings(validated_mappings)

        # Update field mappings in each source
        for field in self.unified_schema['fields']:
            canonical_name = field['canonical_name']
            self.canonical_fields.add(canonical_name)

            for source_id, source_field in field['source_mappings'].items():
                if source_id in self.sources:
                    self.sources[source_id].field_mappings[canonical_name] = source_field

        print(f"Built unified schema with {len(self.canonical_fields)} fields")
        return self.unified_schema

    def apply_validated_mappings(self, validated_mappings: Dict[str, Any]):
        # Apply human-validated mappings to schema

        for mapping in validated_mappings.get('corrected_mappings', []):
            # Update field mappings with corrections
            for source in self.sources.values():
                if mapping['source'] in source.field_mappings.values():
                    # Find canonical name and update
                    for canonical, source_field in source.field_mappings.items():
                        if source_field == mapping['source']:
                            source.field_mappings[canonical] = mapping['corrected_target']
                            break

# Part 2: Query Translation Engine


class QueryTranslator:
    # Translates unified SQL to source-specific queries

    def __init__(self, registry: DataSourceRegistry):
        self.registry = registry
        self.query_cache = {}

    def parse_sql_query(self, sql: str) -> Dict[str, Any]:
        # Basic SQL parsing

        sql_lower = sql.lower()

        # Extract components
        parsed = {
            'type': None,
            'fields': [],
            'tables': [],
            'conditions': [],
            'joins': [],
            'group_by': [],
            'order_by': [],
            'limit': None
        }

        # Determine query type
        if sql_lower.startswith('select'):
            parsed['type'] = 'select'

            # Extract fields
            select_match = re.search(r'select\s+(.*?)\s+from', sql_lower)
            if select_match:
                fields_str = select_match.group(1)
                if fields_str.strip() == '*':
                    parsed['fields'] = ['*']
                else:
                    parsed['fields'] = [f.strip()
                                        for f in fields_str.split(',')]

            # Extract tables
            from_match = re.search(r'from\s+([\w,\s]+)', sql_lower)
            if from_match:
                tables_str = from_match.group(1)
                # Handle WHERE clause boundary
                if 'where' in tables_str:
                    tables_str = tables_str.split('where')[0]
                parsed['tables'] = [t.strip() for t in tables_str.split(',')]

            # Extract WHERE conditions
            where_match = re.search(
                r'where\s+(.*?)(?:group|order|limit|$)', sql_lower)
            if where_match:
                parsed['conditions'] = [where_match.group(1).strip()]

            # Extract GROUP BY
            group_match = re.search(r'group\s+by\s+([\w,\s]+)', sql_lower)
            if group_match:
                parsed['group_by'] = [g.strip()
                                      for g in group_match.group(1).split(',')]

            # Extract ORDER BY
            order_match = re.search(r'order\s+by\s+([\w,\s]+)', sql_lower)
            if order_match:
                parsed['order_by'] = [o.strip()
                                      for o in order_match.group(1).split(',')]

            # Extract LIMIT
            limit_match = re.search(r'limit\s+(\d+)', sql_lower)
            if limit_match:
                parsed['limit'] = int(limit_match.group(1))

        return parsed

    def translate_field_names(self, fields: List[str], source: DataSource) -> List[str]:
        # Translate canonical field names to source-specific names

        translated = []

        for field in fields:
            if field == '*':
                translated.append('*')
            elif field in source.field_mappings:
                # Canonical to source mapping
                source_field = source.field_mappings[field]
                translated.append(source_field)
            else:
                # Check if it's already a source field
                reverse_map = {v: k for k, v in source.field_mappings.items()}
                if field in reverse_map:
                    translated.append(field)
                else:
                    # Field not found, keep as is
                    translated.append(field)

        return translated

    def translate_conditions(self, conditions: List[str], source: DataSource) -> List[str]:
        # Translate field names in WHERE conditions

        translated = []

        for condition in conditions:
            trans_condition = condition

            # Replace canonical field names with source names
            for canonical, source_field in source.field_mappings.items():
                # Use word boundaries to avoid partial replacements
                pattern = r'\b' + re.escape(canonical) + r'\b'
                trans_condition = re.sub(
                    pattern, source_field, trans_condition)

            translated.append(trans_condition)

        return translated

# Part 3: Unified Query Executor


class UnifiedQueryExecutor:
    # Executes queries across heterogeneous sources

    def __init__(self, registry: DataSourceRegistry):
        self.registry = registry
        self.translator = QueryTranslator(registry)
        self.duckdb_conn = duckdb.connect(':memory:')
        self.loaded_tables = {}  # table_name -> DataFrame

    def load_source_as_table(self, source_id: str, table_name: str = None) -> str:
        # Load a data source as a table in DuckDB

        if source_id not in self.registry.sources:
            raise ValueError(f"Source {source_id} not found")

        source = self.registry.sources[source_id]
        table_name = table_name or source_id

        # Load data
        df = source.load_data()

        # Apply field mappings to get canonical names
        df = source.apply_field_mappings(df)

        # Register with DuckDB
        self.duckdb_conn.register(table_name, df)
        self.loaded_tables[table_name] = df

        print(f"Loaded {source_id} as table '{table_name}' ({len(df)} rows)")

        return table_name

    def create_unified_view(self, view_name: str = 'unified_data') -> str:
        # Create a unified view across all sources

        if not self.registry.sources:
            raise ValueError("No sources registered")

        # Load all sources
        for source_id in self.registry.sources:
            self.load_source_as_table(source_id)

        # Get canonical fields
        canonical_fields = list(self.registry.canonical_fields)

        # Build UNION ALL query
        union_parts = []

        for source_id, source in self.registry.sources.items():
            select_parts = []

            for field in canonical_fields:
                if field in source.field_mappings:
                    # Field exists in this source
                    source_field = source.field_mappings[field]
                    # Check if field exists in loaded DataFrame
                    if source_field in self.loaded_tables[source_id].columns:
                        select_parts.append(f'"{source_field}" AS {field}')
                    else:
                        select_parts.append(f'NULL AS {field}')
                else:
                    select_parts.append(f'NULL AS {field}')

            # Add source identifier
            select_parts.append(f"'{source_id}' AS _source")

            query_part = f"SELECT {', '.join(select_parts)} FROM {source_id}"
            union_parts.append(query_part)

        # Create view
        create_view_sql = f"CREATE OR REPLACE VIEW {view_name} AS\n" + \
            "\nUNION ALL\n".join(union_parts)

        self.duckdb_conn.execute(create_view_sql)

        print(
            f"Created unified view '{view_name}' with {len(canonical_fields)} fields")

        return view_name

    def execute_query(self, sql: str) -> pd.DataFrame:
        # Execute a SQL query on unified data

        try:
            result = self.duckdb_conn.execute(sql).fetchdf()
            return result
        except Exception as e:
            print(f"Query execution error: {e}")
            return pd.DataFrame()

    def explain_query_plan(self, sql: str) -> str:
        # Explain how the query will be executed

        try:
            plan = self.duckdb_conn.execute(f"EXPLAIN {sql}").fetchall()
            return '\n'.join([str(row) for row in plan])
        except Exception as e:
            return f"Could not explain query: {e}"

# Part 4: Query Intelligence with AI


class QueryAssistant:
    # AI-powered query assistance

    def __init__(self, executor: UnifiedQueryExecutor):
        self.executor = executor
        self.query_history = []

    def natural_language_to_sql(self, nl_query: str, schema_context: Dict[str, Any]) -> str:
        # Convert natural language query to SQL

        # Build schema description for context
        fields_desc = []
        if schema_context and 'fields' in schema_context:
            for field in schema_context['fields'][:20]:  # Limit for prompt size
                fields_desc.append(
                    f"- {field['canonical_name']} ({field['semantic_type']})")

        fields_str = '\n'.join(fields_desc)

        prompt = f"""
        Convert this natural language query to SQL:
        
        Query: {nl_query}
        
        Available fields:
        {fields_str}
        
        Table name: unified_data
        
        Return only the SQL query, no explanation.
        SQL:"""

        sql = llm_json_call(prompt, temperature=0.1)

        if isinstance(sql, dict):
            # Extract SQL from JSON response
            sql = sql.get('sql', '') or sql.get('query', '')

        # Clean up the response
        sql = sql.strip()
        if sql.startswith('```'):
            # Remove markdown code blocks
            sql = sql.split('```')[1]
            if sql.startswith('sql'):
                sql = sql[3:]

        return sql.strip()

    def suggest_query_optimization(self, sql: str, execution_time: float) -> str:
        # Suggest query optimizations

        if execution_time < 0.1:
            return "Query is already fast"

        suggestions = []

        sql_lower = sql.lower()

        # Check for SELECT *
        if 'select *' in sql_lower:
            suggestions.append(
                "Consider selecting only needed columns instead of *")

        # Check for missing WHERE clause
        if 'where' not in sql_lower and 'select' in sql_lower:
            suggestions.append("Add WHERE clause to filter data early")

        # Check for LIMIT
        if 'limit' not in sql_lower:
            suggestions.append("Add LIMIT clause if you don't need all rows")

        return '\n'.join(suggestions) if suggestions else "No obvious optimizations found"

    def generate_sample_queries(self, schema: Dict[str, Any]) -> List[str]:
        # Generate sample queries based on schema

        queries = []

        if not schema or 'fields' not in schema:
            return queries

        fields = schema['fields']

        # Basic SELECT query
        if fields:
            field_names = [f['canonical_name'] for f in fields[:5]]
            queries.append(
                f"SELECT {', '.join(field_names)} FROM unified_data LIMIT 10")

        # Count query
        queries.append("SELECT COUNT(*) as total_records FROM unified_data")

        # Group by query for status/category fields
        for field in fields:
            if field['semantic_type'] in ['status', 'category']:
                queries.append(
                    f"SELECT {field['canonical_name']}, COUNT(*) as count " +
                    f"FROM unified_data GROUP BY {field['canonical_name']}"
                )
                break

        # Filter query for identifiers
        for field in fields:
            if field['semantic_type'] == 'identifier':
                queries.append(
                    f"SELECT * FROM unified_data WHERE {field['canonical_name']} IS NOT NULL LIMIT 5"
                )
                break

        # Aggregation query for quantities
        for field in fields:
            if field['semantic_type'] in ['quantity', 'monetary']:
                queries.append(
                    f"SELECT SUM({field['canonical_name']}) as total, " +
                    f"AVG({field['canonical_name']}) as average FROM unified_data"
                )
                break

        return queries

# Part 5: Complete Integration Pipeline


class HybridAgenticIntegration:
    # Complete integration system bringing all modules together

    def __init__(self):
        self.registry = DataSourceRegistry()
        self.executor = None
        self.assistant = None
        self.validation_session = None

    def setup_from_files(self, file_paths: List[str], auto_validate: bool = True):
        # Setup integration from file list

        print("\n" + "="*60)
        print("SETTING UP HYBRID AGENTIC INTEGRATION")
        print("="*60)

        # Register all sources
        for file_path in file_paths:
            if os.path.exists(file_path):
                source_id = os.path.basename(file_path).replace(
                    '.csv', '').replace('.tsv', '').replace('.json', '')

                # Determine type
                if file_path.endswith('.csv'):
                    source_type = 'csv'
                elif file_path.endswith('.tsv'):
                    source_type = 'tsv'
                elif file_path.endswith('.json'):
                    source_type = 'json'
                else:
                    continue

                self.registry.register_source(
                    source_id, source_type, file_path)

        # Build unified schema
        print("\nBuilding unified schema...")
        self.registry.build_unified_schema()

        # Auto-validate high confidence mappings
        if auto_validate:
            self.auto_validate_mappings()

        # Create query executor
        self.executor = UnifiedQueryExecutor(self.registry)
        self.assistant = QueryAssistant(self.executor)

        # Create unified view
        self.executor.create_unified_view()

        print("\nIntegration setup complete!")

    def auto_validate_mappings(self):
        # Automatically validate high-confidence mappings

        print("\nAuto-validating high confidence mappings...")

        validated = 0
        for source in self.registry.sources.values():
            for canonical, source_field in source.field_mappings.items():
                # Auto-approve exact matches
                if canonical.lower() == source_field.lower():
                    validated += 1

        print(f"Auto-validated {validated} exact match mappings")

    def execute_unified_query(self, sql: str) -> pd.DataFrame:
        # Execute query on unified data

        if not self.executor:
            print("Integration not setup. Run setup_from_files first.")
            return pd.DataFrame()

        print(f"\nExecuting: {sql}")

        start_time = datetime.now()
        result = self.executor.execute_query(sql)
        execution_time = (datetime.now() - start_time).total_seconds()

        print(f"Returned {len(result)} rows in {execution_time:.3f} seconds")

        # Get optimization suggestions if slow
        if execution_time > 1.0:
            suggestions = self.assistant.suggest_query_optimization(
                sql, execution_time)
            if suggestions:
                print(f"\nOptimization suggestions:\n{suggestions}")

        return result

    def query_with_natural_language(self, nl_query: str) -> pd.DataFrame:
        # Query using natural language

        if not self.assistant:
            print("Assistant not initialized")
            return pd.DataFrame()

        print(f"\nNatural language query: {nl_query}")

        # Convert to SQL
        sql = self.assistant.natural_language_to_sql(
            nl_query, self.registry.unified_schema)

        if sql:
            print(f"Generated SQL: {sql}")
            return self.execute_unified_query(sql)
        else:
            print("Could not generate SQL from natural language query")
            return pd.DataFrame()

    def show_unified_schema(self):
        # Display the unified schema

        if not self.registry.unified_schema:
            print("No unified schema available")
            return

        print("\n" + "="*60)
        print("UNIFIED SCHEMA")
        print("="*60)

        schema = self.registry.unified_schema
        print(f"\nTotal fields: {len(schema['fields'])}")
        print(f"Data sources: {', '.join(schema['sources'])}")

        print("\nField mappings:")
        for field in schema['fields'][:15]:  # Show first 15
            print(f"\n{field['canonical_name']} ({field['semantic_type']}):")
            for source, source_field in field['source_mappings'].items():
                print(f"  {source}: {source_field}")

# Example functions


def example_basic_integration():
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic Integration Setup")
    print("="*60)

    # Create integration system
    integration = HybridAgenticIntegration()

    # Setup with customer files
    files = [
        'workshop_data/customers_v1.csv',
        'workshop_data/customers_v2.csv'
    ]

    existing = [f for f in files if os.path.exists(f)]

    if len(existing) < 2:
        print("Need customer data files. Run fake_data_generator.py first.")
        return

    # Setup integration
    integration.setup_from_files(existing)

    # Show schema
    integration.show_unified_schema()

    # Execute sample query
    sql = "SELECT customer_id, customer_name FROM unified_data LIMIT 5"
    result = integration.execute_unified_query(sql)

    if not result.empty:
        print("\nQuery results:")
        print(result)


def example_supply_chain_integration():
    print("\n" + "="*60)
    print("EXAMPLE 2: Supply Chain Integration")
    print("="*60)

    integration = HybridAgenticIntegration()

    # Setup with supply chain files
    files = [
        'supply_chain_data/plant_a_production.csv',
        'supply_chain_data/plant_b_production.csv',
        'supply_chain_data/wms_inventory.csv'
    ]

    existing = [f for f in files if os.path.exists(f)]

    if not existing:
        print("Supply chain data not found. Run supply_chain_data_generator.py first.")
        return

    # Setup integration
    integration.setup_from_files(existing)

    # Execute aggregation query
    sql = """
    SELECT 
        _source as data_source,
        COUNT(*) as record_count
    FROM unified_data
    GROUP BY _source
    """

    result = integration.execute_unified_query(sql)

    if not result.empty:
        print("\nRecords per source:")
        print(result)


def example_natural_language_queries():
    print("\n" + "="*60)
    print("EXAMPLE 3: Natural Language Queries")
    print("="*60)

    integration = HybridAgenticIntegration()

    # Setup with available files
    files = [
        'workshop_data/customers_v1.csv',
        'workshop_data/orders.csv'
    ]

    existing = [f for f in files if os.path.exists(f)]

    if not existing:
        print("Data files not found. Run fake_data_generator.py first.")
        return

    integration.setup_from_files(existing)

    # Test natural language queries
    nl_queries = [
        "Show me all customers",
        "Count total orders",
        "What are the different order statuses?"
    ]

    for nl_query in nl_queries:
        result = integration.query_with_natural_language(nl_query)
        if not result.empty:
            print(f"\nResults for '{nl_query}':")
            print(result.head())


def example_complex_analytics():
    print("\n" + "="*60)
    print("EXAMPLE 4: Complex Analytics Query")
    print("="*60)

    integration = HybridAgenticIntegration()

    # Setup with mixed data
    files = [
        'workshop_data/customers_v1.csv',
        'workshop_data/customers_v2.csv',
        'workshop_data/customers_v3.csv',
        'workshop_data/orders.csv'
    ]

    existing = [f for f in files if os.path.exists(f)]

    if len(existing) < 2:
        print("Insufficient data files for complex analytics.")
        return

    integration.setup_from_files(existing)

    # Complex analytical query
    sql = """
    SELECT 
        _source,
        COUNT(*) as total_records,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM unified_data
    WHERE customer_id IS NOT NULL
    GROUP BY _source
    ORDER BY total_records DESC
    """

    result = integration.execute_unified_query(sql)

    if not result.empty:
        print("\nAnalytics results:")
        print(result)

    # Show query plan
    print("\nQuery execution plan:")
    plan = integration.executor.explain_query_plan(sql)
    print(plan[:500])  # Show first 500 chars


def example_performance_comparison():
    print("\n" + "="*60)
    print("EXAMPLE 5: Performance Comparison")
    print("="*60)

    integration = HybridAgenticIntegration()

    # Load multiple sources
    files = [
        'workshop_data/customers_v1.csv',
        'workshop_data/customers_v2.csv',
        'workshop_data/orders.csv'
    ]

    existing = [f for f in files if os.path.exists(f)]

    if not existing:
        print("Data files not found.")
        return

    integration.setup_from_files(existing)

    # Compare query approaches
    print("\nComparing query approaches:")

    # 1. Simple query
    sql1 = "SELECT COUNT(*) FROM unified_data"
    result1 = integration.execute_unified_query(sql1)

    # 2. Filtered query
    sql2 = "SELECT COUNT(*) FROM unified_data WHERE customer_id IS NOT NULL"
    result2 = integration.execute_unified_query(sql2)

    # 3. Aggregation query
    sql3 = """
    SELECT 
        _source,
        COUNT(*) as cnt
    FROM unified_data
    GROUP BY _source
    """
    result3 = integration.execute_unified_query(sql3)

    # Generate sample queries
    print("\nSample queries for this schema:")
    samples = integration.assistant.generate_sample_queries(
        integration.registry.unified_schema)
    for i, sample in enumerate(samples[:3], 1):
        print(f"{i}. {sample}")


def example_interactive_query_session():
    print("\n" + "="*60)
    print("EXAMPLE 6: Interactive Query Session")
    print("="*60)

    integration = HybridAgenticIntegration()

    # Setup with available data
    all_files = []

    # Add customer files
    for i in range(1, 4):
        f = f'workshop_data/customers_v{i}.csv'
        if os.path.exists(f):
            all_files.append(f)

    # Add supply chain files
    supply_files = [
        'supply_chain_data/plant_a_production.csv',
        'supply_chain_data/wms_inventory.csv'
    ]

    for f in supply_files:
        if os.path.exists(f):
            all_files.append(f)

    if not all_files:
        print("No data files found.")
        return

    print(f"Loading {len(all_files)} data sources...")
    integration.setup_from_files(all_files)

    # Show available fields
    print("\nAvailable fields in unified schema:")
    fields = [f['canonical_name']
              for f in integration.registry.unified_schema['fields'][:20]]
    print(', '.join(fields))

    # Interactive query loop
    print("\nInteractive Query Mode (type 'exit' to quit)")
    print("You can use SQL or natural language")

    while True:
        query = input("\nEnter query: ").strip()

        if query.lower() == 'exit':
            break

        if query.lower().startswith('select'):
            # SQL query
            result = integration.execute_unified_query(query)
        else:
            # Try as natural language
            result = integration.query_with_natural_language(query)

        if not result.empty:
            print("\nResults:")
            print(result.head(10))
            print(f"... ({len(result)} total rows)")

# Main execution


def main():
    print("="*60)
    print("MODULE 5: UNIFIED SQL INTERFACE")
    print("="*60)
    print("\nIntegration of all components for unified data access")

    # Run examples
    example_basic_integration()
    example_supply_chain_integration()
    example_natural_language_queries()
    example_complex_analytics()
    example_performance_comparison()

    # Offer interactive session
    print("\n" + "="*60)
    print("INTERACTIVE MODE")
    print("="*60)

    response = input(
        "\nWould you like to try the interactive query session? (y/n): ")

    if response.lower() == 'y':
        example_interactive_query_session()

    print("\n" + "="*60)
    print("WORKSHOP COMPLETE!")
    print("="*60)
    print("\nYou've learned to build a Hybrid Agentic Integration Architecture:")
    print("1. AI agents for understanding data semantics")
    print("2. Robust CSV introspection and schema extraction")
    print("3. Pattern matching for schema alignment")
    print("4. Human-in-the-loop validation")
    print("5. Unified SQL interface over heterogeneous sources")
    print("\nKey capabilities demonstrated:")
    print("- Multi-LLM support (Ollama, OpenAI, etc.)")
    print("- Handles inconsistent formats and naming")
    print("- Learns from human feedback")
    print("- Natural language to SQL conversion")
    print("- Works with both simple and complex supply chain data")
    print("\nThank you for participating!")


if __name__ == "__main__":
    main()
