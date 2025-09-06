# module_6_pyspark.py
# Module 6: PySpark Implementation of Hybrid Agentic Integration
# Distributed processing for large-scale data integration

import os
import json
import hashlib
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import pyspark.pandas as ps

# Import our LLM interface from earlier modules
from llm_providers import llm_interface, llm_json_call

# Part 1: Spark Session Management


class SparkIntegrationSession:
    # Manages Spark session with appropriate configuration for our use case

    def __init__(self, app_name: str = "HybridAgenticIntegration", local_mode: bool = True):
        self.app_name = app_name
        self.local_mode = local_mode
        self.spark = None
        self.broadcast_mappings = None
        self.cached_schemas = {}

    def initialize_spark(self, memory: str = "4g", cores: int = 4) -> SparkSession:
        # Initialize Spark with configuration suitable for schema matching workload

        builder = SparkSession.builder.appName(self.app_name)

        if self.local_mode:
            # Local mode for laptop/development
            builder = builder.master(f"local[{cores}]") \
                .config("spark.driver.memory", memory) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        else:
            # Cluster mode configuration
            builder = builder.config("spark.sql.shuffle.partitions", "200") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.adaptive.enabled", "true")

        # Common configurations
        builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")

        self.spark = builder.getOrCreate()
        self.spark.sparkContext.setLogLevel("WARN")

        print(f"Spark session initialized: {self.spark.version}")
        print(f"Mode: {'Local' if self.local_mode else 'Cluster'}")
        print(f"Executors: {self.get_executor_count()}")

        return self.spark

    def get_executor_count(self) -> int:
        # Get number of available executors
        sc = self.spark.sparkContext
        return len([executor for executor in sc.statusTracker().getExecutorInfos()])

    def broadcast_field_mappings(self, mappings: Dict[str, Any]) -> None:
        # Broadcast field mappings to all executors for efficient lookup

        if self.spark:
            self.broadcast_mappings = self.spark.sparkContext.broadcast(
                mappings)
            print(f"Broadcasted {len(mappings)} field mappings to executors")

    def stop(self):
        # Clean shutdown
        if self.spark:
            self.spark.stop()

# Part 2: Distributed Schema Introspection


class DistributedSchemaIntrospector:
    # Distributed version of schema introspection using Spark

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.schema_cache = {}

    def read_csv_distributed(self, file_path: str, sample_size: int = 10000) -> DataFrame:
        # Read CSV using Spark with schema inference

        # First pass: sample for schema inference
        sample_df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("samplingRatio", 0.1) \
            .csv(file_path) \
            .limit(sample_size)

        # Analyze sample to determine optimal settings
        schema_info = self.analyze_schema_sample(sample_df)

        # Second pass: read with optimized settings
        reader = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("multiLine", "true") \
            .option("escape", '"') \
            .option("nullValue", "") \
            .option("nanValue", "NaN")

        # Add delimiter if detected
        if 'delimiter' in schema_info:
            reader = reader.option("delimiter", schema_info['delimiter'])

        df = reader.csv(file_path)

        print(
            f"Loaded {file_path}: {df.count()} rows, {len(df.columns)} columns")

        return df

    def analyze_schema_sample(self, df: DataFrame) -> Dict[str, Any]:
        # Analyze schema characteristics from sample

        schema_info = {
            'columns': df.columns,
            'dtypes': dict(df.dtypes),
            'row_count': df.count(),
            'null_counts': {},
            'cardinalities': {}
        }

        # Calculate null counts and cardinality for each column
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            schema_info['null_counts'][col] = null_count

            # Approximate cardinality for categorical detection
            approx_distinct = df.agg(
                F.approx_count_distinct(col)).collect()[0][0]
            schema_info['cardinalities'][col] = approx_distinct

        return schema_info

    def profile_column_distributed(self, df: DataFrame, column: str) -> Dict[str, Any]:
        # Profile a single column using Spark operations

        total_count = df.count()

        # Basic statistics
        null_count = df.filter(F.col(column).isNull()).count()
        distinct_count = df.select(column).distinct().count()

        # Sample values for pattern analysis
        samples = df.select(column) \
            .filter(F.col(column).isNotNull()) \
            .limit(100) \
            .collect()
        sample_values = [row[column] for row in samples]

        profile = {
            'column': column,
            'total_rows': total_count,
            'null_count': null_count,
            'null_percentage': (null_count / total_count * 100) if total_count > 0 else 0,
            'distinct_count': distinct_count,
            'uniqueness': (distinct_count / total_count) if total_count > 0 else 0,
            'samples': sample_values[:10]
        }

        # Type-specific profiling
        dtype = dict(df.dtypes)[column]

        if dtype in ['int', 'bigint', 'float', 'double']:
            # Numeric profiling
            stats = df.select(
                F.min(column).alias('min'),
                F.max(column).alias('max'),
                F.mean(column).alias('mean'),
                F.stddev(column).alias('stddev')
            ).collect()[0]

            profile.update({
                'min': stats['min'],
                'max': stats['max'],
                'mean': stats['mean'],
                'stddev': stats['stddev']
            })

        elif dtype == 'string':
            # String profiling
            length_stats = df.select(
                F.min(F.length(column)).alias('min_length'),
                F.max(F.length(column)).alias('max_length'),
                F.avg(F.length(column)).alias('avg_length')
            ).collect()[0]

            profile.update({
                'min_length': length_stats['min_length'],
                'max_length': length_stats['max_length'],
                'avg_length': length_stats['avg_length']
            })

        return profile

    def infer_semantic_type_distributed(self, df: DataFrame, column: str) -> str:
        # Infer semantic type using distributed pattern matching

        # Sample data for pattern analysis
        samples = df.select(column) \
            .filter(F.col(column).isNotNull()) \
            .limit(1000)

        # Check for common patterns using Spark SQL functions
        patterns = {
            'email': samples.filter(F.col(column).rlike(r'^[^@]+@[^@]+\.[^@]+$')).count(),
            'phone': samples.filter(F.col(column).rlike(r'^\+?[\d\s\-\(\)]+$')).count(),
            'date': samples.filter(F.col(column).rlike(r'^\d{4}-\d{2}-\d{2}$')).count(),
            'identifier': samples.filter(F.col(column).rlike(r'^[A-Z]{2,}-?\d+$')).count(),
            'url': samples.filter(F.col(column).rlike(r'^https?://')).count()
        }

        total_samples = samples.count()

        # Find dominant pattern
        for pattern_type, match_count in patterns.items():
            if match_count > total_samples * 0.8:
                return pattern_type

        # Check column name hints
        col_lower = column.lower()
        if any(term in col_lower for term in ['id', 'key', 'code']):
            return 'identifier'
        elif any(term in col_lower for term in ['name', 'desc', 'title']):
            return 'text'
        elif any(term in col_lower for term in ['date', 'time', 'created', 'updated']):
            return 'timestamp'
        elif any(term in col_lower for term in ['amount', 'price', 'cost', 'total']):
            return 'monetary'
        elif any(term in col_lower for term in ['status', 'state', 'flag']):
            return 'status'

        return 'general'

# Part 3: Distributed Pattern Matching


class DistributedPatternMatcher:
    # Distributed pattern matching using Spark

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.mapping_cache = {}

    def create_field_similarity_matrix(self, source_df: DataFrame, target_df: DataFrame) -> DataFrame:
        # Create similarity matrix between source and target fields

        source_cols = source_df.columns
        target_cols = target_df.columns

        # Create cartesian product of column names
        source_col_df = self.spark.createDataFrame(
            [(col,) for col in source_cols],
            ["source_field"]
        )

        target_col_df = self.spark.createDataFrame(
            [(col,) for col in target_cols],
            ["target_field"]
        )

        # Cross join to get all combinations
        combinations = source_col_df.crossJoin(target_col_df)

        # Calculate similarities using UDF
        similarity_udf = F.udf(self.calculate_field_similarity, FloatType())

        similarity_matrix = combinations.withColumn(
            "similarity",
            similarity_udf(F.col("source_field"), F.col("target_field"))
        )

        return similarity_matrix

    def calculate_field_similarity(self, field1: str, field2: str) -> float:
        # Calculate similarity between two field names
        # This runs on executors so should be self-contained

        # Simple token-based similarity
        tokens1 = set(field1.lower().replace('_', ' ').split())
        tokens2 = set(field2.lower().replace('_', ' ').split())

        if not tokens1 or not tokens2:
            return 0.0

        intersection = tokens1 & tokens2
        union = tokens1 | tokens2

        jaccard = len(intersection) / len(union) if union else 0.0

        # Exact match bonus
        if field1.lower() == field2.lower():
            return 1.0

        # Substring bonus
        if field1.lower() in field2.lower() or field2.lower() in field1.lower():
            return max(0.7, jaccard)

        return jaccard

    def find_best_mappings_distributed(self, similarity_matrix: DataFrame, threshold: float = 0.5) -> DataFrame:
        # Find best field mappings from similarity matrix

        # Window to rank similarities for each source field
        window = Window.partitionBy(
            "source_field").orderBy(F.desc("similarity"))

        # Add rank and filter best matches
        best_mappings = similarity_matrix \
            .withColumn("rank", F.row_number().over(window)) \
            .filter((F.col("rank") == 1) & (F.col("similarity") >= threshold)) \
            .select("source_field", "target_field", "similarity")

        return best_mappings

    def validate_mappings_with_data(self, source_df: DataFrame, target_df: DataFrame,
                                    mappings_df: DataFrame) -> DataFrame:
        # Validate mappings using actual data patterns

        # Collect mappings to driver for validation
        mappings = mappings_df.collect()

        validation_results = []

        for row in mappings:
            source_field = row['source_field']
            target_field = row['target_field']
            name_similarity = row['similarity']

            # Sample data from both fields
            source_samples = source_df.select(
                source_field).limit(100).collect()
            target_samples = target_df.select(
                target_field).limit(100).collect()

            # Calculate data similarity
            data_similarity = self.calculate_data_similarity(
                source_samples, target_samples)

            # Combined confidence
            confidence = (name_similarity * 0.6) + (data_similarity * 0.4)

            validation_results.append({
                'source_field': source_field,
                'target_field': target_field,
                'name_similarity': name_similarity,
                'data_similarity': data_similarity,
                'confidence': confidence
            })

        # Convert back to DataFrame
        validated_df = self.spark.createDataFrame(validation_results)

        return validated_df

    def calculate_data_similarity(self, source_samples: List, target_samples: List) -> float:
        # Compare data patterns between samples

        if not source_samples or not target_samples:
            return 0.0

        # Extract non-null values
        source_values = [row[0]
                         for row in source_samples if row[0] is not None]
        target_values = [row[0]
                         for row in target_samples if row[0] is not None]

        if not source_values or not target_values:
            return 0.0

        # Check type compatibility
        source_types = set(type(v).__name__ for v in source_values[:10])
        target_types = set(type(v).__name__ for v in target_values[:10])

        if source_types & target_types:
            type_similarity = 1.0
        else:
            type_similarity = 0.0

        # Check value overlap for strings
        if all(isinstance(v, str) for v in source_values[:5]):
            source_set = set(source_values)
            target_set = set(target_values)

            overlap = len(source_set & target_set)
            total = len(source_set | target_set)

            value_similarity = overlap / total if total > 0 else 0.0
        else:
            value_similarity = 0.5  # Default for non-string types

        return (type_similarity + value_similarity) / 2

# Part 4: Distributed LLM Integration


class DistributedLLMProcessor:
    # Handle LLM calls in distributed fashion

    def __init__(self, spark: SparkSession, batch_size: int = 10):
        self.spark = spark
        self.batch_size = batch_size
        self.cache = {}

    def process_field_batch_with_llm(self, fields: List[str]) -> List[Dict[str, Any]]:
        # Process a batch of fields through LLM
        # This runs on executors so needs to be self-contained

        results = []

        for field in fields:
            # Check cache first
            cache_key = f"field_{field}"
            if cache_key in self.cache:
                results.append(self.cache[cache_key])
                continue

            # Call LLM for semantic understanding
            prompt = f"""
            Analyze this database field name: {field}
            
            Return JSON with:
            - canonical_name: standardized snake_case name
            - semantic_type: type of data (identifier, date, amount, etc.)
            - common_aliases: list of common alternative names
            
            JSON only:"""

            try:
                result = llm_json_call(prompt, temperature=0.1)
                if result:
                    result['original_field'] = field
                    self.cache[cache_key] = result
                    results.append(result)
                else:
                    # Fallback
                    results.append({
                        'original_field': field,
                        'canonical_name': field.lower(),
                        'semantic_type': 'unknown',
                        'common_aliases': []
                    })
            except Exception as e:
                # Handle LLM failures gracefully
                print(f"LLM error for field {field}: {e}")
                results.append({
                    'original_field': field,
                    'canonical_name': field.lower(),
                    'semantic_type': 'unknown',
                    'common_aliases': []
                })

        return results

    def distributed_semantic_analysis(self, df: DataFrame) -> DataFrame:
        # Analyze all columns semantically using distributed LLM calls

        columns = df.columns

        # Create DataFrame of column names
        columns_df = self.spark.createDataFrame(
            [(col,) for col in columns],
            ["field_name"]
        )

        # Process in batches using mapPartitions
        def process_partition(iterator):
            # Collect partition data
            fields = [row['field_name'] for row in iterator]

            if not fields:
                return []

            # Process batch with LLM
            results = self.process_field_batch_with_llm(fields)

            return results

        # Apply distributed processing
        schema = StructType([
            StructField("original_field", StringType(), True),
            StructField("canonical_name", StringType(), True),
            StructField("semantic_type", StringType(), True),
            StructField("common_aliases", ArrayType(StringType()), True)
        ])

        semantic_df = columns_df.rdd \
            .mapPartitions(process_partition) \
            .toDF(schema)

        return semantic_df

# Part 5: Unified Spark SQL Interface


class SparkUnifiedSQL:
    # Unified SQL interface using Spark SQL

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.registered_sources = {}
        self.unified_schema = None

    def register_data_source(self, source_id: str, df: DataFrame, mappings: Dict[str, str] = None):
        # Register a DataFrame as a data source

        # Apply field mappings if provided
        if mappings:
            # Rename columns to canonical names
            for original, canonical in mappings.items():
                if original in df.columns:
                    df = df.withColumnRenamed(original, canonical)

        # Register as temporary view
        df.createOrReplaceTempView(source_id)
        self.registered_sources[source_id] = df

        print(f"Registered {source_id} with {df.count()} rows")

    def create_unified_view(self, view_name: str = "unified_data"):
        # Create unified view across all sources

        if not self.registered_sources:
            raise ValueError("No sources registered")

        # Get all unique columns across sources
        all_columns = set()
        for df in self.registered_sources.values():
            all_columns.update(df.columns)

        all_columns = sorted(list(all_columns))

        # Build UNION ALL query
        union_queries = []

        for source_id, df in self.registered_sources.items():
            # Select columns, using NULL for missing ones
            select_expr = []
            for col in all_columns:
                if col in df.columns:
                    select_expr.append(f"`{col}`")
                else:
                    select_expr.append(f"NULL as `{col}`")

            # Add source identifier
            select_expr.append(f"'{source_id}' as _source")

            query = f"SELECT {', '.join(select_expr)} FROM {source_id}"
            union_queries.append(query)

        # Create unified view
        unified_query = " UNION ALL ".join(union_queries)

        self.spark.sql(
            f"CREATE OR REPLACE TEMPORARY VIEW {view_name} AS {unified_query}")

        print(f"Created unified view: {view_name}")
        print(f"Total columns: {len(all_columns)}")

        return view_name

    def execute_query(self, sql: str) -> DataFrame:
        # Execute SQL query
        return self.spark.sql(sql)

    def optimize_query_plan(self, sql: str) -> str:
        # Get optimized query plan
        df = self.spark.sql(sql)
        return df.explain(extended=True)

# Part 6: Complete Spark Integration Pipeline


class SparkIntegrationPipeline:
    # Complete pipeline using Spark for distributed processing

    def __init__(self, memory: str = "4g", cores: int = 4):
        self.session = SparkIntegrationSession()
        self.spark = self.session.initialize_spark(memory, cores)
        self.introspector = DistributedSchemaIntrospector(self.spark)
        self.matcher = DistributedPatternMatcher(self.spark)
        self.llm_processor = DistributedLLMProcessor(self.spark)
        self.sql_interface = SparkUnifiedSQL(self.spark)

    def process_data_sources(self, file_paths: List[str]) -> Dict[str, DataFrame]:
        # Load and process multiple data sources

        sources = {}

        for file_path in file_paths:
            if not os.path.exists(file_path):
                continue

            source_id = os.path.basename(file_path).split('.')[0]

            print(f"\nProcessing: {source_id}")

            # Load data
            df = self.introspector.read_csv_distributed(file_path)

            # Semantic analysis
            semantic_df = self.llm_processor.distributed_semantic_analysis(df)

            # Cache for reuse
            df.cache()
            semantic_df.cache()

            sources[source_id] = {
                'data': df,
                'semantic': semantic_df
            }

        return sources

    def align_schemas(self, sources: Dict[str, Dict[str, DataFrame]]) -> DataFrame:
        # Align schemas across sources

        if len(sources) < 2:
            print("Need at least 2 sources for alignment")
            return None

        source_ids = list(sources.keys())
        reference_id = source_ids[0]
        reference_df = sources[reference_id]['data']

        all_mappings = []

        for other_id in source_ids[1:]:
            other_df = sources[other_id]['data']

            print(f"\nAligning {other_id} to {reference_id}")

            # Create similarity matrix
            similarity_matrix = self.matcher.create_field_similarity_matrix(
                other_df, reference_df)

            # Find best mappings
            mappings = self.matcher.find_best_mappings_distributed(
                similarity_matrix)

            # Validate with data
            validated = self.matcher.validate_mappings_with_data(
                other_df, reference_df, mappings)

            # Add source identifier
            validated = validated.withColumn("source", F.lit(other_id))

            all_mappings.append(validated)

        # Combine all mappings
        if all_mappings:
            combined_mappings = all_mappings[0]
            for mapping_df in all_mappings[1:]:
                combined_mappings = combined_mappings.union(mapping_df)

            return combined_mappings

        return None

    def create_unified_interface(self, sources: Dict[str, Dict[str, DataFrame]],
                                 mappings_df: DataFrame = None):
        # Create unified SQL interface

        # Apply mappings and register sources
        for source_id, source_data in sources.items():
            df = source_data['data']

            # Get mappings for this source
            if mappings_df:
                source_mappings = mappings_df.filter(
                    F.col("source") == source_id).collect()
                mapping_dict = {row['source_field']: row['target_field']
                                for row in source_mappings}
            else:
                mapping_dict = None

            self.sql_interface.register_data_source(
                source_id, df, mapping_dict)

        # Create unified view
        self.sql_interface.create_unified_view()

    def run_query(self, sql: str) -> DataFrame:
        # Execute query on unified data
        return self.sql_interface.execute_query(sql)

# Example functions


def example_spark_basic():
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic Spark Integration")
    print("="*60)

    # Initialize pipeline
    pipeline = SparkIntegrationPipeline(memory="2g", cores=2)

    # Test data files
    files = [
        'workshop_data/customers_v1.csv',
        'workshop_data/customers_v2.csv'
    ]

    existing = [f for f in files if os.path.exists(f)]

    if len(existing) < 2:
        print("Need customer data files. Run fake_data_generator.py first.")
        pipeline.spark.stop()
        return

    # Process sources
    sources = pipeline.process_data_sources(existing)

    # Show schema info
    for source_id, source_data in sources.items():
        df = source_data['data']
        print(f"\n{source_id}: {df.count()} rows, {len(df.columns)} columns")
        df.show(5)

    pipeline.spark.stop()


def example_distributed_profiling():
    print("\n" + "="*60)
    print("EXAMPLE 2: Distributed Column Profiling")
    print("="*60)

    pipeline = SparkIntegrationPipeline()

    # Load supply chain data
    file_path = 'supply_chain_data/plant_a_production.csv'

    if not os.path.exists(file_path):
        print("Supply chain data not found. Run supply_chain_data_generator.py first.")
        pipeline.spark.stop()
        return

    # Load data
    df = pipeline.introspector.read_csv_distributed(file_path)

    # Profile each column
    print("\nColumn profiles:")
    for column in df.columns[:3]:  # First 3 columns
        profile = pipeline.introspector.profile_column_distributed(df, column)
        print(f"\n{column}:")
        print(f"  Nulls: {profile['null_percentage']:.1f}%")
        print(f"  Distinct: {profile['distinct_count']}")
        print(f"  Uniqueness: {profile['uniqueness']:.3f}")

        if 'mean' in profile:
            print(f"  Mean: {profile['mean']:.2f}")
        if 'avg_length' in profile:
            print(f"  Avg length: {profile['avg_length']:.1f}")

    pipeline.spark.stop()


def example_distributed_matching():
    print("\n" + "="*60)
    print("EXAMPLE 3: Distributed Schema Matching")
    print("="*60)

    pipeline = SparkIntegrationPipeline()

    files = [
        'workshop_data/customers_v1.csv',
        'workshop_data/customers_v2.csv'
    ]

    existing = [f for f in files if os.path.exists(f)]

    if len(existing) < 2:
        print("Need both customer files for matching.")
        pipeline.spark.stop()
        return

    # Load sources
    sources = pipeline.process_data_sources(existing)

    # Align schemas
    mappings = pipeline.align_schemas(sources)

    if mappings:
        print("\nField mappings (sorted by confidence):")
        mappings.orderBy(F.desc("confidence")).show(20, truncate=False)

        # Show low confidence mappings
        low_conf = mappings.filter(F.col("confidence") < 0.7)
        if low_conf.count() > 0:
            print("\nLow confidence mappings needing review:")
            low_conf.show(truncate=False)

    pipeline.spark.stop()


def example_unified_sql():
    print("\n" + "="*60)
    print("EXAMPLE 4: Unified SQL on Spark")
    print("="*60)

    pipeline = SparkIntegrationPipeline()

    files = [
        'workshop_data/customers_v1.csv',
        'workshop_data/customers_v2.csv',
        'workshop_data/customers_v3.csv'
    ]

    existing = [f for f in files if os.path.exists(f)]

    if not existing:
        print("No data files found.")
        pipeline.spark.stop()
        return

    # Process and align
    sources = pipeline.process_data_sources(existing)
    mappings = pipeline.align_schemas(sources) if len(sources) > 1 else None

    # Create unified interface
    pipeline.create_unified_interface(sources, mappings)

    # Run queries
    queries = [
        "SELECT COUNT(*) as total FROM unified_data",
        "SELECT _source, COUNT(*) as records FROM unified_data GROUP BY _source",
        "SELECT * FROM unified_data LIMIT 5"
    ]

    for sql in queries:
        print(f"\nQuery: {sql}")
        result = pipeline.run_query(sql)
        result.show(truncate=False)

    pipeline.spark.stop()


def example_performance_test():
    print("\n" + "="*60)
    print("EXAMPLE 5: Performance Comparison")
    print("="*60)

    pipeline = SparkIntegrationPipeline()

    # Generate larger test data
    print("\nGenerating performance test data...")

    # Create large DataFrame
    from pyspark.sql import Row
    import random

    # Generate 1M rows
    def generate_row(i):
        return Row(
            customer_id=f"CUST{i:07d}",
            name=f"Customer_{i}",
            amount=random.uniform(10, 1000),
            status=random.choice(['Active', 'Inactive', 'Pending']),
            created_date=f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
        )

    large_df = pipeline.spark.parallelize(range(1000000), 100) \
        .map(generate_row) \
        .toDF()

    large_df.createOrReplaceTempView("large_table")

    print(f"Created test data: {large_df.count()} rows")

    # Test different query patterns
    queries = [
        ("Count", "SELECT COUNT(*) FROM large_table"),
        ("Filter", "SELECT * FROM large_table WHERE status = 'Active' LIMIT 1000"),
        ("Aggregation", "SELECT status, COUNT(*), AVG(amount) FROM large_table GROUP BY status"),
        ("Complex", """
            SELECT 
                status,
                COUNT(*) as count,
                MIN(amount) as min_amt,
                MAX(amount) as max_amt,
                AVG(amount) as avg_amt
            FROM large_table
            WHERE amount > 100
            GROUP BY status
            HAVING COUNT(*) > 10000
        """)
    ]

    import time

    print("\nQuery performance:")
    for name, sql in queries:
        start = time.time()
        result = pipeline.spark.sql(sql)
        result.collect()  # Force evaluation
        elapsed = time.time() - start

        print(f"{name}: {elapsed:.3f} seconds")

    pipeline.spark.stop()

# Main execution


def main():
    print("="*60)
    print("MODULE 6: PYSPARK IMPLEMENTATION")
    print("="*60)
    print("\nDistributed processing for schema matching and integration")

    # Run examples
    example_spark_basic()
    example_distributed_profiling()
    example_distributed_matching()
    example_unified_sql()
    example_performance_test()

    print("\n" + "="*60)
    print("KEY TAKEAWAYS")
    print("="*60)
    print("\nSpark advantages for this use case:")
    print("- Handles files too large for pandas (>1GB)")
    print("- Parallel column profiling")
    print("- Distributed similarity calculations")
    print("- Scales to cluster if needed")

    print("\nSpark limitations:")
    print("- LLM calls still bottlenecked at API level")
    print("- Small data overhead not worth it (<100MB)")
    print("- Complex to manage state across executors")
    print("- Requires Spark infrastructure")

    print("\nWhen to use Spark version:")
    print("- Data sources > 1GB")
    print("- Need to process 100+ sources")
    print("- Already have Spark infrastructure")
    print("- Batch processing acceptable")


if __name__ == "__main__":
    main()
