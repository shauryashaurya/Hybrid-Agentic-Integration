# module_8_palantir_foundry.py
# Module 8: Palantir Foundry Implementation
# Adapting semantic integration to Foundry's paradigms

from typing import List, Dict, Any, Optional, Set
from datetime import datetime
import json
import pandas as pd

# Note: In actual Foundry, you'd import from transforms and foundry_ml
# These are simulated interfaces for demonstration


class Transform:
    """Simulated Foundry Transform decorator"""
    pass


class Dataset:
    """Simulated Foundry Dataset"""
    pass


class OntologyType:
    """Simulated Ontology Type"""
    pass

# Part 1: Foundry Dataset Integration


class FoundryDatasetAdapter:
    # Adapts our schema matching to Foundry datasets

    def __init__(self, dataset_rid: str):
        self.dataset_rid = dataset_rid
        self.schema = None
        self.branch = "master"
        self.transaction_rid = None

    def read_schema(self) -> Dict[str, Any]:
        # In Foundry, this would use the Catalog API
        # Returns schema in Foundry format

        foundry_schema = {
            "datasetRid": self.dataset_rid,
            "schema": {
                "fieldSchemaList": [
                    {
                        "name": "customer_id",
                        "type": "STRING",
                        "nullable": True,
                        "userDefinedTypeClass": None
                    },
                    {
                        "name": "amount",
                        "type": "DOUBLE",
                        "nullable": True,
                        "userDefinedTypeClass": None
                    }
                ],
                "primaryKey": ["customer_id"],
                "dataFrameReaderClass": "com.palantir.foundry.spark.input.ParquetDataFrameReader"
            },
            "metadata": {
                "modified": "2024-01-15T10:00:00Z",
                "modifiedBy": "user@company.com"
            }
        }

        return foundry_schema

    def to_unified_schema(self, foundry_schema: Dict[str, Any]) -> Dict[str, Any]:
        # Convert Foundry schema to our unified format

        fields = []
        for field in foundry_schema["schema"]["fieldSchemaList"]:
            fields.append({
                "name": field["name"],
                "type": self.map_foundry_type(field["type"]),
                "nullable": field.get("nullable", True),
                "foundry_type": field["type"]
            })

        return {
            "dataset_rid": foundry_schema["datasetRid"],
            "fields": fields,
            "primary_key": foundry_schema["schema"].get("primaryKey", [])
        }

    def map_foundry_type(self, foundry_type: str) -> str:
        # Map Foundry types to our semantic types

        type_mapping = {
            "STRING": "string",
            "INTEGER": "integer",
            "LONG": "integer",
            "DOUBLE": "float",
            "FLOAT": "float",
            "BOOLEAN": "boolean",
            "DATE": "date",
            "TIMESTAMP": "timestamp",
            "ARRAY": "array",
            "STRUCT": "struct",
            "MAP": "map"
        }

        return type_mapping.get(foundry_type, "unknown")

# Part 2: Pipeline Builder Pattern


class PipelineBuilderIntegration:
    # Integrate with Foundry's Pipeline Builder

    def __init__(self):
        self.nodes = []
        self.edges = []
        self.transforms = {}

    def create_schema_matching_pipeline(self, source_datasets: List[str],
                                        target_dataset: str) -> Dict[str, Any]:
        # Create a Pipeline Builder compatible pipeline

        pipeline = {
            "pipelineRid": f"ri.pipelines.{datetime.now().timestamp()}",
            "name": "Schema Matching Pipeline",
            "nodes": [],
            "edges": [],
            "configuration": {
                "sparkConfiguration": {
                    "driverMemory": "4g",
                    "executorMemory": "4g",
                    "dynamicAllocation": True
                }
            }
        }

        # Add source nodes
        for idx, source in enumerate(source_datasets):
            node = {
                "nodeId": f"source_{idx}",
                "type": "DATASET_INPUT",
                "configuration": {
                    "datasetRid": source
                }
            }
            pipeline["nodes"].append(node)

        # Add schema introspection transform
        introspection_node = {
            "nodeId": "schema_introspection",
            "type": "TRANSFORM",
            "configuration": {
                "transformType": "PYTHON",
                "code": self.generate_introspection_code()
            }
        }
        pipeline["nodes"].append(introspection_node)

        # Add pattern matching transform
        matching_node = {
            "nodeId": "pattern_matching",
            "type": "TRANSFORM",
            "configuration": {
                "transformType": "PYTHON",
                "code": self.generate_matching_code()
            }
        }
        pipeline["nodes"].append(matching_node)

        # Add output node
        output_node = {
            "nodeId": "output",
            "type": "DATASET_OUTPUT",
            "configuration": {
                "datasetRid": target_dataset
            }
        }
        pipeline["nodes"].append(output_node)

        # Connect nodes
        for idx in range(len(source_datasets)):
            pipeline["edges"].append({
                "from": f"source_{idx}",
                "to": "schema_introspection"
            })

        pipeline["edges"].extend([
            {"from": "schema_introspection", "to": "pattern_matching"},
            {"from": "pattern_matching", "to": "output"}
        ])

        return pipeline

    def generate_introspection_code(self) -> str:
        # Generate Foundry-compatible transform code

        code = '''
from transforms.api import transform, Input, Output
from pyspark.sql import functions as F

@transform(
    output=Output("/path/to/introspected_schemas"),
    source1=Input("/path/to/source1"),
    source2=Input("/path/to/source2")
)
def introspect_schemas(output, source1, source2):
    # Analyze source schemas
    schema1 = source1.schema
    schema2 = source2.schema
    
    # Create schema comparison DataFrame
    comparison_data = []
    
    for field in schema1.fields:
        comparison_data.append({
            'source': 'source1',
            'field_name': field.name,
            'field_type': str(field.dataType),
            'nullable': field.nullable
        })
    
    for field in schema2.fields:
        comparison_data.append({
            'source': 'source2',
            'field_name': field.name,
            'field_type': str(field.dataType),
            'nullable': field.nullable
        })
    
    comparison_df = output.spark.createDataFrame(comparison_data)
    output.write_dataframe(comparison_df)
'''
        return code

    def generate_matching_code(self) -> str:
        # Generate pattern matching transform code

        code = '''
from transforms.api import transform, Input, Output
from pyspark.sql import functions as F

@transform(
    output=Output("/path/to/field_mappings"),
    schemas=Input("/path/to/introspected_schemas")
)
def match_patterns(output, schemas):
    # Read schemas
    df = schemas.dataframe()
    
    # Self-join to create field pairs
    df1 = df.filter(F.col("source") == "source1").alias("s1")
    df2 = df.filter(F.col("source") == "source2").alias("s2")
    
    # Calculate similarity (simplified)
    pairs = df1.crossJoin(df2)
    
    # Add similarity score using UDF
    from pyspark.sql.types import FloatType
    
    def calculate_similarity(field1, field2):
        # Token-based similarity
        tokens1 = set(field1.lower().split('_'))
        tokens2 = set(field2.lower().split('_'))
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = tokens1 & tokens2
        union = tokens1 | tokens2
        
        return float(len(intersection)) / float(len(union))
    
    similarity_udf = F.udf(calculate_similarity, FloatType())
    
    mappings = pairs.withColumn(
        "similarity",
        similarity_udf(F.col("s1.field_name"), F.col("s2.field_name"))
    ).filter(F.col("similarity") > 0.5)
    
    output.write_dataframe(mappings)
'''
        return code

# Part 3: Code Repository Integration


class CodeRepositoryIntegration:
    # Integrate with Foundry Code Repositories

    def __init__(self, repository_rid: str):
        self.repository_rid = repository_rid
        self.branch = "master"

    def create_semantic_matching_library(self) -> Dict[str, str]:
        # Create library structure for Code Repository

        files = {
            "setup.py": self.generate_setup_py(),
            "semantic_matcher/__init__.py": self.generate_init_py(),
            "semantic_matcher/introspector.py": self.generate_introspector_py(),
            "semantic_matcher/matcher.py": self.generate_matcher_py(),
            "semantic_matcher/transforms.py": self.generate_transforms_py(),
            "tests/test_matcher.py": self.generate_tests_py()
        }

        return files

    def generate_setup_py(self) -> str:
        return '''
from setuptools import setup, find_packages

setup(
    name="semantic-matcher",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.0.0",
        "pyspark>=3.0.0"
    ],
    python_requires=">=3.8"
)
'''

    def generate_init_py(self) -> str:
        return '''
from .introspector import SchemaIntrospector
from .matcher import PatternMatcher

__all__ = ["SchemaIntrospector", "PatternMatcher"]
'''

    def generate_introspector_py(self) -> str:
        return '''
from pyspark.sql import DataFrame
from typing import Dict, Any

class SchemaIntrospector:
    """Introspect Foundry dataset schemas"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def analyze_dataset(self, df: DataFrame) -> Dict[str, Any]:
        """Analyze dataset schema and statistics"""
        
        schema_info = {
            "fields": [],
            "row_count": df.count(),
            "statistics": {}
        }
        
        for field in df.schema.fields:
            field_info = {
                "name": field.name,
                "type": str(field.dataType),
                "nullable": field.nullable
            }
            
            # Calculate statistics
            if field.dataType.simpleString() in ["int", "bigint", "double"]:
                stats = df.select(
                    F.min(field.name).alias("min"),
                    F.max(field.name).alias("max"),
                    F.avg(field.name).alias("avg")
                ).collect()[0]
                
                field_info["statistics"] = {
                    "min": stats["min"],
                    "max": stats["max"],
                    "avg": stats["avg"]
                }
            
            schema_info["fields"].append(field_info)
        
        return schema_info
'''

    def generate_matcher_py(self) -> str:
        return '''
from typing import List, Dict, Tuple

class PatternMatcher:
    """Match fields between schemas"""
    
    def __init__(self):
        self.mappings = {}
    
    def calculate_similarity(self, field1: str, field2: str) -> float:
        """Calculate field name similarity"""
        
        # Exact match
        if field1.lower() == field2.lower():
            return 1.0
        
        # Token overlap
        tokens1 = set(field1.lower().replace('_', ' ').split())
        tokens2 = set(field2.lower().replace('_', ' ').split())
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = tokens1 & tokens2
        union = tokens1 | tokens2
        
        return len(intersection) / len(union)
    
    def find_best_matches(self, source_fields: List[str], 
                         target_fields: List[str], 
                         threshold: float = 0.5) -> List[Dict[str, Any]]:
        """Find best field matches"""
        
        matches = []
        
        for source in source_fields:
            best_match = None
            best_score = 0.0
            
            for target in target_fields:
                score = self.calculate_similarity(source, target)
                
                if score > best_score and score >= threshold:
                    best_score = score
                    best_match = target
            
            if best_match:
                matches.append({
                    "source": source,
                    "target": best_match,
                    "confidence": best_score
                })
        
        return matches
'''

    def generate_transforms_py(self) -> str:
        return '''
from transforms.api import transform, Input, Output
from semantic_matcher import SchemaIntrospector, PatternMatcher

@transform(
    output=Output("/path/to/unified_schema"),
    source1=Input("/path/to/source1"),
    source2=Input("/path/to/source2")
)
def create_unified_schema(output, source1, source2):
    """Create unified schema from multiple sources"""
    
    spark = output.spark
    introspector = SchemaIntrospector(spark)
    matcher = PatternMatcher()
    
    # Analyze schemas
    schema1 = introspector.analyze_dataset(source1.dataframe())
    schema2 = introspector.analyze_dataset(source2.dataframe())
    
    # Find mappings
    fields1 = [f["name"] for f in schema1["fields"]]
    fields2 = [f["name"] for f in schema2["fields"]]
    
    mappings = matcher.find_best_matches(fields1, fields2)
    
    # Create unified view
    df1 = source1.dataframe()
    df2 = source2.dataframe()
    
    # Rename columns based on mappings
    for mapping in mappings:
        if mapping["confidence"] > 0.8:
            df2 = df2.withColumnRenamed(mapping["target"], mapping["source"])
    
    # Union datasets
    unified = df1.unionByName(df2, allowMissingColumns=True)
    
    output.write_dataframe(unified)
'''

    def generate_tests_py(self) -> str:
        return '''
import pytest
from semantic_matcher import PatternMatcher

def test_exact_match():
    matcher = PatternMatcher()
    assert matcher.calculate_similarity("customer_id", "customer_id") == 1.0

def test_case_insensitive():
    matcher = PatternMatcher()
    assert matcher.calculate_similarity("Customer_ID", "customer_id") == 1.0

def test_token_overlap():
    matcher = PatternMatcher()
    score = matcher.calculate_similarity("customer_id", "cust_id")
    assert score > 0.5
'''

# Part 4: Ontology Integration


class OntologyIntegration:
    # Map to Foundry Ontology

    def __init__(self):
        self.object_types = {}
        self.link_types = {}
        self.action_types = {}

    def create_object_type_from_unified_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
        # Create Ontology object type from unified schema

        object_type = {
            "apiName": "UnifiedCustomer",
            "displayName": "Unified Customer",
            "description": "Unified view of customer across sources",
            "properties": [],
            "primaryKey": "customer_id",
            "titleProperty": "customer_name"
        }

        # Map fields to properties
        for field in schema.get("fields", []):
            property_def = {
                "apiName": field["name"],
                "displayName": self.to_display_name(field["name"]),
                "dataType": self.map_to_ontology_type(field["type"]),
                "nullable": field.get("nullable", True)
            }

            # Add semantic metadata
            if field.get("semantic_type"):
                property_def["metadata"] = {
                    "semanticType": field["semantic_type"]
                }

            object_type["properties"].append(property_def)

        return object_type

    def to_display_name(self, field_name: str) -> str:
        # Convert field name to display name
        words = field_name.replace('_', ' ').split()
        return ' '.join(word.capitalize() for word in words)

    def map_to_ontology_type(self, schema_type: str) -> str:
        # Map schema types to Ontology types

        type_mapping = {
            "string": "STRING",
            "integer": "INTEGER",
            "float": "DOUBLE",
            "boolean": "BOOLEAN",
            "date": "DATE",
            "timestamp": "TIMESTAMP",
            "identifier": "STRING",
            "monetary": "DECIMAL",
            "email": "STRING",
            "phone": "STRING"
        }

        return type_mapping.get(schema_type, "STRING")

    def create_writeback_dataset(self, object_type: Dict[str, Any]) -> Dict[str, Any]:
        # Create writeback dataset configuration

        writeback = {
            "datasetRid": f"ri.datasets.{object_type['apiName'].lower()}",
            "objectType": object_type["apiName"],
            "mappings": {}
        }

        # Map properties to columns
        for prop in object_type["properties"]:
            writeback["mappings"][prop["apiName"]] = {
                "columnName": prop["apiName"],
                "propertyType": prop["dataType"]
            }

        return writeback

# Part 5: Workshop Integration


class WorkshopIntegration:
    # Integrate with Foundry Workshop (UI builder)

    def __init__(self):
        self.widgets = []
        self.variables = {}
        self.functions = {}

    def create_schema_validation_app(self) -> Dict[str, Any]:
        # Create Workshop app for schema validation

        app = {
            "applicationRid": f"ri.workshop.app.{datetime.now().timestamp()}",
            "name": "Schema Mapping Validator",
            "pages": [],
            "variables": {},
            "functions": {}
        }

        # Main page
        main_page = {
            "pageId": "main",
            "layout": {
                "type": "FLEX",
                "direction": "COLUMN",
                "widgets": []
            }
        }

        # Object Table widget for mappings
        mapping_table = {
            "widgetId": "mapping_table",
            "type": "OBJECT_TABLE",
            "configuration": {
                "objectType": "FieldMapping",
                "columns": [
                    {"property": "sourceField", "width": 200},
                    {"property": "targetField", "width": 200},
                    {"property": "confidence", "width": 100},
                    {"property": "status", "width": 150}
                ],
                "enableSelection": True,
                "enableFiltering": True
            }
        }
        main_page["layout"]["widgets"].append(mapping_table)

        # Confidence chart widget
        confidence_chart = {
            "widgetId": "confidence_chart",
            "type": "CHART",
            "configuration": {
                "chartType": "HISTOGRAM",
                "data": {
                    "objectSet": "FieldMapping",
                    "aggregation": {
                        "groupBy": ["confidence"],
                        "count": True
                    }
                },
                "xAxis": {"property": "confidence"},
                "yAxis": {"aggregation": "count"}
            }
        }
        main_page["layout"]["widgets"].append(confidence_chart)

        # Validation form
        validation_form = {
            "widgetId": "validation_form",
            "type": "FORM",
            "configuration": {
                "fields": [
                    {
                        "name": "status",
                        "type": "DROPDOWN",
                        "options": ["Approved", "Rejected", "Needs Review"]
                    },
                    {
                        "name": "notes",
                        "type": "TEXT_AREA"
                    }
                ],
                "submitAction": "validateMapping"
            }
        }
        main_page["layout"]["widgets"].append(validation_form)

        app["pages"].append(main_page)

        # Add validation function
        app["functions"]["validateMapping"] = {
            "inputs": ["selectedMapping", "status", "notes"],
            "code": '''
function validateMapping(mapping, status, notes) {
    // Update mapping status
    mapping.status = status;
    mapping.validationNotes = notes;
    mapping.validatedBy = currentUser();
    mapping.validatedAt = new Date();
    
    // Save to dataset
    Actions.updateFieldMapping(mapping);
    
    // Refresh table
    Variables.refreshTable.set(true);
}
'''
        }

        return app

# Part 6: Foundry ML Integration


class FoundryMLIntegration:
    # Integrate with Foundry ML for model deployment

    def __init__(self):
        self.models = {}
        self.model_versions = {}

    def create_semantic_matching_model(self) -> Dict[str, Any]:
        # Define model for semantic matching

        model_definition = {
            "modelRid": f"ri.models.semantic_matcher",
            "name": "Semantic Field Matcher",
            "modelType": "PYTHON_FUNCTION",
            "inputs": [
                {"name": "source_field", "type": "STRING"},
                {"name": "target_field", "type": "STRING"},
                {"name": "source_samples", "type": "ARRAY<STRING>"},
                {"name": "target_samples", "type": "ARRAY<STRING>"}
            ],
            "outputs": [
                {"name": "similarity_score", "type": "DOUBLE"},
                {"name": "confidence", "type": "DOUBLE"},
                {"name": "semantic_type", "type": "STRING"}
            ]
        }

        return model_definition

    def generate_model_code(self) -> str:
        # Generate model implementation

        code = '''
import pandas as pd
import numpy as np
from typing import List, Dict, Any

class SemanticMatcher:
    """Foundry ML model for semantic field matching"""
    
    def __init__(self):
        self.cache = {}
    
    def predict(self, source_field: str, target_field: str,
                source_samples: List[str], target_samples: List[str]) -> Dict[str, Any]:
        """Predict field similarity"""
        
        # Calculate name similarity
        name_similarity = self.calculate_name_similarity(source_field, target_field)
        
        # Calculate data pattern similarity
        pattern_similarity = self.calculate_pattern_similarity(
            source_samples, target_samples
        )
        
        # Determine semantic type
        semantic_type = self.infer_semantic_type(source_field, source_samples)
        
        # Combined confidence
        confidence = (name_similarity * 0.6 + pattern_similarity * 0.4)
        
        return {
            "similarity_score": name_similarity,
            "confidence": confidence,
            "semantic_type": semantic_type
        }
    
    def calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate name-based similarity"""
        
        if name1.lower() == name2.lower():
            return 1.0
        
        # Token-based similarity
        tokens1 = set(name1.lower().replace('_', ' ').split())
        tokens2 = set(name2.lower().replace('_', ' ').split())
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = tokens1 & tokens2
        union = tokens1 | tokens2
        
        return len(intersection) / len(union)
    
    def calculate_pattern_similarity(self, samples1: List[str], 
                                    samples2: List[str]) -> float:
        """Calculate data pattern similarity"""
        
        if not samples1 or not samples2:
            return 0.5
        
        # Check type compatibility
        types1 = set(type(self.infer_type(s)).__name__ for s in samples1[:5])
        types2 = set(type(self.infer_type(s)).__name__ for s in samples2[:5])
        
        if types1 & types2:
            return 0.8
        
        return 0.2
    
    def infer_type(self, value: str) -> Any:
        """Infer Python type from string"""
        
        try:
            return int(value)
        except:
            try:
                return float(value)
            except:
                return str(value)
    
    def infer_semantic_type(self, field_name: str, samples: List[str]) -> str:
        """Infer semantic type of field"""
        
        field_lower = field_name.lower()
        
        if 'id' in field_lower or 'key' in field_lower:
            return 'identifier'
        elif 'date' in field_lower or 'time' in field_lower:
            return 'timestamp'
        elif 'amount' in field_lower or 'price' in field_lower:
            return 'monetary'
        elif 'email' in field_lower:
            return 'email'
        
        # Check samples
        if samples and '@' in samples[0]:
            return 'email'
        
        return 'general'
'''
        return code

    def create_model_deployment(self, model_rid: str) -> Dict[str, Any]:
        # Create deployment configuration

        deployment = {
            "deploymentRid": f"ri.deployments.{datetime.now().timestamp()}",
            "modelRid": model_rid,
            "environment": {
                "cpu": 2,
                "memory": "4Gi",
                "replicas": 2
            },
            "endpoints": [
                {
                    "name": "predict",
                    "path": "/predict",
                    "method": "POST"
                }
            ],
            "monitoring": {
                "enableMetrics": True,
                "enableLogging": True,
                "alertThresholds": {
                    "latency_p99": 1000,  # ms
                    "error_rate": 0.01
                }
            }
        }

        return deployment

# Example usage functions


def example_foundry_dataset_integration():
    print("\n" + "="*60)
    print("EXAMPLE 1: Foundry Dataset Integration")
    print("="*60)

    # Create dataset adapter
    adapter = FoundryDatasetAdapter("ri.foundry.dataset.customers")

    # Read Foundry schema
    foundry_schema = adapter.read_schema()
    print("Foundry schema format:")
    print(json.dumps(foundry_schema["schema"]["fieldSchemaList"], indent=2))

    # Convert to unified format
    unified = adapter.to_unified_schema(foundry_schema)
    print("\nUnified schema:")
    for field in unified["fields"]:
        print(
            f"  {field['name']}: {field['type']} (Foundry: {field['foundry_type']})")


def example_pipeline_builder():
    print("\n" + "="*60)
    print("EXAMPLE 2: Pipeline Builder Integration")
    print("="*60)

    builder = PipelineBuilderIntegration()

    # Create pipeline
    pipeline = builder.create_schema_matching_pipeline(
        source_datasets=["ri.dataset.source1", "ri.dataset.source2"],
        target_dataset="ri.dataset.unified"
    )

    print("Pipeline structure:")
    print(f"  Nodes: {len(pipeline['nodes'])}")
    print(f"  Edges: {len(pipeline['edges'])}")

    print("\nGenerated transform code preview:")
    print(builder.generate_introspection_code()[:500] + "...")


def example_code_repository():
    print("\n" + "="*60)
    print("EXAMPLE 3: Code Repository Structure")
    print("="*60)

    repo = CodeRepositoryIntegration("ri.code.semantic_matcher")

    # Generate library files
    files = repo.create_semantic_matching_library()

    print("Generated repository structure:")
    for filepath in sorted(files.keys()):
        print(f"  {filepath}")

    print("\nMatcher implementation preview:")
    print(files["semantic_matcher/matcher.py"][:600] + "...")


def example_ontology_mapping():
    print("\n" + "="*60)
    print("EXAMPLE 4: Ontology Mapping")
    print("="*60)

    ontology = OntologyIntegration()

    # Create unified schema
    unified_schema = {
        "fields": [
            {"name": "customer_id", "type": "identifier", "nullable": False},
            {"name": "customer_name", "type": "string", "nullable": True},
            {"name": "total_amount", "type": "monetary", "nullable": True},
            {"name": "created_date", "type": "timestamp", "nullable": True}
        ]
    }

    # Create object type
    object_type = ontology.create_object_type_from_unified_schema(
        unified_schema)

    print("Ontology Object Type:")
    print(f"  API Name: {object_type['apiName']}")
    print(f"  Display Name: {object_type['displayName']}")
    print("\nProperties:")
    for prop in object_type["properties"]:
        print(
            f"  {prop['apiName']}: {prop['dataType']} ({prop['displayName']})")


def example_workshop_app():
    print("\n" + "="*60)
    print("EXAMPLE 5: Workshop Application")
    print("="*60)

    workshop = WorkshopIntegration()

    # Create validation app
    app = workshop.create_schema_validation_app()

    print("Workshop Application Structure:")
    print(f"  Name: {app['name']}")
    print(f"  Pages: {len(app['pages'])}")

    if app['pages']:
        widgets = app['pages'][0]['layout']['widgets']
        print(f"  Widgets: {len(widgets)}")
        for widget in widgets:
            print(f"    - {widget['widgetId']}: {widget['type']}")


def example_ml_model():
    print("\n" + "="*60)
    print("EXAMPLE 6: Foundry ML Model")
    print("="*60)

    ml = FoundryMLIntegration()

    # Create model definition
    model = ml.create_semantic_matching_model()

    print("Model Definition:")
    print(f"  Name: {model['name']}")
    print(f"  Type: {model['modelType']}")
    print("\nInputs:")
    for input_spec in model['inputs']:
        print(f"  - {input_spec['name']}: {input_spec['type']}")
    print("\nOutputs:")
    for output_spec in model['outputs']:
        print(f"  - {output_spec['name']}: {output_spec['type']}")

    # Create deployment
    deployment = ml.create_model_deployment(model['modelRid'])
    print(f"\nDeployment Configuration:")
    print(f"  CPU: {deployment['environment']['cpu']}")
    print(f"  Memory: {deployment['environment']['memory']}")
    print(f"  Replicas: {deployment['environment']['replicas']}")

# Main execution


def main():
    print("="*60)
    print("MODULE 8: PALANTIR FOUNDRY PERSPECTIVE")
    print("="*60)
    print("\nAdapting semantic integration to Foundry paradigms")

    # Run examples
    example_foundry_dataset_integration()
    example_pipeline_builder()
    example_code_repository()
    example_ontology_mapping()
    example_workshop_app()
    example_ml_model()

    print("\n" + "="*60)
    print("FOUNDRY INTEGRATION PATTERNS")
    print("="*60)

    print("\nKey Foundry concepts mapped:")
    print("- Datasets: Store raw and unified schemas")
    print("- Transforms: Python or SQL for schema matching")
    print("- Pipeline Builder: Visual pipeline construction")
    print("- Code Repositories: Reusable matching library")
    print("- Ontology: Semantic layer over unified data")
    print("- Workshop: UI for validation and review")
    print("- Foundry ML: Deploy matching models")

    print("\nFoundry advantages:")
    print("- Built-in versioning and lineage")
    print("- Ontology provides semantic layer")
    print("- Workshop for no-code validation UI")
    print("- Integrated security and governance")

    print("\nFoundry constraints:")
    print("- Transforms run in scheduled batches")
    print("- Limited real-time processing")
    print("- Proprietary APIs and formats")
    print("- Requires Foundry infrastructure")


if __name__ == "__main__":
    main()
