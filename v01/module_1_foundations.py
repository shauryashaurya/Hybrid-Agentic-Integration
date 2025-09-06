# module_1_foundations.py
# Module 1: Understanding AI Agents for Data Integration
# This module introduces core concepts of using AI for data understanding

import os
import json
import re
from typing import List, Dict, Any
from datetime import datetime
import pandas as pd
from dotenv import load_dotenv
import openai

# Load environment variables
load_dotenv()

# Configure OpenAI client
# We use gpt-3.5-turbo for cost efficiency in the workshop
openai.api_key = os.getenv('OPENAI_API_KEY')
MODEL = os.getenv('OPENAI_MODEL', 'gpt-3.5-turbo')

# Part 1: What is an AI Agent for Data?
# An agent is a system that can perceive its environment and take actions
# For data integration, our agents will:
# - Perceive: read and understand data structures
# - Reason: identify patterns and relationships
# - Act: transform and normalize data


def simple_llm_call(prompt: str, temperature: float = 0.1) -> str:
    # Basic function to interact with LLM
    # Low temperature for consistent, deterministic responses
    try:
        response = openai.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": "You are a data integration expert."},
                {"role": "user", "content": prompt}
            ],
            temperature=temperature
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"Error calling LLM: {e}")
        return ""

# Part 2: Field Name Normalization
# One of the simplest tasks - standardizing column names


def normalize_field_name_simple(field_name: str) -> str:
    # Traditional approach - rule-based normalization
    # Convert to lowercase, replace spaces and special chars with underscore
    normalized = field_name.lower()
    normalized = re.sub(r'[^a-z0-9]+', '_', normalized)
    normalized = re.sub(r'_+', '_', normalized)
    normalized = normalized.strip('_')
    return normalized


def normalize_field_name_with_ai(field_name: str) -> Dict[str, Any]:
    # AI-powered normalization that understands semantic meaning
    prompt = f"""
    Normalize this database field name: '{field_name}'
    
    Return a JSON object with:
    - normalized_name: snake_case version
    - semantic_type: what kind of data this represents (e.g., identifier, date, amount, status)
    - suggested_standard: industry-standard name if applicable
    
    Example input: 'Cust_ID'
    Example output: {{"normalized_name": "customer_id", "semantic_type": "identifier", "suggested_standard": "customer_id"}}
    
    Input: '{field_name}'
    Output:"""

    response = simple_llm_call(prompt)

    try:
        # Parse JSON response
        return json.loads(response)
    except:
        # Fallback to simple normalization if parsing fails
        return {
            "normalized_name": normalize_field_name_simple(field_name),
            "semantic_type": "unknown",
            "suggested_standard": normalize_field_name_simple(field_name)
        }

# Example 1: Basic field normalization


def example_field_normalization():
    print("\n" + "="*60)
    print("EXAMPLE 1: Field Name Normalization")
    print("="*60)

    # Sample field names from our generated data
    field_names = [
        'Customer_ID',
        'CustID',
        'client_number',
        'Registration_Date',
        'SignupDate',
        'created_at',
        'Account_Status',
        'active'
    ]

    print("\nTraditional rule-based approach:")
    for field in field_names:
        normalized = normalize_field_name_simple(field)
        print(f"  {field:20} -> {normalized}")

    print("\nAI-powered semantic understanding:")
    for field in field_names:
        result = normalize_field_name_with_ai(field)
        print(f"  {field:20}")
        print(f"    Normalized: {result.get('normalized_name', 'unknown')}")
        print(f"    Type: {result.get('semantic_type', 'unknown')}")
        print(f"    Standard: {result.get('suggested_standard', 'unknown')}")

# Part 3: Data Type Inference
# Understanding what type of data we're dealing with


def infer_data_type_from_samples(samples: List[str]) -> Dict[str, Any]:
    # Traditional approach - pattern matching
    # Check for common patterns

    if all(s == '' or s == 'NULL' for s in samples):
        return {"type": "null", "confidence": 1.0}

    # Try integer
    try:
        [int(s) for s in samples if s]
        return {"type": "integer", "confidence": 0.9}
    except:
        pass

    # Try float
    try:
        [float(s) for s in samples if s]
        return {"type": "float", "confidence": 0.9}
    except:
        pass

    # Check for dates
    date_patterns = [
        r'\d{4}-\d{2}-\d{2}',
        r'\d{2}/\d{2}/\d{4}',
        r'\d{2}-\d{2}-\d{4}'
    ]

    for pattern in date_patterns:
        if all(re.match(pattern, s) for s in samples if s):
            return {"type": "date", "confidence": 0.85}

    # Check for boolean
    bool_values = {'true', 'false', 'yes', 'no',
                   'y', 'n', '1', '0', 'active', 'inactive'}
    if all(s.lower() in bool_values for s in samples if s):
        return {"type": "boolean", "confidence": 0.8}

    # Default to string
    return {"type": "string", "confidence": 0.7}


def infer_data_type_with_ai(column_name: str, samples: List[str]) -> Dict[str, Any]:
    # AI-powered type inference with semantic understanding
    # Use first 10 samples
    samples_str = ', '.join([f"'{s}'" for s in samples[:10]])

    prompt = f"""
    Analyze this data column and infer its type:
    
    Column name: {column_name}
    Sample values: {samples_str}
    
    Return a JSON object with:
    - data_type: the primary data type (string, integer, float, date, boolean, identifier, enum)
    - format: specific format if applicable (e.g., date format, currency)
    - nullable: whether nulls are present
    - semantic_meaning: what this data represents in business terms
    - validation_rules: suggested validation rules
    
    Output:"""

    response = simple_llm_call(prompt)

    try:
        return json.loads(response)
    except:
        # Fallback to traditional inference
        traditional_result = infer_data_type_from_samples(samples)
        return {
            "data_type": traditional_result["type"],
            "confidence": traditional_result["confidence"],
            "format": "unknown",
            "nullable": any(s == '' for s in samples),
            "semantic_meaning": "unknown"
        }

# Example 2: Type inference from data samples


def example_type_inference():
    print("\n" + "="*60)
    print("EXAMPLE 2: Data Type Inference")
    print("="*60)

    # Load sample data
    df = pd.read_csv('workshop_data/customers_v1.csv')

    print("\nAnalyzing customers_v1.csv columns:")

    for column in df.columns:
        # Get non-null samples
        samples = df[column].dropna().astype(str).tolist()[:10]

        print(f"\nColumn: {column}")
        print(f"  Samples: {samples[:3]}")

        # Traditional inference
        traditional = infer_data_type_from_samples(samples)
        print(
            f"  Traditional inference: {traditional['type']} (confidence: {traditional['confidence']})")

        # AI-powered inference
        ai_result = infer_data_type_with_ai(column, samples)
        print(f"  AI inference:")
        print(f"    Type: {ai_result.get('data_type', 'unknown')}")
        print(f"    Format: {ai_result.get('format', 'unknown')}")
        print(f"    Meaning: {ai_result.get('semantic_meaning', 'unknown')}")

# Part 4: Building Blocks - Prompts, Parsers, Validators


class DataUnderstandingAgent:
    # Basic agent that combines perception, reasoning, and action

    def __init__(self, model: str = MODEL):
        self.model = model
        self.context = []  # Store understanding context

    def perceive(self, data_sample: pd.DataFrame) -> Dict[str, Any]:
        # Perceive the structure and content of data
        perception = {
            "shape": data_sample.shape,
            "columns": list(data_sample.columns),
            "dtypes": data_sample.dtypes.to_dict(),
            "sample_values": {}
        }

        for col in data_sample.columns:
            samples = data_sample[col].dropna().head(5).tolist()
            perception["sample_values"][col] = samples

        return perception

    def reason(self, perception: Dict[str, Any]) -> Dict[str, Any]:
        # Reason about the data to understand its meaning

        # Build a comprehensive prompt
        prompt = f"""
        Analyze this dataset structure and provide insights:
        
        Columns: {perception['columns']}
        Shape: {perception['shape']}
        Sample values: {json.dumps(perception['sample_values'], indent=2)}
        
        Provide a JSON response with:
        - dataset_type: what kind of data this represents
        - primary_key: likely primary key column
        - relationships: potential foreign keys or relationships
        - data_quality_issues: any obvious problems
        - recommended_transformations: suggested improvements
        
        Output:"""

        response = simple_llm_call(prompt)

        try:
            reasoning = json.loads(response)
        except:
            reasoning = {
                "dataset_type": "unknown",
                "primary_key": perception['columns'][0] if perception['columns'] else None,
                "relationships": [],
                "data_quality_issues": [],
                "recommended_transformations": []
            }

        self.context.append(reasoning)
        return reasoning

    def act(self, data: pd.DataFrame, reasoning: Dict[str, Any]) -> pd.DataFrame:
        # Take action based on reasoning - normalize column names

        transformed = data.copy()

        # Normalize column names based on our understanding
        new_columns = {}
        for col in transformed.columns:
            normalized = normalize_field_name_with_ai(col)
            new_columns[col] = normalized.get(
                'suggested_standard', col.lower())

        transformed.rename(columns=new_columns, inplace=True)

        return transformed

# Example 3: Complete agent workflow


def example_agent_workflow():
    print("\n" + "="*60)
    print("EXAMPLE 3: Complete Agent Workflow")
    print("="*60)

    # Create an agent
    agent = DataUnderstandingAgent()

    # Load sample data
    df = pd.read_csv('workshop_data/customers_v2.csv')

    print("\nStep 1: PERCEIVE - Understanding the data structure")
    perception = agent.perceive(df.head(10))
    print(
        f"  Detected {perception['shape'][1]} columns, {perception['shape'][0]} sample rows")
    print(f"  Columns: {perception['columns']}")

    print("\nStep 2: REASON - Analyzing meaning and relationships")
    reasoning = agent.reason(perception)
    print(f"  Dataset type: {reasoning.get('dataset_type', 'unknown')}")
    print(f"  Primary key: {reasoning.get('primary_key', 'unknown')}")
    print(f"  Issues found: {reasoning.get('data_quality_issues', [])}")

    print("\nStep 3: ACT - Transform based on understanding")
    transformed = agent.act(df.head(10), reasoning)
    print(f"  Original columns: {list(df.columns)}")
    print(f"  Transformed columns: {list(transformed.columns)}")

# Part 5: Building Confidence Scores


def calculate_mapping_confidence(source_field: str, target_field: str, samples: List[str]) -> float:
    # Calculate confidence score for field mapping

    # Simple heuristic-based confidence
    confidence = 0.5  # Base confidence

    # Check name similarity
    source_lower = source_field.lower()
    target_lower = target_field.lower()

    if source_lower == target_lower:
        confidence += 0.3
    elif source_lower in target_lower or target_lower in source_lower:
        confidence += 0.2

    # Check for common abbreviations
    abbreviations = {
        'cust': 'customer',
        'id': 'identifier',
        'qty': 'quantity',
        'amt': 'amount',
        'dt': 'date'
    }

    for abbr, full in abbreviations.items():
        if abbr in source_lower and full in target_lower:
            confidence += 0.1

    # Check data patterns
    if samples:
        pattern_confidence = infer_data_type_from_samples(samples)
        confidence += pattern_confidence['confidence'] * 0.2

    return min(confidence, 1.0)

# Example 4: Confidence scoring


def example_confidence_scoring():
    print("\n" + "="*60)
    print("EXAMPLE 4: Mapping Confidence Scores")
    print("="*60)

    # Test field mappings
    mappings = [
        ('Customer_ID', 'customer_id', ['CUST1001', 'CUST1002']),
        ('CustID', 'customer_id', ['2001', '2002']),
        ('client_number', 'customer_id', ['CLT-3001', 'CLT-3002']),
        ('Full_Name', 'customer_name', ['John Doe', 'Jane Smith']),
        ('Email_Address', 'email', ['john@example.com', 'jane@example.com'])
    ]

    print("\nField mapping confidence scores:")
    for source, target, samples in mappings:
        confidence = calculate_mapping_confidence(source, target, samples)
        print(f"  {source:20} -> {target:20} Confidence: {confidence:.2f}")

# Main execution


def main():
    print("="*60)
    print("MODULE 1: FOUNDATIONS - AI AGENTS FOR DATA")
    print("="*60)
    print("\nThis module demonstrates core concepts of using AI for data integration")

    # Check if API key is configured
    if not os.getenv('OPENAI_API_KEY'):
        print("\nWARNING: OpenAI API key not found in .env file")
        print("Some examples will use fallback methods")

    # Run examples
    example_field_normalization()
    example_type_inference()
    example_agent_workflow()
    example_confidence_scoring()

    print("\n" + "="*60)
    print("MODULE 1 COMPLETE")
    print("="*60)
    print("\nKey concepts covered:")
    print("- AI agents perceive, reason, and act on data")
    print("- Semantic understanding vs rule-based processing")
    print("- Type inference and pattern recognition")
    print("- Confidence scoring for decisions")
    print("\nNext: Module 2 - Source Introspection")


if __name__ == "__main__":
    main()
