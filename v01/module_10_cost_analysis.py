# module_10_cost_analysis.py
# Module 10: Comprehensive Cost Analysis for Hybrid Agentic Integration
# Detailed framework for understanding and optimizing costs across all components

import os
import json
import math
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import pandas as pd
from enum import Enum

# Part 1: Cost Model Foundations


class CostComponent(Enum):
    # All cost components in the system
    LLM_API = "llm_api"
    COMPUTE = "compute"
    STORAGE = "storage"
    NETWORK = "network"
    HUMAN_TIME = "human_time"
    INFRASTRUCTURE = "infrastructure"
    LICENSING = "licensing"
    OPPORTUNITY = "opportunity"


@dataclass
class LLMCostModel:
    # Detailed cost model for LLM operations
    provider: str
    input_token_cost: float  # Cost per 1K tokens
    output_token_cost: float  # Cost per 1K tokens
    context_window: int
    requests_per_minute: int
    requests_per_day: int
    minimum_latency_ms: int

    def calculate_prompt_cost(self, prompt: str, response: str) -> float:
        # Calculate cost for a single prompt-response pair
        # Rough token estimation: 1 token ~= 4 characters
        input_tokens = len(prompt) / 4
        output_tokens = len(response) / 4

        input_cost = (input_tokens / 1000) * self.input_token_cost
        output_cost = (output_tokens / 1000) * self.output_token_cost

        return input_cost + output_cost

    def calculate_field_mapping_cost(self, num_fields: int, samples_per_field: int = 5) -> float:
        # Calculate cost for mapping a schema

        # Typical prompt for field analysis
        avg_prompt_size = 200 + (samples_per_field * 20)  # Base + samples
        avg_response_size = 150  # JSON response

        cost_per_field = self.calculate_prompt_cost(
            "x" * avg_prompt_size,
            "x" * avg_response_size
        )

        return cost_per_field * num_fields

    def calculate_monthly_cost(self, schemas_per_day: int, fields_per_schema: int) -> float:
        # Calculate monthly API costs

        daily_fields = schemas_per_day * fields_per_schema
        daily_cost = self.calculate_field_mapping_cost(daily_fields)
        monthly_cost = daily_cost * 30

        # Add rate limit overhead (retries, delays)
        overhead_factor = 1.1

        return monthly_cost * overhead_factor


class ComputeCostModel:
    # Model compute costs for different platforms

    def __init__(self):
        # Cost per hour for different instance types
        self.instance_costs = {
            # Local laptop (electricity cost)
            "laptop_4gb": 0.05,  # ~50W * $0.10/kWh

            # Cloud instances
            "aws_t3_medium": 0.0416,
            "aws_m5_xlarge": 0.192,
            "aws_m5_4xlarge": 0.768,
            "aws_g4dn_xlarge": 0.526,  # GPU instance

            # Spark cluster
            "databricks_standard": 0.40,  # DBU cost
            "databricks_premium": 0.55,
            "emr_m5_xlarge": 0.25,

            # Serverless
            "lambda_gb_second": 0.0000166667,
            "fargate_vcpu_hour": 0.04048,
        }

    def calculate_processing_cost(self, data_size_gb: float, instance_type: str,
                                  processing_rate_gb_per_hour: float) -> float:
        # Calculate cost to process data

        hours_needed = data_size_gb / processing_rate_gb_per_hour
        hourly_cost = self.instance_costs.get(instance_type, 0)

        return hours_needed * hourly_cost

    def calculate_spark_cluster_cost(self, data_size_gb: float, num_workers: int = 4) -> Dict[str, float]:
        # Calculate Spark cluster costs

        # Processing rates (GB/hour)
        processing_rates = {
            "schema_introspection": 50,  # Per worker
            "pattern_matching": 20,  # More compute intensive
            "data_transformation": 100
        }

        costs = {}

        for operation, rate in processing_rates.items():
            effective_rate = rate * num_workers
            hours = data_size_gb / effective_rate

            # Driver + workers cost
            cluster_cost = self.instance_costs["aws_m5_xlarge"] * \
                (1 + num_workers)
            costs[operation] = hours * cluster_cost

        costs["total"] = sum(v for k, v in costs.items() if k != "total")

        return costs


class StorageCostModel:
    # Model storage costs across platforms

    def __init__(self):
        # Cost per GB per month
        self.storage_costs = {
            "s3_standard": 0.023,
            "s3_infrequent": 0.0125,
            "s3_glacier": 0.004,
            "ebs_gp3": 0.08,
            "delta_lake": 0.023,  # On S3
            "duckdb_local": 0.0,  # Local storage
            "postgres_rds": 0.115
        }

        # Cost per 1M requests
        self.request_costs = {
            "s3_get": 0.4,
            "s3_put": 5.0,
            "dynamodb_read": 0.25,
            "dynamodb_write": 1.25
        }

    def calculate_storage_cost(self, size_gb: float, storage_type: str, months: int = 1) -> float:
        # Calculate storage cost

        monthly_cost = size_gb * self.storage_costs.get(storage_type, 0)
        return monthly_cost * months

    def calculate_schema_registry_cost(self, num_schemas: int, versions_per_schema: int = 5) -> float:
        # Calculate cost for schema registry storage

        # Estimate: 10KB per schema version
        size_gb = (num_schemas * versions_per_schema * 10) / (1024 * 1024)

        # Use S3 for durability
        storage_cost = self.calculate_storage_cost(size_gb, "s3_standard", 12)

        # Add DynamoDB for metadata
        dynamodb_cost = 5.0  # Base cost for small table

        return storage_cost + dynamodb_cost * 12


class HumanCostModel:
    # Model human time costs

    def __init__(self, hourly_rate: float = 75.0):
        self.hourly_rate = hourly_rate

        # Time estimates for different tasks (minutes)
        self.task_times = {
            "review_mapping": 0.5,  # Per field mapping
            "validate_complex_mapping": 2.0,
            "correct_mapping": 1.5,
            "investigate_failure": 30.0,
            "update_rules": 15.0,
            "train_user": 120.0
        }

    def calculate_validation_cost(self, num_mappings: int, accuracy: float) -> float:
        # Calculate human validation cost

        # Only need to review uncertain mappings
        uncertain_mappings = num_mappings * (1 - accuracy)

        # Time for review
        review_time_hours = (uncertain_mappings *
                             self.task_times["review_mapping"]) / 60

        # Some need correction
        correction_ratio = 0.3
        corrections = uncertain_mappings * correction_ratio
        correction_time_hours = (
            corrections * self.task_times["correct_mapping"]) / 60

        total_hours = review_time_hours + correction_time_hours

        return total_hours * self.hourly_rate

    def calculate_maintenance_cost(self, schemas_per_month: int) -> float:
        # Calculate ongoing maintenance cost

        # Rule updates
        rule_updates_per_month = schemas_per_month * 0.1  # 10% need rule updates
        rule_time_hours = (rule_updates_per_month *
                           self.task_times["update_rules"]) / 60

        # Failure investigation
        failures_per_month = schemas_per_month * 0.05  # 5% failure rate
        failure_time_hours = (failures_per_month *
                              self.task_times["investigate_failure"]) / 60

        total_hours = rule_time_hours + failure_time_hours

        return total_hours * self.hourly_rate

# Part 2: Comparative Cost Analysis


class ComparativeCostAnalyzer:
    # Compare costs across different approaches

    def __init__(self):
        self.llm_models = {
            "gpt-3.5-turbo": LLMCostModel(
                provider="openai",
                input_token_cost=0.0005,
                output_token_cost=0.0015,
                context_window=16385,
                requests_per_minute=3500,
                requests_per_day=10000,
                minimum_latency_ms=500
            ),
            "gpt-4": LLMCostModel(
                provider="openai",
                input_token_cost=0.03,
                output_token_cost=0.06,
                context_window=8192,
                requests_per_minute=500,
                requests_per_day=10000,
                minimum_latency_ms=1000
            ),
            "claude-3-haiku": LLMCostModel(
                provider="anthropic",
                input_token_cost=0.00025,
                output_token_cost=0.00125,
                context_window=200000,
                requests_per_minute=1000,
                requests_per_day=10000,
                minimum_latency_ms=400
            ),
            "llama2-70b": LLMCostModel(
                provider="replicate",
                input_token_cost=0.00065,
                output_token_cost=0.00275,
                context_window=4096,
                requests_per_minute=100,
                requests_per_day=10000,
                minimum_latency_ms=2000
            ),
            "ollama-local": LLMCostModel(
                provider="local",
                input_token_cost=0.0,  # Only compute cost
                output_token_cost=0.0,
                context_window=4096,
                requests_per_minute=10,  # Limited by local GPU
                requests_per_day=999999,
                minimum_latency_ms=5000
            )
        }

        self.compute_model = ComputeCostModel()
        self.storage_model = StorageCostModel()
        self.human_model = HumanCostModel()

    def compare_llm_costs(self, schemas_per_month: int, fields_per_schema: int) -> pd.DataFrame:
        # Compare LLM costs across providers

        results = []

        for model_name, model in self.llm_models.items():
            # API costs
            api_cost = model.calculate_monthly_cost(
                schemas_per_month / 30,
                fields_per_schema
            )

            # Add compute cost for local models
            compute_cost = 0
            if model.provider == "local":
                # Ollama needs GPU instance
                hours_per_month = (schemas_per_month *
                                   fields_per_schema * 0.1) / 60
                compute_cost = hours_per_month * \
                    self.compute_model.instance_costs["aws_g4dn_xlarge"]

            # Throughput limitations
            daily_capacity = min(
                model.requests_per_day,
                model.requests_per_minute * 60 * 8  # 8 hour processing window
            )

            days_needed = (schemas_per_month *
                           fields_per_schema) / daily_capacity

            results.append({
                "model": model_name,
                "provider": model.provider,
                "api_cost_monthly": api_cost,
                "compute_cost_monthly": compute_cost,
                "total_cost_monthly": api_cost + compute_cost,
                "daily_capacity": daily_capacity,
                "days_to_process": days_needed,
                "feasible": days_needed <= 30
            })

        return pd.DataFrame(results)

    def compare_platform_costs(self, data_size_gb: float) -> pd.DataFrame:
        # Compare costs across platforms

        results = []

        # Local laptop
        local_cost = self.compute_model.calculate_processing_cost(
            data_size_gb, "laptop_4gb", 5.0  # 5GB/hour on laptop
        )

        results.append({
            "platform": "Local Laptop",
            "compute_cost": local_cost,
            "storage_cost": 0,
            "network_cost": 0,
            "total_cost": local_cost,
            "processing_time_hours": data_size_gb / 5.0
        })

        # Single EC2 instance
        ec2_cost = self.compute_model.calculate_processing_cost(
            data_size_gb, "aws_m5_xlarge", 20.0
        )

        storage_cost = self.storage_model.calculate_storage_cost(
            data_size_gb, "ebs_gp3", 1
        )

        results.append({
            "platform": "EC2 Single Instance",
            "compute_cost": ec2_cost,
            "storage_cost": storage_cost,
            "network_cost": data_size_gb * 0.09,  # Egress cost
            "total_cost": ec2_cost + storage_cost + data_size_gb * 0.09,
            "processing_time_hours": data_size_gb / 20.0
        })

        # Spark cluster
        spark_costs = self.compute_model.calculate_spark_cluster_cost(
            data_size_gb)
        storage_cost = self.storage_model.calculate_storage_cost(
            data_size_gb, "s3_standard", 1
        )

        results.append({
            "platform": "Spark Cluster (4 workers)",
            "compute_cost": spark_costs["total"],
            "storage_cost": storage_cost,
            "network_cost": 0,  # Within AWS
            "total_cost": spark_costs["total"] + storage_cost,
            "processing_time_hours": data_size_gb / 200.0  # Parallel processing
        })

        # Databricks
        databricks_hours = data_size_gb / 200.0
        databricks_dbu = databricks_hours * 4  # 4 DBU for cluster
        databricks_cost = databricks_dbu * \
            self.compute_model.instance_costs["databricks_standard"]

        results.append({
            "platform": "Databricks",
            "compute_cost": databricks_cost,
            "storage_cost": storage_cost,
            "network_cost": 0,
            "total_cost": databricks_cost + storage_cost,
            "processing_time_hours": databricks_hours
        })

        return pd.DataFrame(results)

# Part 3: TCO (Total Cost of Ownership) Calculator


class TCOCalculator:
    # Calculate total cost of ownership

    def __init__(self):
        self.cost_analyzer = ComparativeCostAnalyzer()

    def calculate_tco(self, scenario: Dict[str, Any]) -> Dict[str, Any]:
        # Calculate comprehensive TCO

        # Extract scenario parameters
        data_sources = scenario["data_sources"]
        monthly_new_sources = scenario["monthly_new_sources"]
        avg_fields_per_source = scenario["avg_fields_per_source"]
        data_volume_gb = scenario["data_volume_gb"]
        accuracy_target = scenario["accuracy_target"]
        time_horizon_months = scenario["time_horizon_months"]

        # Initial setup costs
        setup_costs = {
            "infrastructure_setup": 5000,  # One-time
            "initial_training": 40 * self.cost_analyzer.human_model.hourly_rate,
            "poc_development": 80 * self.cost_analyzer.human_model.hourly_rate
        }

        # Monthly operational costs
        monthly_costs = {}

        # LLM costs
        llm_model = scenario.get("llm_model", "gpt-3.5-turbo")
        llm_cost_model = self.cost_analyzer.llm_models[llm_model]
        monthly_costs["llm_api"] = llm_cost_model.calculate_monthly_cost(
            monthly_new_sources,
            avg_fields_per_source
        )

        # Compute costs
        monthly_compute = self.cost_analyzer.compute_model.calculate_processing_cost(
            data_volume_gb / 30,
            scenario.get("compute_instance", "aws_m5_xlarge"),
            20.0
        ) * 30
        monthly_costs["compute"] = monthly_compute

        # Storage costs (accumulating)
        total_sources = data_sources + \
            (monthly_new_sources * time_horizon_months)
        total_storage_gb = total_sources * (data_volume_gb / data_sources)
        monthly_costs["storage"] = self.cost_analyzer.storage_model.calculate_storage_cost(
            total_storage_gb,
            scenario.get("storage_type", "s3_standard")
        )

        # Human costs
        monthly_mappings = monthly_new_sources * avg_fields_per_source
        monthly_costs["human_validation"] = self.cost_analyzer.human_model.calculate_validation_cost(
            monthly_mappings,
            accuracy_target
        )
        monthly_costs["human_maintenance"] = self.cost_analyzer.human_model.calculate_maintenance_cost(
            monthly_new_sources
        )

        # Calculate TCO
        total_setup = sum(setup_costs.values())
        total_monthly = sum(monthly_costs.values())
        total_tco = total_setup + (total_monthly * time_horizon_months)

        # Cost breakdown
        breakdown = {
            "setup_costs": setup_costs,
            "monthly_costs": monthly_costs,
            "total_setup": total_setup,
            "total_monthly": total_monthly,
            "total_tco": total_tco,
            "cost_per_source": total_tco / total_sources,
            "cost_per_field": total_tco / (total_sources * avg_fields_per_source)
        }

        return breakdown

    def compare_with_traditional(self, scenario: Dict[str, Any]) -> Dict[str, Any]:
        # Compare with traditional ETL approach

        # Traditional approach costs
        traditional_costs = {
            "setup": {
                "etl_development": 200 * self.cost_analyzer.human_model.hourly_rate,
                "infrastructure": 10000
            },
            "monthly": {
                "etl_maintenance": 40 * self.cost_analyzer.human_model.hourly_rate,
                "manual_mapping": scenario["monthly_new_sources"] * 5 * self.cost_analyzer.human_model.hourly_rate,
                "compute": scenario["data_volume_gb"] * 0.5,  # ETL compute
                "storage": scenario["data_volume_gb"] * 0.023
            }
        }

        # AI approach costs
        ai_tco = self.calculate_tco(scenario)

        # Traditional TCO
        traditional_setup = sum(traditional_costs["setup"].values())
        traditional_monthly = sum(traditional_costs["monthly"].values())
        traditional_tco = traditional_setup + \
            (traditional_monthly * scenario["time_horizon_months"])

        # Comparison
        comparison = {
            "ai_tco": ai_tco["total_tco"],
            "traditional_tco": traditional_tco,
            "savings": traditional_tco - ai_tco["total_tco"],
            "savings_percentage": ((traditional_tco - ai_tco["total_tco"]) / traditional_tco) * 100,
            "break_even_months": traditional_setup / (traditional_monthly - ai_tco["total_monthly"])
        }

        return comparison

# Part 4: Cost Optimization Strategies


class CostOptimizer:
    # Optimize costs across different dimensions

    def __init__(self):
        self.strategies = []

    def analyze_cost_drivers(self, tco_breakdown: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Identify main cost drivers

        monthly_costs = tco_breakdown["monthly_costs"]
        total_monthly = tco_breakdown["total_monthly"]

        drivers = []

        for component, cost in monthly_costs.items():
            percentage = (cost / total_monthly) * 100
            drivers.append({
                "component": component,
                "monthly_cost": cost,
                "percentage": percentage,
                "category": "high" if percentage > 30 else "medium" if percentage > 15 else "low"
            })

        return sorted(drivers, key=lambda x: x["monthly_cost"], reverse=True)

    def generate_optimization_strategies(self, scenario: Dict[str, Any],
                                         tco_breakdown: Dict[str, Any]) -> List[Dict[str, Any]]:
        # Generate specific optimization strategies

        strategies = []
        drivers = self.analyze_cost_drivers(tco_breakdown)

        for driver in drivers:
            if driver["component"] == "llm_api" and driver["category"] == "high":
                strategies.append({
                    "strategy": "Switch to cheaper LLM model",
                    "action": "Use claude-3-haiku instead of GPT-4",
                    "potential_savings": driver["monthly_cost"] * 0.7,
                    "effort": "low",
                    "risk": "medium"
                })

                strategies.append({
                    "strategy": "Implement LLM response caching",
                    "action": "Cache similar field mappings",
                    "potential_savings": driver["monthly_cost"] * 0.3,
                    "effort": "medium",
                    "risk": "low"
                })

                strategies.append({
                    "strategy": "Use local Ollama for dev/test",
                    "action": "Reserve API calls for production only",
                    "potential_savings": driver["monthly_cost"] * 0.4,
                    "effort": "low",
                    "risk": "low"
                })

            elif driver["component"] == "compute" and driver["category"] == "high":
                strategies.append({
                    "strategy": "Use spot instances",
                    "action": "Switch to AWS spot for batch processing",
                    "potential_savings": driver["monthly_cost"] * 0.7,
                    "effort": "medium",
                    "risk": "medium"
                })

                strategies.append({
                    "strategy": "Optimize processing schedule",
                    "action": "Process during off-peak hours",
                    "potential_savings": driver["monthly_cost"] * 0.2,
                    "effort": "low",
                    "risk": "low"
                })

            elif driver["component"] == "human_validation" and driver["category"] == "high":
                strategies.append({
                    "strategy": "Improve model accuracy",
                    "action": "Fine-tune models to reduce validation needs",
                    "potential_savings": driver["monthly_cost"] * 0.4,
                    "effort": "high",
                    "risk": "low"
                })

                strategies.append({
                    "strategy": "Implement confidence thresholds",
                    "action": "Auto-approve high confidence mappings",
                    "potential_savings": driver["monthly_cost"] * 0.3,
                    "effort": "medium",
                    "risk": "medium"
                })

            elif driver["component"] == "storage" and driver["category"] in ["medium", "high"]:
                strategies.append({
                    "strategy": "Implement data lifecycle policies",
                    "action": "Move old data to glacier storage",
                    "potential_savings": driver["monthly_cost"] * 0.6,
                    "effort": "low",
                    "risk": "low"
                })

                strategies.append({
                    "strategy": "Compress historical data",
                    "action": "Use Parquet with snappy compression",
                    "potential_savings": driver["monthly_cost"] * 0.3,
                    "effort": "medium",
                    "risk": "low"
                })

        return sorted(strategies, key=lambda x: x["potential_savings"], reverse=True)

    def calculate_optimized_tco(self, scenario: Dict[str, Any],
                                strategies: List[str]) -> Dict[str, Any]:
        # Calculate TCO with optimization strategies applied

        optimized_scenario = scenario.copy()

        for strategy in strategies:
            if strategy == "use_cheaper_llm":
                optimized_scenario["llm_model"] = "claude-3-haiku"
            elif strategy == "use_spot_instances":
                optimized_scenario["compute_instance"] = "spot_m5_xlarge"
            elif strategy == "auto_approve_high_confidence":
                optimized_scenario["accuracy_target"] = 0.95
            elif strategy == "use_glacier_storage":
                optimized_scenario["storage_type"] = "s3_glacier"

        calculator = TCOCalculator()
        return calculator.calculate_tco(optimized_scenario)

# Part 5: Real-world Scenarios


class CostScenarios:
    # Define and analyze real-world scenarios

    @staticmethod
    def small_startup_scenario() -> Dict[str, Any]:
        # Small startup with limited budget
        return {
            "name": "Small Startup",
            "data_sources": 5,
            "monthly_new_sources": 2,
            "avg_fields_per_source": 20,
            "data_volume_gb": 10,
            "accuracy_target": 0.85,
            "time_horizon_months": 12,
            "llm_model": "claude-3-haiku",
            "compute_instance": "laptop_4gb",
            "storage_type": "duckdb_local",
            "constraints": {
                "max_monthly_budget": 500,
                "max_setup_cost": 2000,
                "technical_expertise": "medium"
            }
        }

    @staticmethod
    def mid_size_company_scenario() -> Dict[str, Any]:
        # Mid-size company with moderate scale
        return {
            "name": "Mid-size Company",
            "data_sources": 50,
            "monthly_new_sources": 10,
            "avg_fields_per_source": 50,
            "data_volume_gb": 500,
            "accuracy_target": 0.90,
            "time_horizon_months": 24,
            "llm_model": "gpt-3.5-turbo",
            "compute_instance": "aws_m5_xlarge",
            "storage_type": "s3_standard",
            "constraints": {
                "max_monthly_budget": 5000,
                "max_setup_cost": 20000,
                "technical_expertise": "high"
            }
        }

    @staticmethod
    def enterprise_scenario() -> Dict[str, Any]:
        # Large enterprise with high volume
        return {
            "name": "Enterprise",
            "data_sources": 500,
            "monthly_new_sources": 50,
            "avg_fields_per_source": 100,
            "data_volume_gb": 10000,
            "accuracy_target": 0.95,
            "time_horizon_months": 36,
            "llm_model": "gpt-4",
            "compute_instance": "databricks_premium",
            "storage_type": "delta_lake",
            "constraints": {
                "max_monthly_budget": 50000,
                "max_setup_cost": 100000,
                "technical_expertise": "high",
                "compliance_required": True
            }
        }

    @staticmethod
    def high_frequency_trading_scenario() -> Dict[str, Any]:
        # Financial services with real-time requirements
        return {
            "name": "High Frequency Trading",
            "data_sources": 100,
            "monthly_new_sources": 20,
            "avg_fields_per_source": 200,
            "data_volume_gb": 5000,
            "accuracy_target": 0.99,
            "time_horizon_months": 12,
            "llm_model": "gpt-4",
            "compute_instance": "aws_g4dn_xlarge",
            "storage_type": "s3_standard",
            "constraints": {
                "max_latency_ms": 100,
                "accuracy_critical": True,
                "regulatory_compliance": True
            }
        }

# Part 6: Cost Monitoring and Alerting


class CostMonitor:
    # Monitor and alert on cost anomalies

    def __init__(self):
        self.cost_history = []
        self.alerts = []
        self.thresholds = {}

    def set_thresholds(self, component: str, warning: float, critical: float):
        # Set alerting thresholds
        self.thresholds[component] = {
            "warning": warning,
            "critical": critical
        }

    def track_daily_costs(self, date: datetime, costs: Dict[str, float]):
        # Track daily cost data

        record = {
            "date": date,
            "costs": costs,
            "total": sum(costs.values())
        }

        self.cost_history.append(record)

        # Check thresholds
        for component, cost in costs.items():
            if component in self.thresholds:
                if cost > self.thresholds[component]["critical"]:
                    self.alerts.append({
                        "date": date,
                        "component": component,
                        "level": "critical",
                        "message": f"{component} cost ${cost:.2f} exceeds critical threshold"
                    })
                elif cost > self.thresholds[component]["warning"]:
                    self.alerts.append({
                        "date": date,
                        "component": component,
                        "level": "warning",
                        "message": f"{component} cost ${cost:.2f} exceeds warning threshold"
                    })

    def detect_anomalies(self, lookback_days: int = 7) -> List[Dict[str, Any]]:
        # Detect cost anomalies

        if len(self.cost_history) < lookback_days:
            return []

        anomalies = []
        recent = self.cost_history[-lookback_days:]

        # Calculate statistics
        for component in recent[0]["costs"].keys():
            values = [r["costs"][component] for r in recent]
            mean = sum(values) / len(values)
            std = (sum((x - mean) ** 2 for x in values) / len(values)) ** 0.5

            # Check latest value
            latest = recent[-1]["costs"][component]
            z_score = (latest - mean) / std if std > 0 else 0

            if abs(z_score) > 2:
                anomalies.append({
                    "component": component,
                    "latest_cost": latest,
                    "average_cost": mean,
                    "z_score": z_score,
                    "severity": "high" if abs(z_score) > 3 else "medium"
                })

        return anomalies

    def generate_cost_report(self) -> Dict[str, Any]:
        # Generate comprehensive cost report

        if not self.cost_history:
            return {}

        # Calculate totals
        total_costs = {}
        for record in self.cost_history:
            for component, cost in record["costs"].items():
                if component not in total_costs:
                    total_costs[component] = 0
                total_costs[component] += cost

        # Calculate trends
        if len(self.cost_history) > 1:
            first_week = self.cost_history[:7] if len(
                self.cost_history) > 7 else self.cost_history[:len(self.cost_history)//2]
            last_week = self.cost_history[-7:]

            first_avg = sum(r["total"] for r in first_week) / len(first_week)
            last_avg = sum(r["total"] for r in last_week) / len(last_week)
            trend = ((last_avg - first_avg) / first_avg) * \
                100 if first_avg > 0 else 0
        else:
            trend = 0

        report = {
            "period_days": len(self.cost_history),
            "total_cost": sum(total_costs.values()),
            "cost_by_component": total_costs,
            "daily_average": sum(total_costs.values()) / len(self.cost_history),
            "trend_percentage": trend,
            "alerts_count": len(self.alerts),
            "critical_alerts": len([a for a in self.alerts if a["level"] == "critical"])
        }

        return report

# Example usage functions


def example_basic_cost_calculation():
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic Cost Calculation")
    print("="*60)

    # Calculate costs for different LLM models
    llm_cost = LLMCostModel(
        provider="openai",
        input_token_cost=0.0005,
        output_token_cost=0.0015,
        context_window=16385,
        requests_per_minute=3500,
        requests_per_day=10000,
        minimum_latency_ms=500
    )

    # Single schema mapping
    fields = 50
    cost = llm_cost.calculate_field_mapping_cost(fields)
    print(f"Cost to map schema with {fields} fields: ${cost:.4f}")

    # Monthly costs
    schemas_per_day = 10
    monthly = llm_cost.calculate_monthly_cost(schemas_per_day, fields)
    print(f"Monthly cost for {schemas_per_day} schemas/day: ${monthly:.2f}")


def example_platform_comparison():
    print("\n" + "="*60)
    print("EXAMPLE 2: Platform Cost Comparison")
    print("="*60)

    analyzer = ComparativeCostAnalyzer()

    # Compare platforms for 1TB of data
    data_size = 1000  # GB
    comparison = analyzer.compare_platform_costs(data_size)

    print(f"Processing {data_size}GB of data:")
    print(comparison.to_string(index=False))


def example_llm_comparison():
    print("\n" + "="*60)
    print("EXAMPLE 3: LLM Provider Comparison")
    print("="*60)

    analyzer = ComparativeCostAnalyzer()

    # Compare LLM costs
    schemas = 100  # per month
    fields = 50

    comparison = analyzer.compare_llm_costs(schemas, fields)
    print(f"Processing {schemas} schemas/month with {fields} fields each:")
    print(comparison.to_string(index=False))


def example_tco_analysis():
    print("\n" + "="*60)
    print("EXAMPLE 4: Total Cost of Ownership")
    print("="*60)

    calculator = TCOCalculator()

    # Analyze different scenarios
    scenarios = [
        CostScenarios.small_startup_scenario(),
        CostScenarios.mid_size_company_scenario(),
        CostScenarios.enterprise_scenario()
    ]

    for scenario in scenarios:
        print(f"\n{scenario['name']} Scenario:")
        tco = calculator.calculate_tco(scenario)

        print(f"  Setup cost: ${tco['total_setup']:,.2f}")
        print(f"  Monthly cost: ${tco['total_monthly']:,.2f}")
        print(
            f"  {scenario['time_horizon_months']}-month TCO: ${tco['total_tco']:,.2f}")
        print(f"  Cost per source: ${tco['cost_per_source']:,.2f}")
        print(f"  Cost per field: ${tco['cost_per_field']:,.2f}")


def example_cost_optimization():
    print("\n" + "="*60)
    print("EXAMPLE 5: Cost Optimization Strategies")
    print("="*60)

    calculator = TCOCalculator()
    optimizer = CostOptimizer()

    # Get baseline TCO
    scenario = CostScenarios.mid_size_company_scenario()
    baseline_tco = calculator.calculate_tco(scenario)

    # Find optimization opportunities
    drivers = optimizer.analyze_cost_drivers(baseline_tco)

    print("Cost drivers:")
    for driver in drivers:
        print(
            f"  {driver['component']:20} ${driver['monthly_cost']:8.2f} ({driver['percentage']:.1f}%)")

    # Generate strategies
    strategies = optimizer.generate_optimization_strategies(
        scenario, baseline_tco)

    print("\nTop optimization strategies:")
    for strategy in strategies[:5]:
        print(f"  {strategy['strategy']}")
        print(f"    Savings: ${strategy['potential_savings']:.2f}/month")
        print(f"    Effort: {strategy['effort']}, Risk: {strategy['risk']}")


def example_traditional_comparison():
    print("\n" + "="*60)
    print("EXAMPLE 6: AI vs Traditional ETL")
    print("="*60)

    calculator = TCOCalculator()

    scenario = CostScenarios.mid_size_company_scenario()
    comparison = calculator.compare_with_traditional(scenario)

    print(f"Scenario: {scenario['name']}")
    print(f"  AI Approach TCO: ${comparison['ai_tco']:,.2f}")
    print(f"  Traditional ETL TCO: ${comparison['traditional_tco']:,.2f}")
    print(f"  Total Savings: ${comparison['savings']:,.2f}")
    print(f"  Savings Percentage: {comparison['savings_percentage']:.1f}%")
    print(f"  Break-even: {comparison['break_even_months']:.1f} months")


def example_cost_monitoring():
    print("\n" + "="*60)
    print("EXAMPLE 7: Cost Monitoring")
    print("="*60)

    monitor = CostMonitor()

    # Set thresholds
    monitor.set_thresholds("llm_api", warning=50, critical=100)
    monitor.set_thresholds("compute", warning=100, critical=200)

    # Simulate daily costs
    import random

    for day in range(30):
        date = datetime.now() - timedelta(days=30-day)
        costs = {
            "llm_api": random.uniform(40, 120),
            "compute": random.uniform(80, 150),
            "storage": random.uniform(10, 20),
            "human_validation": random.uniform(50, 100)
        }
        monitor.track_daily_costs(date, costs)

    # Check for anomalies
    anomalies = monitor.detect_anomalies()
    if anomalies:
        print("Anomalies detected:")
        for anomaly in anomalies:
            print(
                f"  {anomaly['component']}: ${anomaly['latest_cost']:.2f} (z-score: {anomaly['z_score']:.2f})")

    # Generate report
    report = monitor.generate_cost_report()
    print(f"\n30-day Cost Report:")
    print(f"  Total: ${report['total_cost']:.2f}")
    print(f"  Daily Average: ${report['daily_average']:.2f}")
    print(f"  Trend: {report['trend_percentage']:+.1f}%")
    print(
        f"  Alerts: {report['alerts_count']} ({report['critical_alerts']} critical)")

# Main execution


def main():
    print("="*60)
    print("MODULE 10: COMPREHENSIVE COST ANALYSIS")
    print("="*60)
    print("\nDetailed cost analysis for semantic integration systems")

    # Run examples
    example_basic_cost_calculation()
    example_platform_comparison()
    example_llm_comparison()
    example_tco_analysis()
    example_cost_optimization()
    example_traditional_comparison()
    example_cost_monitoring()

    print("\n" + "="*60)
    print("KEY COST INSIGHTS")
    print("="*60)

    print("\nMajor cost components:")
    print("- LLM API calls (20-40% of total)")
    print("- Human validation (15-30% of total)")
    print("- Compute resources (10-25% of total)")
    print("- Storage (5-15% of total)")
    print("- Maintenance (10-20% of total)")

    print("\nCost optimization priorities:")
    print("1. Cache LLM responses aggressively")
    print("2. Use confidence thresholds to reduce validation")
    print("3. Choose appropriate LLM model for accuracy needs")
    print("4. Use spot instances for batch processing")
    print("5. Implement data lifecycle management")

    print("\nBreak-even analysis:")
    print("- Small scale: 6-9 months vs traditional ETL")
    print("- Medium scale: 3-6 months")
    print("- Large scale: 2-4 months")

    print("\nCost scaling characteristics:")
    print("- LLM costs scale linearly with fields")
    print("- Compute scales sub-linearly (parallelization)")
    print("- Human costs scale with uncertainty")
    print("- Storage scales with retention policy")


if __name__ == "__main__":
    main()
