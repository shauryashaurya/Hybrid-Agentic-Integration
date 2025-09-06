# module_7_streaming.py
# Module 7: Streaming Integration for Real-time Schema Matching
# Kafka, schema evolution, and incremental learning

import os
import json
import time
import pickle
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
import threading
from collections import deque
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.streaming import StreamingQuery

# For Kafka simulation in local mode
import queue
import random

# Import from previous modules
from llm_providers import llm_interface, llm_json_call

# Part 1: Streaming Schema Registry


@dataclass
class SchemaVersion:
    # Represents a version of a schema
    version_id: str
    schema: Dict[str, Any]
    timestamp: datetime
    source: str
    fields: List[str]
    field_types: Dict[str, str]

    def is_compatible_with(self, other: 'SchemaVersion') -> bool:
        # Check backward compatibility
        # All fields in old schema should exist in new schema
        return all(field in other.fields for field in self.fields)

    def get_changes_from(self, other: 'SchemaVersion') -> Dict[str, Any]:
        # Detect changes from another version
        added_fields = set(other.fields) - set(self.fields)
        removed_fields = set(self.fields) - set(other.fields)

        type_changes = {}
        for field in set(self.fields) & set(other.fields):
            if self.field_types.get(field) != other.field_types.get(field):
                type_changes[field] = {
                    'from': self.field_types.get(field),
                    'to': other.field_types.get(field)
                }

        return {
            'added': list(added_fields),
            'removed': list(removed_fields),
            'type_changes': type_changes
        }


class StreamingSchemaRegistry:
    # Manages schema evolution in streaming context

    def __init__(self):
        self.schemas = {}  # source -> list of SchemaVersion
        self.current_schemas = {}  # source -> current SchemaVersion
        self.evolution_log = []
        self.compatibility_mode = 'BACKWARD'  # BACKWARD, FORWARD, FULL, NONE

    def register_schema(self, source: str, schema: Dict[str, Any]) -> SchemaVersion:
        # Register new schema version

        version_id = f"{source}_v{len(self.schemas.get(source, [])) + 1}"

        # Extract fields and types
        fields = []
        field_types = {}

        if 'fields' in schema:
            for field in schema['fields']:
                field_name = field.get('name', field.get('field_name'))
                fields.append(field_name)
                field_types[field_name] = field.get('type', 'unknown')

        schema_version = SchemaVersion(
            version_id=version_id,
            schema=schema,
            timestamp=datetime.now(),
            source=source,
            fields=fields,
            field_types=field_types
        )

        # Check compatibility
        if source in self.current_schemas:
            current = self.current_schemas[source]

            if self.compatibility_mode == 'BACKWARD':
                if not current.is_compatible_with(schema_version):
                    changes = current.get_changes_from(schema_version)
                    print(f"Schema evolution detected for {source}:")
                    print(f"  Added fields: {changes['added']}")
                    print(f"  Removed fields: {changes['removed']}")
                    print(f"  Type changes: {changes['type_changes']}")

            # Log evolution
            self.evolution_log.append({
                'source': source,
                'from_version': current.version_id,
                'to_version': schema_version.version_id,
                'timestamp': datetime.now(),
                'changes': current.get_changes_from(schema_version)
            })

        # Store schema
        if source not in self.schemas:
            self.schemas[source] = []

        self.schemas[source].append(schema_version)
        self.current_schemas[source] = schema_version

        return schema_version

    def get_unified_schema(self) -> Dict[str, Any]:
        # Get unified schema across all sources

        all_fields = set()
        field_sources = {}  # field -> list of sources

        for source, schema_version in self.current_schemas.items():
            for field in schema_version.fields:
                all_fields.add(field)
                if field not in field_sources:
                    field_sources[field] = []
                field_sources[field].append(source)

        return {
            'fields': list(all_fields),
            'field_sources': field_sources,
            'sources': list(self.current_schemas.keys()),
            'version': datetime.now().isoformat()
        }

# Part 2: Kafka Integration Layer


class KafkaSimulator:
    # Simulates Kafka for local testing without Kafka infrastructure

    def __init__(self):
        self.topics = {}  # topic -> queue
        self.consumers = {}  # consumer_group -> offset

    def create_topic(self, topic: str):
        if topic not in self.topics:
            self.topics[topic] = deque(maxlen=10000)
            print(f"Created topic: {topic}")

    def produce(self, topic: str, key: str, value: Dict[str, Any]):
        # Produce message to topic
        if topic not in self.topics:
            self.create_topic(topic)

        message = {
            'key': key,
            'value': value,
            'timestamp': datetime.now().isoformat(),
            'offset': len(self.topics[topic])
        }

        self.topics[topic].append(message)

    def consume(self, topic: str, consumer_group: str, batch_size: int = 10) -> List[Dict]:
        # Consume messages from topic
        if topic not in self.topics:
            return []

        if consumer_group not in self.consumers:
            self.consumers[consumer_group] = 0

        offset = self.consumers[consumer_group]
        messages = []

        topic_messages = list(self.topics[topic])

        for i in range(offset, min(offset + batch_size, len(topic_messages))):
            messages.append(topic_messages[i])

        if messages:
            self.consumers[consumer_group] = offset + len(messages)

        return messages


class StreamingDataSource:
    # Represents a streaming data source

    def __init__(self, source_id: str, kafka_sim: KafkaSimulator):
        self.source_id = source_id
        self.kafka = kafka_sim
        self.topic = f"data_{source_id}"
        self.schema_topic = f"schema_{source_id}"
        self.kafka.create_topic(self.topic)
        self.kafka.create_topic(self.schema_topic)
        self.message_count = 0

    def send_record(self, record: Dict[str, Any]):
        # Send single record
        self.kafka.produce(
            self.topic,
            key=f"{self.source_id}_{self.message_count}",
            value=record
        )
        self.message_count += 1

    def send_batch(self, records: List[Dict[str, Any]]):
        # Send batch of records
        for record in records:
            self.send_record(record)

    def send_schema_change(self, new_schema: Dict[str, Any]):
        # Notify schema change
        self.kafka.produce(
            self.schema_topic,
            key=f"{self.source_id}_schema",
            value=new_schema
        )

# Part 3: Streaming Pattern Matcher


class StreamingPatternMatcher:
    # Real-time pattern matching for streaming data

    def __init__(self, window_size: int = 1000):
        self.window_size = window_size
        self.field_statistics = {}  # field -> statistics
        # (source_field, target_field) -> confidence
        self.mapping_confidence = {}
        self.sample_buffer = {}  # field -> recent samples

    def update_field_statistics(self, field: str, value: Any):
        # Update running statistics for a field

        if field not in self.field_statistics:
            self.field_statistics[field] = {
                'count': 0,
                'null_count': 0,
                'distinct_values': set(),
                'type_counts': {},
                'patterns': {}
            }

        stats = self.field_statistics[field]
        stats['count'] += 1

        if value is None:
            stats['null_count'] += 1
        else:
            # Track type
            value_type = type(value).__name__
            stats['type_counts'][value_type] = stats['type_counts'].get(
                value_type, 0) + 1

            # Track distinct values (up to a limit)
            if len(stats['distinct_values']) < 1000:
                stats['distinct_values'].add(str(value))

            # Detect patterns
            if isinstance(value, str):
                if '@' in value:
                    stats['patterns']['email'] = stats['patterns'].get(
                        'email', 0) + 1
                elif value.startswith('http'):
                    stats['patterns']['url'] = stats['patterns'].get(
                        'url', 0) + 1
                elif len(value) > 5 and value[:3].isupper() and value[3:].isdigit():
                    stats['patterns']['identifier'] = stats['patterns'].get(
                        'identifier', 0) + 1

        # Update sample buffer
        if field not in self.sample_buffer:
            self.sample_buffer[field] = deque(maxlen=100)
        self.sample_buffer[field].append(value)

    def calculate_streaming_similarity(self, source_field: str, target_field: str) -> float:
        # Calculate similarity based on streaming statistics

        if source_field not in self.field_statistics or target_field not in self.field_statistics:
            return 0.0

        source_stats = self.field_statistics[source_field]
        target_stats = self.field_statistics[target_field]

        # Name similarity
        name_sim = self.calculate_name_similarity(source_field, target_field)

        # Type similarity
        source_types = set(source_stats['type_counts'].keys())
        target_types = set(target_stats['type_counts'].keys())
        type_sim = len(source_types & target_types) / len(source_types |
                                                          target_types) if source_types | target_types else 0

        # Pattern similarity
        source_patterns = set(source_stats['patterns'].keys())
        target_patterns = set(target_stats['patterns'].keys())
        pattern_sim = len(source_patterns & target_patterns) / len(source_patterns |
                                                                   target_patterns) if source_patterns | target_patterns else 0

        # Value overlap for categorical fields
        value_sim = 0.0
        if len(source_stats['distinct_values']) < 100 and len(target_stats['distinct_values']) < 100:
            overlap = len(source_stats['distinct_values']
                          & target_stats['distinct_values'])
            total = len(source_stats['distinct_values']
                        | target_stats['distinct_values'])
            value_sim = overlap / total if total > 0 else 0

        # Weighted combination
        similarity = (
            name_sim * 0.4 +
            type_sim * 0.3 +
            pattern_sim * 0.2 +
            value_sim * 0.1
        )

        return similarity

    def calculate_name_similarity(self, name1: str, name2: str) -> float:
        # Simple name similarity
        if name1.lower() == name2.lower():
            return 1.0

        tokens1 = set(name1.lower().replace('_', ' ').split())
        tokens2 = set(name2.lower().replace('_', ' ').split())

        if not tokens1 or not tokens2:
            return 0.0

        intersection = tokens1 & tokens2
        union = tokens1 | tokens2

        return len(intersection) / len(union)

    def update_mapping_confidence(self, source_field: str, target_field: str):
        # Update confidence score for a field mapping

        key = (source_field, target_field)
        current_confidence = self.mapping_confidence.get(key, 0.5)

        # Calculate new similarity
        new_similarity = self.calculate_streaming_similarity(
            source_field, target_field)

        # Exponential moving average
        alpha = 0.1  # Learning rate
        updated_confidence = alpha * new_similarity + \
            (1 - alpha) * current_confidence

        self.mapping_confidence[key] = updated_confidence

    def get_best_mapping(self, source_field: str, target_fields: List[str]) -> Tuple[str, float]:
        # Get best mapping for a source field

        best_target = None
        best_confidence = 0.0

        for target in target_fields:
            key = (source_field, target)
            confidence = self.mapping_confidence.get(key, 0.0)

            if confidence > best_confidence:
                best_confidence = confidence
                best_target = target

        return best_target, best_confidence

# Part 4: Incremental Learning System


class IncrementalLearner:
    # Learn and adapt from streaming feedback

    def __init__(self, learning_rate: float = 0.01):
        self.learning_rate = learning_rate
        self.mapping_weights = {}  # (source, target) -> weight
        self.feedback_buffer = deque(maxlen=1000)
        self.model_version = 0

    def add_feedback(self, source_field: str, target_field: str, is_correct: bool):
        # Add user feedback

        feedback = {
            'source': source_field,
            'target': target_field,
            'correct': is_correct,
            'timestamp': datetime.now()
        }

        self.feedback_buffer.append(feedback)

        # Update weights immediately
        self.update_weights(source_field, target_field, is_correct)

    def update_weights(self, source_field: str, target_field: str, is_correct: bool):
        # Update mapping weights based on feedback

        key = (source_field, target_field)
        current_weight = self.mapping_weights.get(key, 0.5)

        if is_correct:
            # Increase weight for correct mapping
            new_weight = current_weight + \
                self.learning_rate * (1 - current_weight)
        else:
            # Decrease weight for incorrect mapping
            new_weight = current_weight - self.learning_rate * current_weight

        self.mapping_weights[key] = max(0.0, min(1.0, new_weight))

    def get_adjusted_confidence(self, source_field: str, target_field: str,
                                base_confidence: float) -> float:
        # Adjust confidence based on learned weights

        key = (source_field, target_field)
        weight = self.mapping_weights.get(key, 0.5)

        # Combine base confidence with learned weight
        adjusted = base_confidence * 0.7 + weight * 0.3

        return adjusted

    def periodic_retraining(self):
        # Periodic batch retraining from feedback buffer

        if len(self.feedback_buffer) < 100:
            return

        print(f"Retraining with {len(self.feedback_buffer)} feedback samples")

        # Group feedback by mapping
        mapping_feedback = {}
        for feedback in self.feedback_buffer:
            key = (feedback['source'], feedback['target'])
            if key not in mapping_feedback:
                mapping_feedback[key] = {'correct': 0, 'total': 0}

            mapping_feedback[key]['total'] += 1
            if feedback['correct']:
                mapping_feedback[key]['correct'] += 1

        # Update weights based on aggregated feedback
        for key, stats in mapping_feedback.items():
            accuracy = stats['correct'] / stats['total']
            self.mapping_weights[key] = accuracy

        self.model_version += 1
        print(f"Model updated to version {self.model_version}")

# Part 5: State Management


class StreamingStateManager:
    # Manage state for streaming operations

    def __init__(self, checkpoint_dir: str = "streaming_checkpoints"):
        self.checkpoint_dir = checkpoint_dir
        self.state = {}
        self.watermarks = {}  # source -> timestamp
        self.checkpoint_interval = 100  # messages
        self.message_count = 0

        os.makedirs(checkpoint_dir, exist_ok=True)

    def update_state(self, key: str, value: Any):
        # Update state
        self.state[key] = value
        self.message_count += 1

        # Periodic checkpoint
        if self.message_count % self.checkpoint_interval == 0:
            self.checkpoint()

    def get_state(self, key: str, default: Any = None) -> Any:
        # Get state value
        return self.state.get(key, default)

    def update_watermark(self, source: str, timestamp: datetime):
        # Update watermark for a source
        if source not in self.watermarks or timestamp > self.watermarks[source]:
            self.watermarks[source] = timestamp

    def checkpoint(self):
        # Save state to disk
        checkpoint_file = os.path.join(
            self.checkpoint_dir,
            f"checkpoint_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pkl"
        )

        checkpoint_data = {
            'state': self.state,
            'watermarks': self.watermarks,
            'message_count': self.message_count,
            'timestamp': datetime.now()
        }

        with open(checkpoint_file, 'wb') as f:
            pickle.dump(checkpoint_data, f)

        print(f"Checkpoint saved: {checkpoint_file}")

    def restore_latest_checkpoint(self) -> bool:
        # Restore from latest checkpoint
        checkpoints = [f for f in os.listdir(
            self.checkpoint_dir) if f.endswith('.pkl')]

        if not checkpoints:
            return False

        latest = max(checkpoints)
        checkpoint_file = os.path.join(self.checkpoint_dir, latest)

        with open(checkpoint_file, 'rb') as f:
            checkpoint_data = pickle.load(f)

        self.state = checkpoint_data['state']
        self.watermarks = checkpoint_data['watermarks']
        self.message_count = checkpoint_data['message_count']

        print(f"Restored from checkpoint: {checkpoint_file}")
        return True

# Part 6: Streaming Integration Pipeline


class StreamingIntegrationPipeline:
    # Complete streaming integration pipeline

    def __init__(self):
        self.kafka = KafkaSimulator()
        self.schema_registry = StreamingSchemaRegistry()
        self.pattern_matcher = StreamingPatternMatcher()
        self.learner = IncrementalLearner()
        self.state_manager = StreamingStateManager()
        self.sources = {}
        self.running = False

    def register_source(self, source_id: str, initial_schema: Dict[str, Any]):
        # Register a streaming source

        source = StreamingDataSource(source_id, self.kafka)
        self.sources[source_id] = source

        # Register schema
        self.schema_registry.register_schema(source_id, initial_schema)

        print(f"Registered streaming source: {source_id}")

        return source

    def process_stream(self, source_id: str, max_messages: int = None):
        # Process messages from a stream

        if source_id not in self.sources:
            print(f"Unknown source: {source_id}")
            return

        source = self.sources[source_id]
        consumer_group = f"processor_{source_id}"

        messages_processed = 0

        while self.running:
            # Check for schema changes
            schema_messages = self.kafka.consume(
                source.schema_topic, f"schema_{consumer_group}", 1)

            for msg in schema_messages:
                new_schema = msg['value']
                print(f"Schema change detected for {source_id}")
                self.schema_registry.register_schema(source_id, new_schema)

            # Process data messages
            data_messages = self.kafka.consume(
                source.topic, consumer_group, 10)

            if not data_messages:
                time.sleep(0.1)
                continue

            for msg in data_messages:
                record = msg['value']

                # Update field statistics
                for field, value in record.items():
                    self.pattern_matcher.update_field_statistics(
                        f"{source_id}.{field}", value)

                # Update watermark
                timestamp = datetime.fromisoformat(msg['timestamp'])
                self.state_manager.update_watermark(source_id, timestamp)

                messages_processed += 1

                if max_messages and messages_processed >= max_messages:
                    return

            # Periodic operations
            if messages_processed % 100 == 0:
                self.update_mappings()
                self.learner.periodic_retraining()

    def update_mappings(self):
        # Update field mappings based on streaming statistics

        unified = self.schema_registry.get_unified_schema()

        for source in unified['sources']:
            schema = self.schema_registry.current_schemas.get(source)
            if not schema:
                continue

            for field in schema.fields:
                source_field = f"{source}.{field}"

                # Find potential mappings
                for other_source in unified['sources']:
                    if other_source == source:
                        continue

                    other_schema = self.schema_registry.current_schemas.get(
                        other_source)
                    if not other_schema:
                        continue

                    for other_field in other_schema.fields:
                        target_field = f"{other_source}.{other_field}"

                        # Update mapping confidence
                        self.pattern_matcher.update_mapping_confidence(
                            source_field, target_field)

    def start_processing(self):
        # Start processing all streams

        self.running = True

        # Restore state if available
        self.state_manager.restore_latest_checkpoint()

        threads = []
        for source_id in self.sources:
            thread = threading.Thread(
                target=self.process_stream,
                args=(source_id,),
                daemon=True
            )
            thread.start()
            threads.append(thread)

        print(f"Started processing {len(threads)} streams")

        return threads

    def stop_processing(self):
        # Stop processing

        self.running = False
        self.state_manager.checkpoint()
        print("Stopped stream processing")

# Part 7: Streaming Query Interface


class StreamingQueryEngine:
    # Execute queries on streaming data

    def __init__(self, pipeline: StreamingIntegrationPipeline):
        self.pipeline = pipeline
        self.query_cache = {}

    def execute_snapshot_query(self, sql: str) -> List[Dict[str, Any]]:
        # Execute query on current snapshot

        # Get current field statistics
        stats = self.pipeline.pattern_matcher.field_statistics

        # Simple query parsing (very basic)
        if "count" in sql.lower():
            # Count query
            total = sum(s['count'] for s in stats.values())
            return [{'count': total}]

        elif "select * from" in sql.lower():
            # Return sample data
            results = []
            for field, samples in self.pipeline.pattern_matcher.sample_buffer.items():
                for sample in list(samples)[:10]:
                    results.append({field: sample})
            return results

        return []

    def execute_windowed_aggregation(self, source: str, field: str,
                                     window_minutes: int = 5) -> Dict[str, Any]:
        # Execute windowed aggregation

        watermark = self.pipeline.state_manager.watermarks.get(source)
        if not watermark:
            return {}

        window_start = watermark - timedelta(minutes=window_minutes)

        field_key = f"{source}.{field}"
        stats = self.pipeline.pattern_matcher.field_statistics.get(
            field_key, {})

        return {
            'source': source,
            'field': field,
            'window_start': window_start.isoformat(),
            'window_end': watermark.isoformat(),
            'count': stats.get('count', 0),
            'null_rate': stats.get('null_count', 0) / stats.get('count', 1),
            'distinct_values': len(stats.get('distinct_values', set()))
        }

# Example functions


def example_streaming_basic():
    print("\n" + "="*60)
    print("EXAMPLE 1: Basic Streaming Setup")
    print("="*60)

    pipeline = StreamingIntegrationPipeline()

    # Register sources with schemas
    schema1 = {
        'fields': [
            {'name': 'customer_id', 'type': 'string'},
            {'name': 'name', 'type': 'string'},
            {'name': 'amount', 'type': 'float'}
        ]
    }

    schema2 = {
        'fields': [
            {'name': 'cust_id', 'type': 'string'},
            {'name': 'customer_name', 'type': 'string'},
            {'name': 'total_amount', 'type': 'float'}
        ]
    }

    source1 = pipeline.register_source('stream1', schema1)
    source2 = pipeline.register_source('stream2', schema2)

    # Send some data
    print("\nSending test data...")

    for i in range(10):
        source1.send_record({
            'customer_id': f'CUST{i:04d}',
            'name': f'Customer {i}',
            'amount': random.uniform(10, 100)
        })

        source2.send_record({
            'cust_id': f'CUST{i:04d}',
            'customer_name': f'Customer {i}',
            'total_amount': random.uniform(10, 100)
        })

    # Process messages
    pipeline.running = True
    pipeline.process_stream('stream1', max_messages=10)

    # Show statistics
    print("\nField statistics:")
    for field, stats in pipeline.pattern_matcher.field_statistics.items():
        print(f"  {field}: {stats['count']} records")


def example_schema_evolution():
    print("\n" + "="*60)
    print("EXAMPLE 2: Schema Evolution")
    print("="*60)

    pipeline = StreamingIntegrationPipeline()

    # Initial schema
    schema_v1 = {
        'fields': [
            {'name': 'id', 'type': 'string'},
            {'name': 'value', 'type': 'float'}
        ]
    }

    source = pipeline.register_source('evolving_stream', schema_v1)

    # Send data with v1 schema
    for i in range(5):
        source.send_record({'id': f'ID{i}', 'value': i * 10.0})

    # Evolve schema - add new field
    schema_v2 = {
        'fields': [
            {'name': 'id', 'type': 'string'},
            {'name': 'value', 'type': 'float'},
            {'name': 'timestamp', 'type': 'string'}  # New field
        ]
    }

    source.send_schema_change(schema_v2)
    pipeline.schema_registry.register_schema('evolving_stream', schema_v2)

    # Send data with v2 schema
    for i in range(5, 10):
        source.send_record({
            'id': f'ID{i}',
            'value': i * 10.0,
            'timestamp': datetime.now().isoformat()
        })

    # Show evolution log
    print("\nSchema evolution log:")
    for evolution in pipeline.schema_registry.evolution_log:
        print(
            f"  {evolution['source']}: {evolution['from_version']} -> {evolution['to_version']}")
        print(f"    Changes: {evolution['changes']}")


def example_streaming_pattern_matching():
    print("\n" + "="*60)
    print("EXAMPLE 3: Streaming Pattern Matching")
    print("="*60)

    pipeline = StreamingIntegrationPipeline()

    # Register two similar sources
    schema1 = {
        'fields': [
            {'name': 'customer_id', 'type': 'string'},
            {'name': 'order_date', 'type': 'string'},
            {'name': 'amount', 'type': 'float'}
        ]
    }

    schema2 = {
        'fields': [
            {'name': 'cust_id', 'type': 'string'},
            {'name': 'date', 'type': 'string'},
            {'name': 'total', 'type': 'float'}
        ]
    }

    source1 = pipeline.register_source('orders_v1', schema1)
    source2 = pipeline.register_source('orders_v2', schema2)

    # Generate similar data
    print("\nGenerating streaming data...")

    for i in range(100):
        date = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}"

        source1.send_record({
            'customer_id': f'CUST{random.randint(1,20):04d}',
            'order_date': date,
            'amount': random.uniform(10, 500)
        })

        source2.send_record({
            'cust_id': f'CUST{random.randint(1,20):04d}',
            'date': date,
            'total': random.uniform(10, 500)
        })

    # Process streams
    pipeline.running = True
    pipeline.process_stream('orders_v1', max_messages=100)
    pipeline.process_stream('orders_v2', max_messages=100)

    # Update mappings
    pipeline.update_mappings()

    # Show mapping confidence
    print("\nField mapping confidence:")
    for (source, target), confidence in pipeline.pattern_matcher.mapping_confidence.items():
        if confidence > 0.5:
            print(f"  {source} -> {target}: {confidence:.3f}")


def example_incremental_learning():
    print("\n" + "="*60)
    print("EXAMPLE 4: Incremental Learning")
    print("="*60)

    pipeline = StreamingIntegrationPipeline()

    # Simulate user feedback
    print("\nSimulating user feedback...")

    # Correct mappings
    pipeline.learner.add_feedback('customer_id', 'cust_id', True)
    pipeline.learner.add_feedback('amount', 'total', True)
    pipeline.learner.add_feedback('order_date', 'date', True)

    # Incorrect mappings
    pipeline.learner.add_feedback('customer_id', 'date', False)
    pipeline.learner.add_feedback('amount', 'cust_id', False)

    # Show learned weights
    print("\nLearned mapping weights:")
    for (source, target), weight in pipeline.learner.mapping_weights.items():
        print(f"  {source} -> {target}: {weight:.3f}")

    # Test adjusted confidence
    base_confidence = 0.6
    adjusted = pipeline.learner.get_adjusted_confidence(
        'customer_id', 'cust_id', base_confidence)
    print(f"\nBase confidence: {base_confidence:.3f}")
    print(f"Adjusted confidence: {adjusted:.3f}")

    # Trigger retraining
    for _ in range(100):
        pipeline.learner.add_feedback(
            'customer_id',
            'cust_id',
            random.random() > 0.2  # 80% correct
        )

    pipeline.learner.periodic_retraining()


def example_state_management():
    print("\n" + "="*60)
    print("EXAMPLE 5: State Management")
    print("="*60)

    # Create pipeline with state
    pipeline = StreamingIntegrationPipeline()

    # Update state
    pipeline.state_manager.update_state('processed_count', 0)
    pipeline.state_manager.update_state('last_processing_time', datetime.now())

    # Process some data
    for i in range(150):  # Trigger checkpoint
        count = pipeline.state_manager.get_state('processed_count', 0)
        pipeline.state_manager.update_state('processed_count', count + 1)

    print(
        f"\nProcessed: {pipeline.state_manager.get_state('processed_count')} messages")

    # Simulate failure and recovery
    print("\nSimulating failure...")

    # Create new pipeline (simulating restart)
    new_pipeline = StreamingIntegrationPipeline()

    # Restore state
    if new_pipeline.state_manager.restore_latest_checkpoint():
        recovered_count = new_pipeline.state_manager.get_state(
            'processed_count', 0)
        print(f"Recovered state: {recovered_count} messages processed")


def example_streaming_queries():
    print("\n" + "="*60)
    print("EXAMPLE 6: Streaming Queries")
    print("="*60)

    pipeline = StreamingIntegrationPipeline()
    query_engine = StreamingQueryEngine(pipeline)

    # Setup source
    schema = {
        'fields': [
            {'name': 'sensor_id', 'type': 'string'},
            {'name': 'temperature', 'type': 'float'},
            {'name': 'timestamp', 'type': 'string'}
        ]
    }

    source = pipeline.register_source('sensors', schema)

    # Stream sensor data
    print("\nStreaming sensor data...")

    for i in range(50):
        source.send_record({
            'sensor_id': f'SENSOR_{random.randint(1,5)}',
            'temperature': random.uniform(20, 30),
            'timestamp': datetime.now().isoformat()
        })

    # Process
    pipeline.running = True
    pipeline.process_stream('sensors', max_messages=50)

    # Execute queries
    print("\nSnapshot query results:")
    results = query_engine.execute_snapshot_query(
        "SELECT COUNT(*) FROM sensors")
    print(f"  Total records: {results}")

    # Windowed aggregation
    print("\nWindowed aggregation:")
    agg = query_engine.execute_windowed_aggregation(
        'sensors', 'temperature', window_minutes=5)
    print(f"  Temperature readings in last 5 minutes: {agg.get('count', 0)}")

# Main execution


def main():
    print("="*60)
    print("MODULE 7: STREAMING INTEGRATION")
    print("="*60)
    print("\nReal-time schema matching and evolution")

    # Run examples
    example_streaming_basic()
    example_schema_evolution()
    example_streaming_pattern_matching()
    example_incremental_learning()
    example_state_management()
    example_streaming_queries()

    print("\n" + "="*60)
    print("KEY CONCEPTS")
    print("="*60)

    print("\nStreaming challenges addressed:")
    print("- Schema evolution without downtime")
    print("- Incremental learning from feedback")
    print("- State management and fault tolerance")
    print("- Real-time pattern matching")
    print("- Windowed aggregations")

    print("\nProduction considerations:")
    print("- Use real Kafka for production")
    print("- Implement proper error handling")
    print("- Monitor lag and throughput")
    print("- Set appropriate watermarks")
    print("- Plan checkpoint storage")

    print("\nWhen to use streaming:")
    print("- Real-time data integration needed")
    print("- Schemas change frequently")
    print("- Continuous learning required")
    print("- Low latency requirements")


if __name__ == "__main__":
    main()
