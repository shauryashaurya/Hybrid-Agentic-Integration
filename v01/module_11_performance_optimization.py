# module_11_performance_optimization.py
# Module 11: Comprehensive Performance Optimization Framework
# Detailed strategies for optimizing every aspect of the integration pipeline

import os
import time
import json
import hashlib
import pickle
from typing import List, Dict, Any, Optional, Tuple, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import deque, OrderedDict
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import threading
import multiprocessing
import pandas as pd
import numpy as np
from enum import Enum

# Part 1: Performance Bottleneck Analysis


class BottleneckType(Enum):
    # Types of performance bottlenecks
    IO_BOUND = "io_bound"
    CPU_BOUND = "cpu_bound"
    MEMORY_BOUND = "memory_bound"
    NETWORK_BOUND = "network_bound"
    API_RATE_LIMITED = "api_rate_limited"
    HUMAN_BLOCKED = "human_blocked"


@dataclass
class PerformanceMetric:
    # Track performance metrics
    operation: str
    start_time: float
    end_time: float
    records_processed: int
    memory_used_mb: float
    cpu_percent: float
    io_wait_percent: float

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time

    @property
    def throughput(self) -> float:
        if self.duration > 0:
            return self.records_processed / self.duration
        return 0

    @property
    def bottleneck_type(self) -> BottleneckType:
        # Identify bottleneck based on metrics
        if self.io_wait_percent > 50:
            return BottleneckType.IO_BOUND
        elif self.cpu_percent > 80:
            return BottleneckType.CPU_BOUND
        elif self.memory_used_mb > 3500:  # Near 4GB limit
            return BottleneckType.MEMORY_BOUND
        else:
            return BottleneckType.NETWORK_BOUND


class PerformanceProfiler:
    # Profile system performance

    def __init__(self):
        self.metrics = []
        self.operation_stats = {}

    def profile_operation(self, operation_name: str, func: Callable, *args, **kwargs) -> Any:
        # Profile a single operation

        import psutil
        process = psutil.Process()

        # Before metrics
        start_time = time.time()
        start_memory = process.memory_info().rss / 1024 / 1024
        start_cpu = process.cpu_percent()

        # Execute operation
        result = func(*args, **kwargs)

        # After metrics
        end_time = time.time()
        end_memory = process.memory_info().rss / 1024 / 1024
        end_cpu = process.cpu_percent()

        # IO wait (approximate)
        io_counters = process.io_counters()
        io_wait = (io_counters.read_time + io_counters.write_time) / \
            1000  # Convert to seconds
        io_wait_percent = (io_wait / (end_time - start_time)) * \
            100 if end_time > start_time else 0

        # Record metric
        metric = PerformanceMetric(
            operation=operation_name,
            start_time=start_time,
            end_time=end_time,
            records_processed=kwargs.get('record_count', 0),
            memory_used_mb=end_memory - start_memory,
            cpu_percent=end_cpu,
            io_wait_percent=io_wait_percent
        )

        self.metrics.append(metric)

        # Update statistics
        if operation_name not in self.operation_stats:
            self.operation_stats[operation_name] = {
                'count': 0,
                'total_time': 0,
                'avg_time': 0,
                'max_time': 0,
                'min_time': float('inf')
            }

        stats = self.operation_stats[operation_name]
        stats['count'] += 1
        stats['total_time'] += metric.duration
        stats['avg_time'] = stats['total_time'] / stats['count']
        stats['max_time'] = max(stats['max_time'], metric.duration)
        stats['min_time'] = min(stats['min_time'], metric.duration)

        return result

    def identify_bottlenecks(self) -> Dict[str, List[str]]:
        # Identify performance bottlenecks

        bottlenecks = {
            BottleneckType.IO_BOUND.value: [],
            BottleneckType.CPU_BOUND.value: [],
            BottleneckType.MEMORY_BOUND.value: [],
            BottleneckType.NETWORK_BOUND.value: []
        }

        for metric in self.metrics:
            bottleneck = metric.bottleneck_type.value
            if metric.operation not in bottlenecks[bottleneck]:
                bottlenecks[bottleneck].append(metric.operation)

        return bottlenecks

    def generate_performance_report(self) -> Dict[str, Any]:
        # Generate comprehensive performance report

        if not self.metrics:
            return {}

        total_time = sum(m.duration for m in self.metrics)

        # Operation breakdown
        operation_breakdown = {}
        for op, stats in self.operation_stats.items():
            operation_breakdown[op] = {
                'percentage': (stats['total_time'] / total_time) * 100,
                'avg_time': stats['avg_time'],
                'calls': stats['count']
            }

        # Bottleneck analysis
        bottlenecks = self.identify_bottlenecks()

        return {
            'total_operations': len(self.metrics),
            'total_time': total_time,
            'operation_breakdown': operation_breakdown,
            'bottlenecks': bottlenecks,
            'recommendations': self.generate_recommendations(bottlenecks)
        }

    def generate_recommendations(self, bottlenecks: Dict[str, List[str]]) -> List[str]:
        # Generate optimization recommendations

        recommendations = []

        if bottlenecks[BottleneckType.IO_BOUND.value]:
            recommendations.append(
                "Implement async IO or use memory-mapped files")
            recommendations.append(
                "Consider SSD storage for better IO performance")

        if bottlenecks[BottleneckType.CPU_BOUND.value]:
            recommendations.append(
                "Use multiprocessing for CPU-intensive operations")
            recommendations.append("Consider vectorized operations with NumPy")

        if bottlenecks[BottleneckType.MEMORY_BOUND.value]:
            recommendations.append(
                "Process data in chunks to reduce memory usage")
            recommendations.append(
                "Use generators instead of loading all data")

        if bottlenecks[BottleneckType.NETWORK_BOUND.value]:
            recommendations.append(
                "Implement request batching and connection pooling")
            recommendations.append("Add local caching for network resources")

        return recommendations

# Part 2: Caching Strategies


class CacheStrategy(Enum):
    # Different caching strategies
    LRU = "lru"  # Least Recently Used
    LFU = "lfu"  # Least Frequently Used
    TTL = "ttl"  # Time To Live
    ADAPTIVE = "adaptive"  # Adaptive based on usage patterns


class OptimizedCache:
    # High-performance cache implementation

    def __init__(self, strategy: CacheStrategy = CacheStrategy.LRU,
                 max_size: int = 1000, ttl_seconds: int = 3600):
        self.strategy = strategy
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.cache = OrderedDict()
        self.access_counts = {}
        self.access_times = {}
        self.hit_count = 0
        self.miss_count = 0
        self.lock = threading.Lock()

    def _generate_key(self, *args, **kwargs) -> str:
        # Generate cache key from arguments
        key_data = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True)
        return hashlib.md5(key_data.encode()).hexdigest()

    def get(self, key: str) -> Optional[Any]:
        # Get value from cache
        with self.lock:
            if key in self.cache:
                # Check TTL
                if self.strategy == CacheStrategy.TTL:
                    if time.time() - self.access_times[key] > self.ttl_seconds:
                        del self.cache[key]
                        del self.access_times[key]
                        self.miss_count += 1
                        return None

                # Update access patterns
                self.hit_count += 1
                self.access_counts[key] = self.access_counts.get(key, 0) + 1
                self.access_times[key] = time.time()

                # Move to end for LRU
                if self.strategy == CacheStrategy.LRU:
                    self.cache.move_to_end(key)

                return self.cache[key]

            self.miss_count += 1
            return None

    def put(self, key: str, value: Any):
        # Put value in cache
        with self.lock:
            # Check size limit
            if len(self.cache) >= self.max_size:
                self._evict()

            self.cache[key] = value
            self.access_counts[key] = 1
            self.access_times[key] = time.time()

    def _evict(self):
        # Evict based on strategy
        if self.strategy == CacheStrategy.LRU:
            # Remove oldest
            self.cache.popitem(last=False)

        elif self.strategy == CacheStrategy.LFU:
            # Remove least frequently used
            min_key = min(self.access_counts, key=self.access_counts.get)
            del self.cache[min_key]
            del self.access_counts[min_key]

        elif self.strategy == CacheStrategy.TTL:
            # Remove expired entries
            current_time = time.time()
            expired = [k for k, t in self.access_times.items()
                       if current_time - t > self.ttl_seconds]
            for key in expired:
                if key in self.cache:
                    del self.cache[key]
                    del self.access_times[key]

            # If still over limit, remove oldest
            if len(self.cache) >= self.max_size:
                oldest_key = min(self.access_times, key=self.access_times.get)
                del self.cache[oldest_key]

        elif self.strategy == CacheStrategy.ADAPTIVE:
            # Adaptive strategy based on recency and frequency
            scores = {}
            current_time = time.time()
            for key in self.cache:
                recency_score = 1.0 / \
                    (current_time - self.access_times[key] + 1)
                frequency_score = self.access_counts.get(key, 0)
                scores[key] = recency_score * frequency_score

            min_key = min(scores, key=scores.get)
            del self.cache[min_key]

    def get_hit_rate(self) -> float:
        # Calculate cache hit rate
        total = self.hit_count + self.miss_count
        return self.hit_count / total if total > 0 else 0

    def clear(self):
        # Clear cache
        with self.lock:
            self.cache.clear()
            self.access_counts.clear()
            self.access_times.clear()
            self.hit_count = 0
            self.miss_count = 0


class MultiLevelCache:
    # Multi-level cache system (L1: memory, L2: disk)

    def __init__(self, l1_size: int = 100, l2_size: int = 1000):
        self.l1_cache = OptimizedCache(CacheStrategy.LRU, l1_size)
        self.l2_cache_dir = "cache_l2"
        self.l2_size = l2_size
        self.l2_index = OrderedDict()

        os.makedirs(self.l2_cache_dir, exist_ok=True)

    def get(self, key: str) -> Optional[Any]:
        # Try L1 first
        value = self.l1_cache.get(key)
        if value is not None:
            return value

        # Try L2
        if key in self.l2_index:
            filepath = os.path.join(self.l2_cache_dir, f"{key}.pkl")
            if os.path.exists(filepath):
                with open(filepath, 'rb') as f:
                    value = pickle.load(f)

                # Promote to L1
                self.l1_cache.put(key, value)
                return value

        return None

    def put(self, key: str, value: Any):
        # Put in L1
        self.l1_cache.put(key, value)

        # Also persist to L2
        if len(self.l2_index) >= self.l2_size:
            # Evict from L2
            oldest_key = next(iter(self.l2_index))
            filepath = os.path.join(self.l2_cache_dir, f"{oldest_key}.pkl")
            if os.path.exists(filepath):
                os.remove(filepath)
            del self.l2_index[oldest_key]

        # Write to L2
        filepath = os.path.join(self.l2_cache_dir, f"{key}.pkl")
        with open(filepath, 'wb') as f:
            pickle.dump(value, f)
        self.l2_index[key] = True

# Part 3: Parallel Processing Optimization


class ParallelProcessor:
    # Optimize parallel processing for different scenarios

    def __init__(self, max_workers: int = None):
        if max_workers is None:
            # Auto-detect optimal worker count
            cpu_count = multiprocessing.cpu_count()
            # Leave one CPU free for system
            self.max_workers = max(1, cpu_count - 1)
        else:
            self.max_workers = max_workers

        self.thread_pool = None
        self.process_pool = None

    def process_io_bound(self, func: Callable, items: List[Any],
                         batch_size: int = None) -> List[Any]:
        # Optimize for IO-bound operations using threads

        if batch_size is None:
            # Auto-determine batch size
            batch_size = max(1, len(items) // (self.max_workers * 4))

        results = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Process in batches
            batches = [items[i:i+batch_size]
                       for i in range(0, len(items), batch_size)]

            # Submit all batches
            futures = []
            for batch in batches:
                future = executor.submit(self._process_batch, func, batch)
                futures.append(future)

            # Collect results
            for future in futures:
                batch_results = future.result()
                results.extend(batch_results)

        return results

    def process_cpu_bound(self, func: Callable, items: List[Any],
                          chunk_size: int = None) -> List[Any]:
        # Optimize for CPU-bound operations using processes

        if chunk_size is None:
            # Auto-determine chunk size
            chunk_size = max(1, len(items) // (self.max_workers * 2))

        results = []

        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Process in chunks
            chunks = [items[i:i+chunk_size]
                      for i in range(0, len(items), chunk_size)]

            # Map function to chunks
            chunk_results = executor.map(self._process_chunk_cpu,
                                         [(func, chunk) for chunk in chunks])

            # Flatten results
            for chunk_result in chunk_results:
                results.extend(chunk_result)

        return results

    def _process_batch(self, func: Callable, batch: List[Any]) -> List[Any]:
        # Process a batch of items
        return [func(item) for item in batch]

    @staticmethod
    def _process_chunk_cpu(args: Tuple[Callable, List[Any]]) -> List[Any]:
        # Static method for multiprocessing
        func, chunk = args
        return [func(item) for item in chunk]

    def pipeline_parallel(self, stages: List[Callable], items: List[Any]) -> List[Any]:
        # Pipeline parallelism for multi-stage processing

        # Create queues between stages
        queues = [multiprocessing.Queue() for _ in range(len(stages) - 1)]

        # Create process for each stage
        processes = []
        for i, stage_func in enumerate(stages):
            input_queue = queues[i-1] if i > 0 else None
            output_queue = queues[i] if i < len(stages) - 1 else None

            p = multiprocessing.Process(
                target=self._run_stage,
                args=(stage_func, input_queue, output_queue)
            )
            p.start()
            processes.append(p)

        # Feed input to first stage
        for item in items:
            queues[0].put(item)

        # Signal end of input
        queues[0].put(None)

        # Collect results from last stage
        results = []
        while True:
            result = queues[-1].get()
            if result is None:
                break
            results.append(result)

        # Wait for all processes
        for p in processes:
            p.join()

        return results

    @staticmethod
    def _run_stage(func: Callable, input_queue: Any, output_queue: Any):
        # Run a pipeline stage
        while True:
            if input_queue:
                item = input_queue.get()
                if item is None:
                    if output_queue:
                        output_queue.put(None)
                    break
            else:
                # First stage - process all items
                break

            result = func(item)

            if output_queue:
                output_queue.put(result)

# Part 4: Memory Optimization


class MemoryOptimizer:
    # Optimize memory usage

    def __init__(self, max_memory_mb: float = 3500):  # Leave 500MB for system
        self.max_memory_mb = max_memory_mb
        self.current_usage_mb = 0

    def chunk_dataframe(self, df: pd.DataFrame, target_memory_mb: float = 100) -> List[pd.DataFrame]:
        # Split DataFrame into memory-efficient chunks

        # Estimate memory per row
        memory_usage = df.memory_usage(deep=True).sum() / 1024 / 1024
        rows_per_mb = len(df) / memory_usage if memory_usage > 0 else len(df)

        # Calculate chunk size
        chunk_rows = int(rows_per_mb * target_memory_mb)
        chunk_rows = max(1, min(chunk_rows, len(df)))

        # Create chunks
        chunks = []
        for start in range(0, len(df), chunk_rows):
            chunk = df.iloc[start:start + chunk_rows].copy()
            chunks.append(chunk)

        return chunks

    def optimize_dtypes(self, df: pd.DataFrame) -> pd.DataFrame:
        # Optimize DataFrame dtypes to reduce memory

        optimized = df.copy()

        for col in optimized.columns:
            col_type = optimized[col].dtype

            if col_type != 'object':
                c_min = optimized[col].min()
                c_max = optimized[col].max()

                # Optimize integers
                if str(col_type)[:3] == 'int':
                    if c_min > np.iinfo(np.int8).min and c_max < np.iinfo(np.int8).max:
                        optimized[col] = optimized[col].astype(np.int8)
                    elif c_min > np.iinfo(np.int16).min and c_max < np.iinfo(np.int16).max:
                        optimized[col] = optimized[col].astype(np.int16)
                    elif c_min > np.iinfo(np.int32).min and c_max < np.iinfo(np.int32).max:
                        optimized[col] = optimized[col].astype(np.int32)

                # Optimize floats
                elif str(col_type)[:5] == 'float':
                    if c_min > np.finfo(np.float16).min and c_max < np.finfo(np.float16).max:
                        optimized[col] = optimized[col].astype(np.float16)
                    elif c_min > np.finfo(np.float32).min and c_max < np.finfo(np.float32).max:
                        optimized[col] = optimized[col].astype(np.float32)

            # Optimize strings
            else:
                # Convert to category if low cardinality
                num_unique = optimized[col].nunique()
                num_total = len(optimized[col])
                if num_unique / num_total < 0.5:
                    optimized[col] = optimized[col].astype('category')

        return optimized

    def create_generator(self, file_path: str, chunk_size: int = 10000):
        # Create memory-efficient generator for large files

        def data_generator():
            with pd.read_csv(file_path, chunksize=chunk_size) as reader:
                for chunk in reader:
                    # Optimize chunk
                    chunk = self.optimize_dtypes(chunk)
                    yield chunk

        return data_generator()

    def monitor_memory(self) -> Dict[str, float]:
        # Monitor current memory usage

        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()

        return {
            'rss_mb': memory_info.rss / 1024 / 1024,
            'vms_mb': memory_info.vms / 1024 / 1024,
            'percent': process.memory_percent(),
            'available_mb': psutil.virtual_memory().available / 1024 / 1024
        }

    def garbage_collect_aggressive(self):
        # Aggressive garbage collection
        import gc

        # Collect all generations
        gc.collect(2)

        # Compact memory if possible
        if hasattr(gc, 'compact'):
            gc.compact()

# Part 5: Query Optimization


class QueryOptimizer:
    # Optimize query performance

    def __init__(self):
        self.query_cache = OptimizedCache(CacheStrategy.LRU, max_size=500)
        self.execution_plans = {}
        self.statistics = {}

    def optimize_sql_query(self, query: str, schema_info: Dict[str, Any]) -> str:
        # Optimize SQL query based on schema information

        optimized = query

        # Add index hints if available
        if 'indexes' in schema_info:
            for index in schema_info['indexes']:
                if index['column'] in query:
                    # Add index hint (syntax varies by database)
                    hint = f"/*+ INDEX({index['table']} {index['name']}) */"
                    optimized = hint + " " + optimized

        # Rewrite subqueries as joins
        if 'IN (SELECT' in optimized.upper():
            # Convert IN subquery to JOIN
            optimized = self._rewrite_in_subquery(optimized)

        # Add LIMIT if not present for safety
        if 'LIMIT' not in optimized.upper():
            optimized += " LIMIT 10000"

        return optimized

    def _rewrite_in_subquery(self, query: str) -> str:
        # Rewrite IN subquery as JOIN
        # Simplified example - real implementation would use SQL parser

        # Pattern: WHERE column IN (SELECT column FROM table)
        # Rewrite: JOIN table ON column = column

        import re
        pattern = r'WHERE\s+(\w+)\s+IN\s*\(\s*SELECT\s+(\w+)\s+FROM\s+(\w+)\s*\)'
        replacement = r'JOIN \3 ON \1 = \3.\2'

        return re.sub(pattern, replacement, query, flags=re.IGNORECASE)

    def create_execution_plan(self, query: str, data_sources: List[str]) -> Dict[str, Any]:
        # Create query execution plan

        plan = {
            'query': query,
            'steps': [],
            'estimated_cost': 0,
            'parallelizable': False
        }

        # Parse query to identify operations
        query_lower = query.lower()

        # Identify operations
        if 'join' in query_lower:
            plan['steps'].append({
                'operation': 'join',
                'type': 'hash_join' if len(data_sources) > 2 else 'nested_loop',
                'cost': len(data_sources) * 100
            })
            plan['parallelizable'] = True

        if 'group by' in query_lower:
            plan['steps'].append({
                'operation': 'aggregate',
                'type': 'hash_aggregate',
                'cost': 50
            })
            plan['parallelizable'] = True

        if 'order by' in query_lower:
            plan['steps'].append({
                'operation': 'sort',
                'type': 'quicksort',
                'cost': 30
            })

        # Calculate total cost
        plan['estimated_cost'] = sum(step['cost'] for step in plan['steps'])

        return plan

    def batch_queries(self, queries: List[str], max_batch_size: int = 10) -> List[List[str]]:
        # Batch queries for efficient execution

        batches = []
        current_batch = []
        current_cost = 0

        for query in queries:
            # Estimate query cost (simplified)
            query_cost = len(query) / 100  # Rough estimate

            if len(current_batch) >= max_batch_size or current_cost + query_cost > 10:
                batches.append(current_batch)
                current_batch = [query]
                current_cost = query_cost
            else:
                current_batch.append(query)
                current_cost += query_cost

        if current_batch:
            batches.append(current_batch)

        return batches

    def cache_query_result(self, query: str, result: Any):
        # Cache query result
        key = hashlib.md5(query.encode()).hexdigest()
        self.query_cache.put(key, result)

    def get_cached_result(self, query: str) -> Optional[Any]:
        # Get cached query result
        key = hashlib.md5(query.encode()).hexdigest()
        return self.query_cache.get(key)

# Part 6: LLM API Optimization


class LLMOptimizer:
    # Optimize LLM API usage for performance

    def __init__(self, max_batch_size: int = 20, max_tokens: int = 4096):
        self.max_batch_size = max_batch_size
        self.max_tokens = max_tokens
        self.request_queue = deque()
        self.response_cache = OptimizedCache(CacheStrategy.LRU, max_size=1000)
        self.rate_limiter = self.RateLimiter(requests_per_minute=60)

    class RateLimiter:
        # Rate limiting for API calls

        def __init__(self, requests_per_minute: int):
            self.requests_per_minute = requests_per_minute
            self.min_interval = 60.0 / requests_per_minute
            self.last_request_time = 0
            self.lock = threading.Lock()

        def wait_if_needed(self):
            # Wait if rate limit would be exceeded
            with self.lock:
                current_time = time.time()
                time_since_last = current_time - self.last_request_time

                if time_since_last < self.min_interval:
                    sleep_time = self.min_interval - time_since_last
                    time.sleep(sleep_time)

                self.last_request_time = time.time()

    def batch_prompts(self, prompts: List[str]) -> List[List[str]]:
        # Batch prompts for efficient API usage

        batches = []
        current_batch = []
        current_tokens = 0

        for prompt in prompts:
            # Estimate tokens (rough: 1 token = 4 chars)
            prompt_tokens = len(prompt) / 4

            if len(current_batch) >= self.max_batch_size or \
               current_tokens + prompt_tokens > self.max_tokens:
                batches.append(current_batch)
                current_batch = [prompt]
                current_tokens = prompt_tokens
            else:
                current_batch.append(prompt)
                current_tokens += prompt_tokens

        if current_batch:
            batches.append(current_batch)

        return batches

    def deduplicate_prompts(self, prompts: List[str]) -> Tuple[List[str], Dict[str, List[int]]]:
        # Deduplicate similar prompts

        unique_prompts = []
        prompt_indices = {}

        for i, prompt in enumerate(prompts):
            # Normalize prompt for comparison
            normalized = prompt.strip().lower()

            if normalized not in prompt_indices:
                unique_prompts.append(prompt)
                prompt_indices[normalized] = []

            prompt_indices[normalized].append(i)

        return unique_prompts, prompt_indices

    def cached_api_call(self, prompt: str) -> Optional[str]:
        # Check cache before making API call

        cache_key = hashlib.md5(prompt.encode()).hexdigest()
        cached_response = self.response_cache.get(cache_key)

        if cached_response:
            return cached_response

        # Rate limit
        self.rate_limiter.wait_if_needed()

        # Make API call (simulated)
        response = self._make_api_call(prompt)

        # Cache response
        if response:
            self.response_cache.put(cache_key, response)

        return response

    def _make_api_call(self, prompt: str) -> str:
        # Simulate API call
        time.sleep(0.1)  # Simulate network latency
        return f"Response for: {prompt[:50]}..."

    def parallel_api_calls(self, prompts: List[str], max_workers: int = 5) -> List[str]:
        # Make parallel API calls with rate limiting

        results = [None] * len(prompts)

        def process_prompt(args):
            index, prompt = args
            result = self.cached_api_call(prompt)
            return index, result

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all prompts
            futures = []
            for i, prompt in enumerate(prompts):
                future = executor.submit(process_prompt, (i, prompt))
                futures.append(future)

            # Collect results
            for future in futures:
                index, result = future.result()
                results[index] = result

        return results

# Part 7: Real-world Performance Scenarios


class PerformanceScenarios:
    # Real-world performance optimization scenarios

    @staticmethod
    def small_batch_low_latency() -> Dict[str, Any]:
        # Optimize for small batches with low latency requirements
        return {
            'name': 'Small Batch Low Latency',
            'characteristics': {
                'batch_size': 10,
                'latency_target_ms': 100,
                'throughput_target': 100,  # records/second
                'memory_limit_mb': 500
            },
            'optimizations': {
                'caching': 'aggressive_memory_cache',
                'parallelism': 'thread_pool_size_2',
                'batching': 'disabled',
                'memory': 'keep_all_in_memory'
            }
        }

    @staticmethod
    def large_batch_throughput() -> Dict[str, Any]:
        # Optimize for large batch processing
        return {
            'name': 'Large Batch Throughput',
            'characteristics': {
                'batch_size': 10000,
                'latency_target_ms': 60000,  # 1 minute
                'throughput_target': 10000,  # records/second
                'memory_limit_mb': 3500
            },
            'optimizations': {
                'caching': 'disk_cache_with_memory_l1',
                'parallelism': 'process_pool_cpu_count',
                'batching': 'optimal_batch_size',
                'memory': 'chunk_processing'
            }
        }

    @staticmethod
    def streaming_continuous() -> Dict[str, Any]:
        # Optimize for continuous streaming
        return {
            'name': 'Streaming Continuous',
            'characteristics': {
                'batch_size': 1,
                'latency_target_ms': 10,
                'throughput_target': 1000,
                'memory_limit_mb': 1000
            },
            'optimizations': {
                'caching': 'ttl_cache_60s',
                'parallelism': 'async_io',
                'batching': 'micro_batching',
                'memory': 'circular_buffer'
            }
        }

    @staticmethod
    def memory_constrained() -> Dict[str, Any]:
        # Optimize for memory-constrained environment
        return {
            'name': 'Memory Constrained',
            'characteristics': {
                'batch_size': 100,
                'latency_target_ms': 1000,
                'throughput_target': 100,
                'memory_limit_mb': 256
            },
            'optimizations': {
                'caching': 'lfu_cache_small',
                'parallelism': 'sequential_processing',
                'batching': 'small_batches',
                'memory': 'aggressive_gc'
            }
        }

# Part 8: Performance Monitoring


class PerformanceMonitor:
    # Real-time performance monitoring

    def __init__(self):
        self.metrics_history = deque(maxlen=1000)
        self.alerts = []
        self.thresholds = {
            'latency_ms': 1000,
            'memory_mb': 3500,
            'cpu_percent': 90,
            'error_rate': 0.01
        }

    def record_metric(self, metric_type: str, value: float, timestamp: datetime = None):
        # Record a performance metric

        if timestamp is None:
            timestamp = datetime.now()

        metric = {
            'type': metric_type,
            'value': value,
            'timestamp': timestamp
        }

        self.metrics_history.append(metric)

        # Check thresholds
        if metric_type in self.thresholds:
            if value > self.thresholds[metric_type]:
                self.alerts.append({
                    'type': metric_type,
                    'value': value,
                    'threshold': self.thresholds[metric_type],
                    'timestamp': timestamp,
                    'severity': 'high' if value > self.thresholds[metric_type] * 1.5 else 'medium'
                })

    def get_performance_summary(self, window_minutes: int = 5) -> Dict[str, Any]:
        # Get performance summary for time window

        cutoff_time = datetime.now() - timedelta(minutes=window_minutes)
        recent_metrics = [m for m in self.metrics_history
                          if m['timestamp'] > cutoff_time]

        if not recent_metrics:
            return {}

        # Group by metric type
        by_type = {}
        for metric in recent_metrics:
            if metric['type'] not in by_type:
                by_type[metric['type']] = []
            by_type[metric['type']].append(metric['value'])

        # Calculate statistics
        summary = {}
        for metric_type, values in by_type.items():
            summary[metric_type] = {
                'avg': sum(values) / len(values),
                'min': min(values),
                'max': max(values),
                'p50': np.percentile(values, 50),
                'p95': np.percentile(values, 95),
                'p99': np.percentile(values, 99)
            }

        return summary

    def get_optimization_recommendations(self) -> List[str]:
        # Generate optimization recommendations based on metrics

        recommendations = []
        summary = self.get_performance_summary()

        if not summary:
            return recommendations

        # Check latency
        if 'latency_ms' in summary:
            if summary['latency_ms']['p95'] > 1000:
                recommendations.append(
                    "High latency detected - consider caching or parallelization")

        # Check memory
        if 'memory_mb' in summary:
            if summary['memory_mb']['avg'] > 3000:
                recommendations.append(
                    "High memory usage - implement chunking or streaming")

        # Check CPU
        if 'cpu_percent' in summary:
            if summary['cpu_percent']['avg'] > 80:
                recommendations.append(
                    "High CPU usage - optimize algorithms or add parallelization")

        # Check cache performance
        if 'cache_hit_rate' in summary:
            if summary['cache_hit_rate']['avg'] < 0.5:
                recommendations.append(
                    "Low cache hit rate - review cache key generation")

        return recommendations

# Example usage functions


def example_profiling():
    print("\n" + "="*60)
    print("EXAMPLE 1: Performance Profiling")
    print("="*60)

    profiler = PerformanceProfiler()

    # Profile different operations
    def io_operation():
        time.sleep(0.1)  # Simulate IO
        return "io_result"

    def cpu_operation():
        # Simulate CPU-intensive work
        result = sum(i**2 for i in range(10000))
        return result

    # Profile operations
    profiler.profile_operation("io_task", io_operation, record_count=100)
    profiler.profile_operation("cpu_task", cpu_operation, record_count=1000)

    # Generate report
    report = profiler.generate_performance_report()

    print("Performance Report:")
    print(f"  Total time: {report['total_time']:.2f}s")
    print("\nOperation breakdown:")
    for op, stats in report['operation_breakdown'].items():
        print(
            f"  {op}: {stats['percentage']:.1f}% ({stats['avg_time']:.3f}s avg)")

    print("\nBottlenecks identified:")
    for bottleneck_type, operations in report['bottlenecks'].items():
        if operations:
            print(f"  {bottleneck_type}: {operations}")


def example_caching():
    print("\n" + "="*60)
    print("EXAMPLE 2: Cache Performance")
    print("="*60)

    # Test different cache strategies
    strategies = [CacheStrategy.LRU, CacheStrategy.LFU, CacheStrategy.TTL]

    for strategy in strategies:
        cache = OptimizedCache(strategy, max_size=100, ttl_seconds=60)

        # Simulate cache usage
        for i in range(500):
            key = f"key_{i % 150}"  # Some keys repeat

            if i % 3 == 0:  # 33% of requests are for existing keys
                key = f"key_{i % 50}"

            value = cache.get(key)
            if value is None:
                cache.put(key, f"value_{i}")

        print(f"\n{strategy.value} Cache:")
        print(f"  Hit rate: {cache.get_hit_rate():.2%}")
        print(f"  Cache size: {len(cache.cache)}")


def example_parallel_processing():
    print("\n" + "="*60)
    print("EXAMPLE 3: Parallel Processing")
    print("="*60)

    processor = ParallelProcessor(max_workers=4)

    # Test data
    items = list(range(1000))

    # IO-bound operation
    def io_task(x):
        time.sleep(0.001)  # Simulate IO
        return x * 2

    # CPU-bound operation
    def cpu_task(x):
        return sum(i**2 for i in range(x, x+100))

    # Test IO-bound parallelization
    start = time.time()
    results_io = processor.process_io_bound(io_task, items)
    io_time = time.time() - start

    # Test CPU-bound parallelization
    start = time.time()
    results_cpu = processor.process_cpu_bound(
        cpu_task, items[:100])  # Fewer items for CPU
    cpu_time = time.time() - start

    print(f"IO-bound processing:")
    print(f"  Items: {len(items)}")
    print(f"  Time: {io_time:.2f}s")
    print(f"  Throughput: {len(items)/io_time:.0f} items/s")

    print(f"\nCPU-bound processing:")
    print(f"  Items: 100")
    print(f"  Time: {cpu_time:.2f}s")
    print(f"  Throughput: {100/cpu_time:.0f} items/s")


def example_memory_optimization():
    print("\n" + "="*60)
    print("EXAMPLE 4: Memory Optimization")
    print("="*60)

    optimizer = MemoryOptimizer()

    # Create sample DataFrame
    data = {
        'id': range(10000),
        'value': np.random.randn(10000),
        'category': ['A', 'B', 'C', 'D'] * 2500,
        'large_int': np.random.randint(0, 1000000, 10000)
    }
    df = pd.DataFrame(data)

    # Memory before optimization
    memory_before = df.memory_usage(deep=True).sum() / 1024 / 1024

    # Optimize dtypes
    df_optimized = optimizer.optimize_dtypes(df)

    # Memory after optimization
    memory_after = df_optimized.memory_usage(deep=True).sum() / 1024 / 1024

    print(f"Memory optimization:")
    print(f"  Before: {memory_before:.2f} MB")
    print(f"  After: {memory_after:.2f} MB")
    print(f"  Reduction: {(1 - memory_after/memory_before)*100:.1f}%")

    # Test chunking
    chunks = optimizer.chunk_dataframe(df, target_memory_mb=1)
    print(f"\nDataFrame chunking:")
    print(f"  Original rows: {len(df)}")
    print(f"  Number of chunks: {len(chunks)}")
    print(f"  Rows per chunk: {len(chunks[0]) if chunks else 0}")


def example_query_optimization():
    print("\n" + "="*60)
    print("EXAMPLE 5: Query Optimization")
    print("="*60)

    optimizer = QueryOptimizer()

    # Test query optimization
    query = """
    SELECT customer_id, COUNT(*) 
    FROM orders 
    WHERE status IN (SELECT status FROM active_statuses)
    GROUP BY customer_id
    """

    schema_info = {
        'indexes': [
            {'table': 'orders', 'column': 'customer_id', 'name': 'idx_customer'},
            {'table': 'orders', 'column': 'status', 'name': 'idx_status'}
        ]
    }

    optimized = optimizer.optimize_sql_query(query, schema_info)

    print("Original query:")
    print(query)
    print("\nOptimized query:")
    print(optimized)

    # Test query batching
    queries = [f"SELECT * FROM table_{i}" for i in range(25)]
    batches = optimizer.batch_queries(queries, max_batch_size=10)

    print(f"\nQuery batching:")
    print(f"  Total queries: {len(queries)}")
    print(f"  Batches created: {len(batches)}")
    print(f"  Queries per batch: {[len(b) for b in batches]}")


def example_llm_optimization():
    print("\n" + "="*60)
    print("EXAMPLE 6: LLM API Optimization")
    print("="*60)

    llm_optimizer = LLMOptimizer()

    # Test prompt batching
    prompts = [f"Analyze field: field_{i}" for i in range(100)]

    batches = llm_optimizer.batch_prompts(prompts)
    print(f"Prompt batching:")
    print(f"  Total prompts: {len(prompts)}")
    print(f"  Batches: {len(batches)}")
    print(f"  Prompts per batch: {[len(b) for b in batches]}")

    # Test deduplication
    prompts_with_dupes = prompts + prompts[:20]  # Add duplicates
    unique, indices = llm_optimizer.deduplicate_prompts(prompts_with_dupes)

    print(f"\nPrompt deduplication:")
    print(f"  Original prompts: {len(prompts_with_dupes)}")
    print(f"  Unique prompts: {len(unique)}")
    print(f"  Saved API calls: {len(prompts_with_dupes) - len(unique)}")

    # Test cache performance
    print(
        f"\nCache hit rate: {llm_optimizer.response_cache.get_hit_rate():.2%}")


def example_performance_monitoring():
    print("\n" + "="*60)
    print("EXAMPLE 7: Performance Monitoring")
    print("="*60)

    monitor = PerformanceMonitor()

    # Simulate performance metrics
    import random

    for i in range(100):
        monitor.record_metric('latency_ms', random.uniform(100, 2000))
        monitor.record_metric('memory_mb', random.uniform(2000, 4000))
        monitor.record_metric('cpu_percent', random.uniform(50, 95))
        monitor.record_metric('cache_hit_rate', random.uniform(0.3, 0.9))

    # Get summary
    summary = monitor.get_performance_summary(window_minutes=10)

    print("Performance Summary:")
    for metric_type, stats in summary.items():
        print(f"\n{metric_type}:")
        print(f"  Average: {stats['avg']:.2f}")
        print(f"  P95: {stats['p95']:.2f}")
        print(f"  P99: {stats['p99']:.2f}")

    # Get recommendations
    recommendations = monitor.get_optimization_recommendations()
    if recommendations:
        print("\nOptimization Recommendations:")
        for rec in recommendations:
            print(f"  - {rec}")

    # Check alerts
    if monitor.alerts:
        print(f"\nAlerts: {len(monitor.alerts)}")
        for alert in monitor.alerts[:3]:
            print(
                f"  {alert['type']}: {alert['value']:.2f} > {alert['threshold']}")

# Main execution


def main():
    print("="*60)
    print("MODULE 11: COMPREHENSIVE PERFORMANCE OPTIMIZATION")
    print("="*60)
    print("\nDetailed performance optimization framework")

    # Run examples
    example_profiling()
    example_caching()
    example_parallel_processing()
    example_memory_optimization()
    example_query_optimization()
    example_llm_optimization()
    example_performance_monitoring()

    print("\n" + "="*60)
    print("KEY PERFORMANCE INSIGHTS")
    print("="*60)

    print("\nPerformance bottlenecks by frequency:")
    print("1. LLM API calls (network-bound)")
    print("2. Data loading (IO-bound)")
    print("3. Pattern matching (CPU-bound)")
    print("4. Large schema processing (memory-bound)")

    print("\nOptimization priorities:")
    print("1. Implement multi-level caching (L1 memory, L2 disk)")
    print("2. Batch LLM API calls (20x improvement)")
    print("3. Use parallel processing for CPU-bound tasks")
    print("4. Chunk large datasets for memory efficiency")
    print("5. Optimize data types (30-50% memory reduction)")

    print("\nPerformance targets by scenario:")
    print("- Real-time: <100ms latency, use memory cache")
    print("- Batch: Maximize throughput, use all CPU cores")
    print("- Streaming: Consistent latency, use circular buffers")
    print("- Memory-constrained: <256MB, use generators")

    print("\nScaling characteristics:")
    print("- Linear scaling with parallel processing up to CPU count")
    print("- Cache hit rates typically 60-80% after warm-up")
    print("- Memory usage grows with unique schemas, not records")
    print("- Network latency dominates for small batches")


if __name__ == "__main__":
    main()
