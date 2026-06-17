#!/usr/bin/env python3
"""
Alternating Priority Flow Control Test

Runs concurrent workers split 50/50 between high and low priority requests
to demonstrate flow control queue behavior under load.
"""

import argparse
import asyncio
import csv
import json
import logging
import os
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Any
import re
import aiohttp
import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class RequestMetrics:
    """Metrics for a single request"""
    request_id: str
    request_type: str
    prompt: str
    priority: str
    start_time: float = 0.0
    end_time: Optional[float] = None
    ttft: Optional[float] = None
    e2e_latency: Optional[float] = None
    tokens_received: int = 0
    token_timestamps: List[float] = None
    success: bool = False
    error: Optional[str] = None
    response_content: str = ""
    response_headers: Dict[str, str] = None

    def __post_init__(self):
        if self.token_timestamps is None:
            self.token_timestamps = []
        if self.response_headers is None:
            self.response_headers = {}

    def calculate_itl(self) -> Dict[str, float]:
        """Calculate inter-token latency statistics"""
        if len(self.token_timestamps) < 2:
            return {}

        import statistics
        itls = [
            self.token_timestamps[i] - self.token_timestamps[i-1]
            for i in range(1, len(self.token_timestamps))
        ]

        return {
            'mean': statistics.mean(itls),
            'median': statistics.median(itls),
            'min': min(itls),
            'max': max(itls),
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        d = asdict(self)
        d['itl_stats'] = self.calculate_itl()
        d.pop('token_timestamps')
        return d


class LLMClient:
    """Client for interacting with LLM endpoints"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.url: Optional[str] = None
        self.model_name: Optional[str] = None
        self.token_high: Optional[str] = None
        self.token_low: Optional[str] = None
        self.session: Optional[aiohttp.ClientSession] = None

    async def initialize(self):
        """Initialize the client"""
        logger.info("Initializing LLM client...")

        llmis_ns = self.config['endpoint']['llmis_namespace']
        llmis_name = self.config['endpoint']['llmis_name']

        # Check if oc is available and logged in
        try:
            subprocess.run(['oc', 'whoami'], check=True, capture_output=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            raise RuntimeError("Not logged into OpenShift. Please run 'oc login' first.")

        logger.info(f"Using LLMInferenceService: {llmis_name}")

        # Get URL
        result = subprocess.run(
            ['oc', 'get', f'LLMinferenceservice/{llmis_name}', '-n', llmis_ns,
             '-ojsonpath={.status.addresses[0].url}'],
            check=True,
            capture_output=True,
            text=True
        )
        self.url = result.stdout.strip()

        # Get model name
        result = subprocess.run(
            ['oc', 'get', f'LLMinferenceservice/{llmis_name}', '-n', llmis_ns,
             '-ojsonpath={.spec.model.name}'],
            check=True,
            capture_output=True,
            text=True
        )
        self.model_name = result.stdout.strip()

        logger.info(f"Model: {self.model_name}")
        logger.info(f"URL: {self.url}")

        # Generate tokens
        token_duration = self.config['endpoint'].get('token_duration', '30m')
        sa_ns_high = self.config['endpoint']['sa_namespace_high']
        sa_ns_low = self.config['endpoint']['sa_namespace_low']

        result = subprocess.run(
            ['oc', 'create', 'token', '-n', sa_ns_high, 'llm-inferencer',
             f'--duration={token_duration}'],
            check=True,
            capture_output=True,
            text=True
        )
        self.token_high = result.stdout.strip()

        result = subprocess.run(
            ['oc', 'create', 'token', '-n', sa_ns_low, 'llm-inferencer',
             f'--duration={token_duration}'],
            check=True,
            capture_output=True,
            text=True
        )
        self.token_low = result.stdout.strip()

        logger.info(f"Generated tokens for high-priority ({sa_ns_high}) and low-priority ({sa_ns_low})")

        # Create aiohttp session
        timeout = aiohttp.ClientTimeout(total=self.config['request'].get('timeout', 120))
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self):
        """Close the client session"""
        if self.session:
            await self.session.close()

    async def send_request(self, metric: RequestMetrics, priority: str = 'high',
                          max_tokens_override: Optional[int] = None) -> RequestMetrics:
        """Send a single inference request and collect metrics"""
        metric.start_time = time.time()
        metric.priority = priority

        # Select token based on priority
        token = self.token_high if priority == 'high' else self.token_low

        # Use override max_tokens if provided (for baseline requests)
        max_tokens = max_tokens_override or self.config['request'].get('max_tokens', 256)

        request_body = {
            'model': self.model_name,
            'messages': [{'role': 'user', 'content': metric.prompt}],
            'temperature': self.config['request'].get('temperature', 0.7),
            'max_tokens': max_tokens,
            'top_p': self.config['request'].get('top_p', 0.9),
            'stream': self.config['request'].get('stream', True),
        }

        headers = {
            'Content-Type': 'application/json',
        }

        if token:
            headers['Authorization'] = f'Bearer {token}'

        try:
            async with self.session.post(
                f'{self.url}/v1/chat/completions',
                json=request_body,
                headers=headers
            ) as response:
                # Capture response headers
                metric.response_headers = dict(response.headers)

                if response.status != 200:
                    error_text = await response.text()
                    metric.error = f"HTTP {response.status}: {error_text}"
                    metric.success = False
                    metric.end_time = time.time()
                    return metric

                if request_body['stream']:
                    # Stream response
                    first_token = True
                    content_parts = []
                    async for line in response.content:
                        line = line.decode('utf-8').strip()
                        if not line or line == 'data: [DONE]':
                            continue

                        if line.startswith('data: '):
                            try:
                                data = json.loads(line[6:])
                                if 'choices' in data and len(data['choices']) > 0:
                                    delta = data['choices'][0].get('delta', {})
                                    if 'content' in delta:
                                        current_time = time.time()
                                        if first_token:
                                            metric.ttft = current_time - metric.start_time
                                            first_token = False
                                        metric.token_timestamps.append(current_time)
                                        metric.tokens_received += 1
                                        content_parts.append(delta['content'])
                            except json.JSONDecodeError:
                                continue
                    metric.response_content = ''.join(content_parts)
                else:
                    data = await response.json()
                    metric.ttft = time.time() - metric.start_time
                    if 'choices' in data and len(data['choices']) > 0:
                        content = data['choices'][0].get('message', {}).get('content', '')
                        metric.response_content = content
                        metric.tokens_received = len(content.split())

                metric.end_time = time.time()
                metric.e2e_latency = metric.end_time - metric.start_time
                metric.success = True

        except asyncio.TimeoutError:
            metric.error = 'Request timeout'
            metric.success = False
            metric.end_time = time.time()
        except Exception as e:
            metric.error = str(e)
            metric.success = False
            metric.end_time = time.time()

        return metric


def extract_router_logs(config: Dict[str, Any], request_id: str, start_time: float,
                       end_time: float) -> Optional[str]:
    """Extract router pod logs for a specific request ID"""
    try:
        router_ns = config['endpoint']['router_pod_namespace']
        router_pattern = config['endpoint']['router_pod_pattern']

        # Find router pod
        result = subprocess.run(
            ['oc', 'get', 'pods', '-n', router_ns, '-o', 'name'],
            check=True,
            capture_output=True,
            text=True
        )

        pods = result.stdout.strip().split('')
        router_pod = None
        for pod in pods:
            if re.search(router_pattern, pod):
                router_pod = pod.replace('pod/', '')
                break

        if not router_pod:
            logger.warning(f"Could not find router pod matching pattern: {router_pattern}")
            return None

        logger.info(f"Extracting logs from router pod: {router_pod}")

        # Calculate time window
        context_seconds = config['trace'].get('log_context_seconds', 10)
        since_time = datetime.fromtimestamp(start_time - context_seconds)

        # Extract logs since start time
        result = subprocess.run(
            ['oc', 'logs', router_pod, '-n', router_ns,
             f'--since-time={since_time.isoformat()}Z'],
            check=True,
            capture_output=True,
            text=True,
            timeout=30
        )

        all_logs = result.stdout

        # Filter logs containing the request ID
        matching_lines = []
        for line in all_logs.split(''):
            if request_id in line:
                matching_lines.append(line)

        if matching_lines:
            logger.info(f"Found {len(matching_lines)} log lines for request {request_id}")
            return '\n'.join(matching_lines)
        else:
            logger.warning(f"No logs found for request {request_id}")
            return None

    except subprocess.TimeoutExpired:
        logger.error(f"Timeout extracting logs for request {request_id}")
        return None
    except Exception as e:
        logger.error(f"Error extracting logs for request {request_id}: {e}")
        return None


def generate_baseline_prompts(config: Dict[str, Any]) -> List[str]:
    """Generate diverse, long prompts for baseline load to saturate KV cache"""
    baseline_config = config.get('baseline', {})

    if not baseline_config.get('use_diverse_prompts', False):
        # Use simple prompts
        return [
            "Explain microservices architecture.",
            "Write about Python async programming.",
            "Describe REST API security best practices.",
            "Compare SQL and NoSQL databases.",
            "Explain Java garbage collection.",
        ]

    num_prompts = baseline_config.get('num_diverse_prompts', 50)
    use_long = baseline_config.get('use_long_prompts', False)
    target_tokens = baseline_config.get('baseline_prompt_tokens', 4000)

    logger.info(f"Generating {num_prompts} diverse baseline prompts (long={use_long})...")

    # Generate diverse topics and tasks
    topics = [
        "microservices", "containers", "Kubernetes", "Docker", "cloud computing", "serverless",
        "machine learning", "deep learning", "neural networks", "transformers", "LLMs",
        "databases", "SQL", "NoSQL", "caching", "message queues", "event streaming",
        "web frameworks", "REST APIs", "GraphQL", "gRPC", "WebSockets",
        "security", "authentication", "authorization", "encryption", "OAuth",
        "testing", "CI/CD", "DevOps", "monitoring", "observability",
        "Python", "Go", "Java", "Rust", "JavaScript", "TypeScript",
        "data structures", "algorithms", "system design", "distributed systems",
    ]

    tasks = [
        "Explain in detail how {} works",
        "Write a comprehensive guide to {}",
        "Describe the architecture and implementation of {}",
        "What are the best practices for using {}",
        "Compare different approaches to implementing {}",
        "Analyze the performance characteristics of {}",
        "Discuss the trade-offs when choosing {}",
        "Provide a deep-dive tutorial on {}",
    ]

    prompts = []
    for i in range(num_prompts):
        topic = topics[i % len(topics)]
        task = tasks[(i // len(topics)) % len(tasks)]
        base_prompt = task.format(topic)

        if use_long:
            # Add context to make prompts longer and more diverse
            # Target: ~target_tokens tokens (roughly 4 chars per token)
            contexts = [
                "for a production system at scale",
                "in the context of enterprise applications",
                "considering modern cloud-native architectures",
                "with a focus on performance and reliability",
                "from both theoretical and practical perspectives",
                "including real-world examples and case studies",
                "covering common pitfalls and how to avoid them",
                "with emphasis on security and best practices",
            ]
            context = contexts[i % len(contexts)]

            extras = [
                "Include detailed code examples with explanations, architecture diagrams, and comprehensive step-by-step implementation guides.",
                "Discuss the complete historical evolution, key milestones, current state of the art, and emerging trends in the field.",
                "Compare exhaustively with all major alternative approaches, analyzing trade-offs, use cases, and decision criteria.",
                "Provide extensive performance benchmarks, profiling results, optimization techniques, and tuning recommendations.",
                "Cover comprehensive integration patterns, ecosystem tools, library choices, and interoperability considerations.",
                "Address scalability challenges, reliability patterns, operational best practices, and production deployment strategies.",
            ]
            extra = extras[i % len(extras)]

            # Add more filler to reach target token count (roughly 4 chars = 1 token)
            target_chars = target_tokens * 4
            prompt = f"{base_prompt} {context}. {extra}"

            # If still too short, add more detail requirements
            if len(prompt) < target_chars:
                padding = [
                    " Provide concrete examples from real-world production systems.",
                    " Include specific metrics, benchmarks, and quantitative analysis.",
                    " Discuss implementation details at the code and architecture level.",
                    " Cover edge cases, failure modes, and recovery strategies.",
                    " Explain monitoring, observability, and debugging approaches.",
                    " Address security considerations and compliance requirements.",
                    " Discuss testing strategies, quality assurance, and validation.",
                    " Cover documentation, team collaboration, and knowledge sharing aspects.",
                ]
                for p in padding:
                    if len(prompt) < target_chars:
                        prompt += p
        else:
            prompt = base_prompt

        prompts.append(prompt)

    logger.info(f"Generated {len(prompts)} baseline prompts")
    return prompts


def collect_cluster_metrics(config: Dict[str, Any]) -> Dict[str, Any]:
    """Collect relevant OpenShift cluster metrics"""
    metrics = {}

    try:
        llmis_ns = config['endpoint']['llmis_namespace']
        llmis_name = config['endpoint']['llmis_name']

        # Get pod status
        result = subprocess.run(
            ['oc', 'get', 'pods', '-n', llmis_ns, '-l', f'serving.kserve.io/inferenceservice={llmis_name}',
             '-o', 'json'],
            check=True,
            capture_output=True,
            text=True
        )
        pod_data = json.loads(result.stdout)
        metrics['pods'] = {
            'total': len(pod_data.get('items', [])),
            'ready': sum(1 for pod in pod_data.get('items', [])
                        if pod.get('status', {}).get('phase') == 'Running')
        }

        # Get resource usage if metrics-server is available
        try:
            result = subprocess.run(
                ['oc', 'adm', 'top', 'pods', '-n', llmis_ns,
                 '-l', f'serving.kserve.io/inferenceservice={llmis_name}'],
                check=True,
                capture_output=True,
                text=True,
                timeout=10
            )
            metrics['resource_usage'] = result.stdout
        except:
            logger.warning("Could not collect pod resource usage (metrics-server may not be available)")


    except Exception as e:
        logger.error(f"Error collecting cluster metrics: {e}")

    return metrics


def get_flow_control_metrics(config: Dict[str, Any]) -> Dict[str, Any]:
    """Query Prometheus for flow control queue metrics"""
    metrics = {}

    try:
        # Query Prometheus for queue sizes and saturation
        # This is a placeholder - actual implementation would query Prometheus API
        # For now, just return empty dict to avoid blocking
        pass
    except Exception as e:
        logger.debug(f"Error querying flow control metrics: {e}")

    return metrics




async def run_alternating_priority_test(client: LLMClient, prompts: List[str], duration: int,
                                        concurrent_requests: int,
                                        max_tokens_override: Optional[int] = None,
                                        config: Optional[Dict[str, Any]] = None) -> Dict[str, List[RequestMetrics]]:
    """Run test with half workers sending high-priority, half low-priority requests

    Workers are staggered to avoid overwhelming auth service.
    Each worker keeps exactly 1 request in flight at a time.
    Workers 0 to N/2 send high-priority, N/2 to N send low-priority.
    """
    start_time = time.time()
    high_workers = concurrent_requests // 2
    low_workers = concurrent_requests - high_workers

    logger.info(f"Starting alternating priority test: {concurrent_requests} concurrent workers for {duration}s")
    logger.info(f"  High-priority workers: {high_workers} (workers 0-{high_workers-1})")
    logger.info(f"  Low-priority workers: {low_workers} (workers {high_workers}-{concurrent_requests-1})")

    end_time = start_time + duration
    request_counter = 0
    high_priority_metrics = []
    low_priority_metrics = []
    lock = asyncio.Lock()

    # Queue monitoring task
    async def queue_monitor():
        """Periodically check and log queue metrics"""
        if not config or not config.get('test', {}).get('monitor_queue_metrics', False):
            return

        interval = config.get('test', {}).get('queue_check_interval', 10)

        while time.time() < end_time:
            await asyncio.sleep(interval)

            # Query Prometheus for queue metrics
            try:
                metrics = get_flow_control_metrics(config)
                if metrics:
                    logger.info(f"📊 Queue Status (elapsed: {time.time() - start_time:.0f}s):")
                    logger.info(f"  Priority 100 (high) queue: {metrics.get('queue_size_priority_100', 'N/A')}")
                    logger.info(f"  Priority 10 (low) queue: {metrics.get('queue_size_priority_10', 'N/A')}")
                    logger.info(f"  Pool saturation: {metrics.get('pool_saturation', 'N/A')}")
            except Exception as e:
                logger.debug(f"Queue monitoring error: {e}")

    async def worker(worker_id: int):
        nonlocal request_counter

        # Determine priority based on worker ID
        # First half = high priority, second half = low priority
        is_high_priority = worker_id < high_workers
        priority = 'high' if is_high_priority else 'low'
        priority_name = 'HIGH' if is_high_priority else 'LOW'

        # Stagger worker starts to avoid auth service overload
        stagger_delay = config.get('test', {}).get('worker_stagger_delay', 0.100)
        await asyncio.sleep(worker_id * stagger_delay)

        while time.time() < end_time:
            # Get next prompt
            async with lock:
                current_request = request_counter
                request_counter += 1

            prompt = prompts[current_request % len(prompts)]

            metric = RequestMetrics(
                request_id=f"{priority}_{worker_id}_{current_request}",
                request_type=priority_name,
                prompt=prompt,
                priority=priority,
            )

            # Send request and wait for completion
            metric = await client.send_request(metric, priority=priority,
                                              max_tokens_override=max_tokens_override)

            async with lock:
                if is_high_priority:
                    high_priority_metrics.append(metric)
                else:
                    low_priority_metrics.append(metric)

    # Start queue monitoring and all workers
    monitor_task = asyncio.create_task(queue_monitor())
    worker_tasks = [asyncio.create_task(worker(i)) for i in range(concurrent_requests)]

    await asyncio.gather(*worker_tasks, return_exceptions=True)
    monitor_task.cancel()

    total_time = time.time() - start_time
    high_successful = sum(1 for m in high_priority_metrics if m.success)
    low_successful = sum(1 for m in low_priority_metrics if m.success)

    logger.info(f"Test complete after {total_time:.1f}s:")
    logger.info(f"  High-priority: {len(high_priority_metrics)} requests, {high_successful} successful ({high_successful/len(high_priority_metrics)*100:.1f}%)")
    logger.info(f"  Low-priority: {len(low_priority_metrics)} requests, {low_successful} successful ({low_successful/len(low_priority_metrics)*100:.1f}%)")

    return {
        'high': high_priority_metrics,
        'low': low_priority_metrics
    }


async def run_baseline_load(client: LLMClient, prompts: List[str], duration: int,
                            concurrent_requests: int, priority: str = 'low',
                            max_tokens_override: Optional[int] = None) -> List[RequestMetrics]:
    """Run baseline load and collect metrics

    Workers are staggered to avoid overwhelming auth service.
    Each worker keeps exactly 1 request in flight at a time.
    """
    start_time = time.time()
    logger.info(f"Starting baseline load: {concurrent_requests} concurrent workers for {duration}s")

    end_time = start_time + duration
    request_counter = 0
    metrics_list = []
    lock = asyncio.Lock()

    async def baseline_worker(worker_id: int):
        nonlocal request_counter

        # Stagger worker starts to avoid auth service overload
        stagger_delay = config.get('test', {}).get('worker_stagger_delay', 0.100)
        await asyncio.sleep(worker_id * stagger_delay)

        while time.time() < end_time:
            # Get next prompt
            async with lock:
                current_request = request_counter
                request_counter += 1

            prompt = prompts[current_request % len(prompts)]

            metric = RequestMetrics(
                request_id=f"baseline_{worker_id}_{current_request}",
                request_type='baseline',
                prompt=prompt,
                priority=priority,
            )

            # Send request and wait for completion
            # This ensures each worker only has 1 request in flight
            metric = await client.send_request(metric, priority=priority,
                                              max_tokens_override=max_tokens_override)

            async with lock:
                metrics_list.append(metric)

    # Start concurrent workers
    tasks = [asyncio.create_task(baseline_worker(i)) for i in range(concurrent_requests)]
    await asyncio.gather(*tasks, return_exceptions=True)

    total_time = time.time() - start_time
    successful = sum(1 for m in metrics_list if m.success)
    logger.info(f"Baseline complete: {len(metrics_list)} requests, {successful} successful ({successful/len(metrics_list)*100:.1f}%)")
    return metrics_list


async def run_trace_request(client: LLMClient, config: Dict[str, Any], prompt: str,
                            request_num: int, output_dir: Path) -> Dict[str, Any]:
    """Run a single trace request and collect all diagnostics"""
    request_id = f"trace_{request_num}_{int(time.time())}"

    logger.info(f"{'='*80}")
    logger.info(f"Trace Request #{request_num}: {request_id}")
    logger.info(f"{'='*80}")

    metric = RequestMetrics(
        request_id=request_id,
        request_type='trace',
        prompt=prompt,
        priority='high',
    )

    # Send request
    metric = await client.send_request(metric, priority='high')

    # Extract router logs
    router_logs = None
    if config['trace'].get('extract_router_logs', True) and metric.success:
        router_logs = extract_router_logs(config, request_id, metric.start_time, metric.end_time)

    # Collect diagnostics
    diagnostics = {
        'request_id': request_id,
        'request_num': request_num,
        'timestamp': datetime.fromtimestamp(metric.start_time).isoformat(),
        'metrics': metric.to_dict(),
        'router_logs': router_logs,
        'response_headers': metric.response_headers,
    }

    # Save individual trace file
    if config['output'].get('save_per_request_details', True):
        trace_file = output_dir / f"trace_{request_num:03d}_{request_id}.json"
        with open(trace_file, 'w') as f:
            json.dump(diagnostics, f, indent=2)
        logger.info(f"Saved trace details to {trace_file}")

    # Print summary
    logger.info(f"Request: {request_id}")
    logger.info(f"  Success: {metric.success}")
    if metric.success:
        logger.info(f"  TTFT: {metric.ttft*1000:.2f}ms")
        logger.info(f"  E2E Latency: {metric.e2e_latency*1000:.2f}ms")
        logger.info(f"  Tokens: {metric.tokens_received}")
        if router_logs:
            logger.info(f"  Router logs: {len(router_logs.split(chr(10)))} lines")
    else:
        logger.info(f"  Error: {metric.error}")

    return diagnostics


async def main():
    parser = argparse.ArgumentParser(description='Alternating Priority Flow Control Test')
    parser.add_argument('--config', default='test_config.yaml', help='Config file')
    parser.add_argument('--workers', type=int, help='Override concurrent workers')
    parser.add_argument('--duration', type=int, help='Override test duration (seconds)')
    parser.add_argument('--output-dir', help='Override output directory')

    args = parser.parse_args()

    # Load configuration
    config_path = Path(args.config)
    if not config_path.exists():
            logger.error(f"Configuration file not found: {config_path}")
            sys.exit(1)

    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Apply CLI overrides
    if args.workers:
        config['test']['concurrent_requests'] = args.workers
    if args.duration:
        config['test']['duration'] = args.duration
    if args.output_dir:
        config['output']['output_dir'] = args.output_dir

    # Create output directory
    output_dir = Path(config['output']['output_dir']) / f"flow-control_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    # Initialize client
    client = LLMClient(config)

    try:
        await client.initialize()

        # Generate prompts
        prompt_tokens = config['test'].get('prompt_tokens', 1500)
        prompts = generate_baseline_prompts(config)

        # Save prompts to file
        prompts_file = output_dir / 'prompts.txt'
        with open(prompts_file, 'w') as f:
            for i, prompt in enumerate(prompts):
                f.write(f"=== Prompt {i} ===\n")
                f.write(f"{prompt}\n\n")
            logger.info(f"Saved {len(prompts)} prompts to {prompts_file}")

            # Collect initial cluster metrics
        logger.info("Collecting initial cluster metrics...")
        initial_metrics = collect_cluster_metrics(config)

        # Run alternating priority test
        logger.info(f"{'='*80}")
        logger.info("ALTERNATING PRIORITY TEST")
        logger.info(f"{'='*80}")

        test_duration = config['test'].get('duration', 120)
        concurrent_workers = config['test']['concurrent_requests']
        max_tokens = config['request'].get('max_tokens', 1500)

        results = await run_alternating_priority_test(
            client,
            prompts,
            test_duration,
            concurrent_workers,
            max_tokens_override=max_tokens,
            config=config
        )

        high_priority_metrics = results['high']
        low_priority_metrics = results['low']

        # Collect final cluster metrics
        logger.info(f"Collecting final cluster metrics...")
        final_metrics = collect_cluster_metrics(config)

        # Analyze results
        logger.info(f"{'='*80}")
        logger.info("TEST RESULTS")
        logger.info(f"{'='*80}")

        def analyze_metrics(metrics, name):
            successful = [m for m in metrics if m.success]
            failed = [m for m in metrics if not m.success]

            logger.info(f"{name} Priority Results:")
            logger.info(f"  Total Requests: {len(metrics)}")
            logger.info(f"  Successful: {len(successful)} ({len(successful)/len(metrics)*100:.1f}%)")
            logger.info(f"  Failed: {len(failed)} ({len(failed)/len(metrics)*100:.1f}%)")

            stats_dict = {}
            if successful:
                latencies = [m.e2e_latency for m in successful]
                ttfts = [m.ttft for m in successful]

                # Calculate P95
                p95 = statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else max(latencies)

                stats_dict = {
                    'mean': statistics.mean(latencies),
                    'median': statistics.median(latencies),
                    'p95': p95,
                    'min': min(latencies),
                    'max': max(latencies),
                    'mean_ttft': statistics.mean(ttfts)
                }

                logger.info(f"  Mean E2E Latency: {stats_dict['mean']:.2f}s")
                logger.info(f"  Median E2E Latency: {stats_dict['median']:.2f}s")
                logger.info(f"  P95 E2E Latency: {stats_dict['p95']:.2f}s")
                logger.info(f"  Range: {stats_dict['min']:.2f}s - {stats_dict['max']:.2f}s")
                logger.info(f"  Mean TTFT: {stats_dict['mean_ttft']:.2f}s")

            if failed:
                error_types = {}
                for m in failed:
                    error = m.error or "Unknown"
                    error_types[error] = error_types.get(error, 0) + 1

                logger.info(f"  Failure breakdown:")
                for error, count in sorted(error_types.items(), key=lambda x: -x[1]):
                    logger.info(f"    {error}: {count} ({count/len(failed)*100:.1f}%)")

            return successful, stats_dict

        high_successful, high_stats = analyze_metrics(high_priority_metrics, "High")
        low_successful, low_stats = analyze_metrics(low_priority_metrics, "Low")

        # Compare priorities
        if high_successful and low_successful and high_stats and low_stats:
            high_mean = high_stats['mean']
            low_mean = low_stats['mean']
            high_p95 = high_stats['p95']
            low_p95 = low_stats['p95']

            high_ttft = high_stats['mean_ttft']
            low_ttft = low_stats['mean_ttft']

            mean_improvement = ((low_mean - high_mean) / low_mean * 100)
            p95_improvement = ((low_p95 - high_p95) / low_p95 * 100)
            ttft_improvement = ((low_ttft - high_ttft) / low_ttft * 100)

            # Success rate comparison
            high_success_rate = len(high_successful) / len(high_priority_metrics) * 100
            low_success_rate = len(low_successful) / len(low_priority_metrics) * 100
            success_rate_diff = high_success_rate - low_success_rate

            logger.info(f"{'='*80}")
            logger.info("📊 PRIORITY COMPARISON")
            logger.info(f"{'='*80}")
            print()

            logger.info("📈 Request Volume:")
            logger.info(f"  High-priority: {len(high_priority_metrics)} requests")
            logger.info(f"  Low-priority: {len(low_priority_metrics)} requests")
            print()

            logger.info("✅ Success Rates:")
            logger.info(f"  High-priority: {high_success_rate:.1f}%")
            logger.info(f"  Low-priority: {low_success_rate:.1f}%")
            logger.info(f"  Difference: {success_rate_diff:+.1f}%")
            print()

            logger.info(f"⏱️  End-to-End Latency (successful requests):")
            logger.info(f"  High-priority mean: {high_mean:.2f}s")
            logger.info(f"  Low-priority mean: {low_mean:.2f}s")
            mean_speedup = low_mean / high_mean if high_mean > 0 else 1.0
            if mean_improvement > 1:
                logger.info(f"  Difference: {mean_improvement:+.1f}% ({mean_speedup:.1f}x faster)")
            else:
                logger.info(f"  Difference: {mean_improvement:+.1f}%")
            print()

            logger.info(f"  High-priority P95: {high_p95:.2f}s")
            logger.info(f"  Low-priority P95: {low_p95:.2f}s")
            p95_speedup = low_p95 / high_p95 if high_p95 > 0 else 1.0
            if p95_improvement > 1:
                logger.info(f"  Difference: {p95_improvement:+.1f}% ({p95_speedup:.1f}x faster)")
            else:
                logger.info(f"  Difference: {p95_improvement:+.1f}%")
            print()

            logger.info(f"⚡ Time to First Token (successful requests):")
            logger.info(f"  High-priority mean: {high_ttft:.2f}s")
            logger.info(f"  Low-priority mean: {low_ttft:.2f}s")
            ttft_speedup = low_ttft / high_ttft if high_ttft > 0 else 1.0
            if ttft_improvement > 1:
                logger.info(f"  Difference: {ttft_improvement:+.1f}% ({ttft_speedup:.1f}x faster)")
            else:
                logger.info(f"  Difference: {ttft_improvement:+.1f}%")
            print()

            logger.info(f"📈 Summary:")
            if success_rate_diff > 10:
                logger.info(f"  ✅ High-priority achieved {success_rate_diff:.1f}% higher success rate")
            if p95_improvement > 10:
                logger.info(f"  ✅ High-priority P95 latency {p95_improvement:.1f}% better")
            if ttft_improvement > 10:
                logger.info(f"  ✅ High-priority TTFT {ttft_improvement:.1f}% better")
            if mean_improvement > 5:
                logger.info(f"  ✅ High-priority mean latency {mean_improvement:.1f}% better")

            if not any([success_rate_diff > 10, p95_improvement > 10, ttft_improvement > 10, mean_improvement > 5]):
                logger.info(f"  ⚠️  No significant priority differentiation observed")

        # Save summary
        summary = {
            'timestamp': datetime.now().isoformat(),
            'config': config,
            'high_priority': {
                'total': len(high_priority_metrics),
                'successful': len(high_successful),
                'success_rate': len(high_successful) / len(high_priority_metrics) * 100 if high_priority_metrics else 0,
                'latency_stats': high_stats if high_stats else {},
                'metrics': [m.to_dict() for m in high_priority_metrics]
            },
            'low_priority': {
                'total': len(low_priority_metrics),
                'successful': len(low_successful),
                'success_rate': len(low_successful) / len(low_priority_metrics) * 100 if low_priority_metrics else 0,
                'latency_stats': low_stats if low_stats else {},
                'metrics': [m.to_dict() for m in low_priority_metrics]
            },
            'comparison': {
                'success_rate_difference': (len(high_successful) / len(high_priority_metrics) - len(low_successful) / len(low_priority_metrics)) * 100 if high_priority_metrics and low_priority_metrics else 0,
                'mean_improvement_pct': ((low_stats.get('mean', 0) - high_stats.get('mean', 0)) / low_stats.get('mean', 1) * 100) if high_stats and low_stats and low_stats.get('mean') else 0,
                'p95_improvement_pct': ((low_stats.get('p95', 0) - high_stats.get('p95', 0)) / low_stats.get('p95', 1) * 100) if high_stats and low_stats and low_stats.get('p95') else 0,
                'ttft_improvement_pct': ((low_stats.get('mean_ttft', 0) - high_stats.get('mean_ttft', 0)) / low_stats.get('mean_ttft', 1) * 100) if high_stats and low_stats and low_stats.get('mean_ttft') else 0,
            },
            'cluster_metrics': {
                'initial': initial_metrics,
                'final': final_metrics
            }
        }

        summary_file = output_dir / 'summary.json'
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        logger.info(f"✅ Summary saved to: {summary_file}")

    finally:
        await client.close()


if __name__ == '__main__':
    asyncio.run(main())
