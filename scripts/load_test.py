from __future__ import annotations

import argparse
import math
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx


def percentile(values: list[float], value: float) -> float:
    index = min(len(values) - 1, math.ceil(len(values) * value) - 1)
    return sorted(values)[index]


def execute(url: str, api_key: str) -> tuple[int, float]:
    started = time.perf_counter()
    response = httpx.get(url, headers={"X-API-Key": api_key}, timeout=10)
    return response.status_code, time.perf_counter() - started


def run(base_url: str, requests: int, concurrency: int, max_p95: float) -> None:
    url = f"{base_url}/v1/partners/PRT0001"
    results = []
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(execute, url, "local-dev-key") for _ in range(requests)]
        for future in as_completed(futures):
            results.append(future.result())
    latencies = [latency for _, latency in results]
    errors = sum(status >= 400 for status, _ in results)
    p95 = percentile(latencies, 0.95)
    print(f"requests={requests} concurrency={concurrency} errors={errors} "
          f"p50={statistics.median(latencies):.3f}s p95={p95:.3f}s")
    if errors or p95 > max_p95:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--max-p95", type=float, default=1.0)
    args = parser.parse_args()
    run(args.base_url, args.requests, args.concurrency, args.max_p95)

