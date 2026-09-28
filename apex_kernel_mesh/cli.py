"""CLI for Apex Kernel Mesh."""

from __future__ import annotations

import argparse
import json
import sys

from .benchmark import run_benchmarks, print_report, _build_sample_registry
from .registry import KernelRegistry
from .router import CrossRegistryRouter
from .composer import DAGComposer, PipelineStage
from .executor import PipelineExecutor
from .health import HealthMonitor
from .balancer import LatencyBudgetPartitioner


def cmd_benchmark(args: argparse.Namespace) -> None:
    """Run benchmark suite."""
    results = run_benchmarks(iterations=args.iterations)
    print_report(results)


def cmd_registry(args: argparse.Namespace) -> None:
    """Show registry summary."""
    registry = _build_sample_registry()
    summary = registry.summary()
    print(json.dumps(summary, indent=2))


def cmd_route(args: argparse.Namespace) -> None:
    """Route a query across the registry."""
    registry = _build_sample_registry()
    router = CrossRegistryRouter(registry)
    result = router.route(
        query=args.query,
        token_budget=args.budget,
        strategy=args.strategy,
    )
    print(f"Selected {result.count} capabilities ({result.total_tokens} tokens)")
    print(f"Token reduction: {result.token_reduction_pct:.1f}%")
    print(f"Canonical order:")
    for key in result.canonical_order:
        print(f"  {key}")


def cmd_compose(args: argparse.Namespace) -> None:
    """Compose and schedule a sample pipeline."""
    composer = DAGComposer()
    stages = [
        PipelineStage("s1", "datacenter", "bin_packing", 50.0),
        PipelineStage("s2", "smart-grid", "opf", 80.0, dependencies=("s1",)),
        PipelineStage("s3", "hft", "arbitrage", 30.0, dependencies=("s1",)),
        PipelineStage("s4", "compiler", "parallelism", 60.0, dependencies=("s2", "s3")),
    ]
    dag = composer.compose(stages)
    print(f"Stages: {dag.stage_count}")
    print(f"Makespan: {dag.makespan_us:.1f} µs")
    print(f"Critical path: {dag.critical_path}")
    print(f"Parallelism: {dag.parallelism_factor:.2f}x")
    groups = composer.parallel_groups(dag)
    print(f"Parallel groups: {groups}")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="apex-kernel-mesh",
        description="Local capability discovery, DAG planning, and pipeline simulation prototype",
    )
    sub = parser.add_subparsers(dest="command")

    # benchmark
    bp = sub.add_parser("benchmark", help="Run benchmark suite")
    bp.add_argument("--iterations", "-n", type=int, default=20)

    # registry
    sub.add_parser("registry", help="Show registry summary")

    # route
    rp = sub.add_parser("route", help="Route a query across kernels")
    rp.add_argument("query", help="Natural language query")
    rp.add_argument("--budget", "-b", type=int, default=500)
    rp.add_argument("--strategy", "-s", default="submodular",
                     choices=["submodular", "exact", "domain"])

    # compose
    sub.add_parser("compose", help="Compose a sample pipeline DAG")

    args = parser.parse_args()

    if args.command == "benchmark":
        cmd_benchmark(args)
    elif args.command == "registry":
        cmd_registry(args)
    elif args.command == "route":
        cmd_route(args)
    elif args.command == "compose":
        cmd_compose(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
