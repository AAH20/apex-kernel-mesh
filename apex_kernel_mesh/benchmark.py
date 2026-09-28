"""Benchmark harness and microsecond telemetry for apex-kernel-mesh.

Runs all core subsystems with representative workloads and produces
structured ASCII performance reports.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from .registry import KernelRegistry, KernelDescriptor, CapabilityDescriptor
from .router import CrossRegistryRouter
from .composer import DAGComposer, PipelineStage
from .executor import PipelineExecutor
from .health import HealthMonitor
from .balancer import LatencyBudgetPartitioner


@dataclass
class BenchmarkResult:
    """Result of a single benchmark measurement."""
    name: str
    latency_us: float
    iterations: int
    p50_us: float = 0.0
    p99_us: float = 0.0
    result_summary: str = ""


def _build_sample_registry() -> KernelRegistry:
    """Build a representative 16-kernel registry for benchmarking."""
    registry = KernelRegistry()

    kernel_specs = [
        ("datacenter-np-hard-kernel", "Datacenter & AI Infrastructure", [
            ("vector_bin_packing", "Multi-dimensional GPU/CPU/RAM bin packing", ("datacenter", "infrastructure")),
            ("thermal_floorplanning", "Thermal-aware rack cooling optimization", ("datacenter", "thermal")),
            ("flow_shop_scheduling", "Job flow shop scheduling with dependencies", ("datacenter", "scheduling")),
        ]),
        ("hft-microstructure-kernel", "Quantitative HFT & Microstructure", [
            ("triangular_arbitrage", "Cross-exchange triangular arbitrage cycle detection", ("finance", "arbitrage")),
            ("optimal_liquidation", "Almgren-Chriss optimal execution trajectory", ("finance", "execution")),
            ("lob_matching", "Microsecond deterministic LOB matching engine", ("finance", "matching")),
        ]),
        ("smart-grid-fusion-vpp-kernel", "Smart Grid & Fusion VPP", [
            ("security_constrained_opf", "N-1 security-constrained optimal power flow", ("energy", "grid")),
            ("unit_commitment", "24-hr mixed-integer thermal + BESS commitment", ("energy", "scheduling")),
            ("vpp_arbitrage", "VPP energy arbitrage vs regulation co-opt", ("energy", "trading")),
        ]),
        ("frontier-ai-compiler-kernel", "Frontier AI Silicon & Compilers", [
            ("tensor_parallelism_4d", "4D tensor parallelism TP/PP/DP/CP scheduling", ("ai", "compiler")),
            ("flash_attention_tiling", "FlashAttention-3 SRAM bank loop tiling", ("ai", "memory")),
            ("moe_dispatch", "Top-k MoE auxiliary-free token dispatch", ("ai", "routing")),
        ]),
        ("apex-mcp-foundry", "Universal MCP Foundry", [
            ("budgeted_selection", "Submodular budgeted capability selector", ("mcp", "selection")),
            ("dependency_graph", "Tarjan SCC cycle detection", ("mcp", "graph")),
            ("sandboxed_execution", "AST-gated sandboxed adapter runner", ("mcp", "execution")),
        ]),
        ("apex-swarm-orchestrator-kernel", "Swarm Orchestrator", [
            ("spectral_clustering", "k-way normalized spectral graph cut", ("swarm", "clustering")),
            ("map_elites", "MAP-Elites quality-diversity evolution", ("swarm", "evolution")),
            ("bls_quorum", "BLS threshold Byzantine fault consensus", ("swarm", "consensus")),
        ]),
        ("autonomous-cyber-defense-kernel", "Autonomous Cyber Defense", [
            ("attack_graph_mincut", "Multi-terminal min-cut quarantine", ("security", "graph")),
            ("stackelberg_honeynet", "Bilevel Stackelberg honeynet allocator", ("security", "deception")),
            ("cfi_verifier", "Formal CFI binary hot-patch verifier", ("security", "verification")),
        ]),
        ("apex-infrastructure-killswitch-kernel", "Infrastructure Kill-Switch", [
            ("relay_coordination", "Combinatorial protection relay grading", ("infrastructure", "protection")),
            ("dvfs_shedding", "4D tensor-parallel DVFS bubble injection", ("infrastructure", "power")),
            ("impedance_splitting", "Complex electrochemical impedance allocation", ("infrastructure", "battery")),
        ]),
    ]

    for kid, name, caps in kernel_specs:
        capabilities = tuple(
            CapabilityDescriptor(
                name=cname,
                description=cdesc,
                domain_tags=tags,
                estimated_tokens=80 + len(cdesc),
                typical_latency_us=50.0,
            )
            for cname, cdesc, tags in caps
        )
        registry.add_kernel(KernelDescriptor(
            kernel_id=kid,
            display_name=name,
            capabilities=capabilities,
            domain_tags=tuple(set(t for c in caps for t in c[2])),
        ))

    return registry


def run_benchmarks(iterations: int = 20) -> list[BenchmarkResult]:
    """Run all benchmark suites."""
    results: list[BenchmarkResult] = []
    registry = _build_sample_registry()

    # 1. Registry Lookup
    timings: list[float] = []
    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        caps = registry.find_capabilities("arbitrage scheduling optimization")
        t1 = time.perf_counter_ns()
        timings.append((t1 - t0) / 1000)

    timings.sort()
    results.append(BenchmarkResult(
        name="Registry Capability Search",
        latency_us=sum(timings) / len(timings),
        iterations=iterations,
        p50_us=timings[len(timings) // 2],
        p99_us=timings[int(len(timings) * 0.99)],
        result_summary=f"{len(caps)} capabilities matched across {registry.kernel_count} kernels",
    ))

    # 2. Cross-Registry Routing
    router = CrossRegistryRouter(registry)
    timings = []
    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        result = router.route(
            "optimize datacenter power while hedging energy futures",
            token_budget=500,
        )
        t1 = time.perf_counter_ns()
        timings.append((t1 - t0) / 1000)

    timings.sort()
    results.append(BenchmarkResult(
        name="Submodular Cross-Registry Routing",
        latency_us=sum(timings) / len(timings),
        iterations=iterations,
        p50_us=timings[len(timings) // 2],
        p99_us=timings[int(len(timings) * 0.99)],
        result_summary=f"{result.count} selected, {result.token_reduction_pct:.1f}% token reduction",
    ))

    # 3. DAG Composition
    composer = DAGComposer()
    stages = [
        PipelineStage("s1", "datacenter-np-hard-kernel", "vector_bin_packing", 50.0),
        PipelineStage("s2", "smart-grid-fusion-vpp-kernel", "security_constrained_opf", 80.0, dependencies=("s1",)),
        PipelineStage("s3", "hft-microstructure-kernel", "triangular_arbitrage", 30.0, dependencies=("s1",)),
        PipelineStage("s4", "frontier-ai-compiler-kernel", "tensor_parallelism_4d", 60.0, dependencies=("s2", "s3")),
        PipelineStage("s5", "apex-infrastructure-killswitch-kernel", "relay_coordination", 45.0, dependencies=("s4",)),
    ]
    timings = []
    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        dag = composer.compose(stages)
        t1 = time.perf_counter_ns()
        timings.append((t1 - t0) / 1000)

    timings.sort()
    results.append(BenchmarkResult(
        name="DAG Composition & CPM Scheduling",
        latency_us=sum(timings) / len(timings),
        iterations=iterations,
        p50_us=timings[len(timings) // 2],
        p99_us=timings[int(len(timings) * 0.99)],
        result_summary=f"{dag.stage_count} stages, makespan={dag.makespan_us:.1f}µs, parallelism={dag.parallelism_factor:.2f}x, critical path={len(dag.critical_path)}",
    ))

    # 4. Pipeline Execution
    executor = PipelineExecutor()
    timings = []
    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        pr = executor.execute(dag)
        t1 = time.perf_counter_ns()
        timings.append((t1 - t0) / 1000)

    timings.sort()
    results.append(BenchmarkResult(
        name="Pipeline Execution (Simulated)",
        latency_us=sum(timings) / len(timings),
        iterations=iterations,
        p50_us=timings[len(timings) // 2],
        p99_us=timings[int(len(timings) * 0.99)],
        result_summary=f"{pr.stage_count} stages executed, success={pr.success}",
    ))

    # 5. Latency Budget Partitioning
    partitioner = LatencyBudgetPartitioner()
    stage_specs = [(f"s{i}", f"kernel-{i}", 50.0 + i * 20) for i in range(8)]
    timings = []
    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        alloc = partitioner.partition(stage_specs, total_budget_us=2000.0)
        t1 = time.perf_counter_ns()
        timings.append((t1 - t0) / 1000)

    timings.sort()
    results.append(BenchmarkResult(
        name="Water-Filling Budget Partitioning",
        latency_us=sum(timings) / len(timings),
        iterations=iterations,
        p50_us=timings[len(timings) // 2],
        p99_us=timings[int(len(timings) * 0.99)],
        result_summary=f"{alloc.stage_count} stages, balance={alloc.balance_ratio:.3f}, max_util={alloc.max_utilization:.3f}",
    ))

    # 6. Byzantine Voting
    monitor = HealthMonitor()
    responses = [("k1", {"value": 42}), ("k2", {"value": 42}), ("k3", {"value": 99}), ("k4", {"value": 42})]
    timings = []
    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        consensus, disagreeing = monitor.byzantine_vote(responses)
        t1 = time.perf_counter_ns()
        timings.append((t1 - t0) / 1000)

    timings.sort()
    results.append(BenchmarkResult(
        name="Byzantine Fault Voting (4 replicas)",
        latency_us=sum(timings) / len(timings),
        iterations=iterations,
        p50_us=timings[len(timings) // 2],
        p99_us=timings[int(len(timings) * 0.99)],
        result_summary=f"Consensus={consensus}, {len(disagreeing)} disagreeing",
    ))

    return results


def print_report(results: list[BenchmarkResult]) -> None:
    """Print structured ASCII benchmark report."""
    header = "Apex Kernel Mesh — Benchmark Telemetry"
    separator = "=" * 72

    print(f"\n{separator}")
    print(f"  {header}")
    print(separator)
    print(f"  {'Subsystem':<42} {'p50 (µs)':>10} {'p99 (µs)':>10}")
    print(f"  {'-' * 42} {'-' * 10} {'-' * 10}")

    for r in results:
        print(f"  {r.name:<42} {r.p50_us:>10.2f} {r.p99_us:>10.2f}")
        print(f"    → {r.result_summary}")

    total_p50 = sum(r.p50_us for r in results)
    print(f"  {'-' * 42} {'-' * 10} {'-' * 10}")
    print(f"  {'TOTAL':<42} {total_p50:>10.2f}")
    print(separator)
    print()
