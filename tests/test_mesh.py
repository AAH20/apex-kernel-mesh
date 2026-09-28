"""Comprehensive test suite for Apex Kernel Mesh.

Tests cover:
  1. Registry auto-discovery and capability indexing
  2. Health-aware kernel filtering
  3. Submodular cross-registry routing
  4. Exact branch-and-bound routing
  5. DAG composition with topological sort
  6. Cycle detection in dependency graphs
  7. Critical Path Method (CPM) scheduling
  8. Pipeline execution with handler dispatch
  9. Circuit breaker state transitions
  10. Byzantine fault voting
  11. Latency budget partitioning (water-filling)
  12. Load balancing
  13. Canonical ordering for KV-cache stability
  14. Benchmark harness
"""

import unittest

from apex_kernel_mesh.registry import (
    KernelRegistry,
    KernelDescriptor,
    CapabilityDescriptor,
    KernelStatus,
)
from apex_kernel_mesh.router import CrossRegistryRouter
from apex_kernel_mesh.composer import DAGComposer, PipelineStage
from apex_kernel_mesh.executor import PipelineExecutor
from apex_kernel_mesh.health import (
    CircuitBreaker,
    CircuitBreakerConfig,
    HealthMonitor,
    BreakerState,
)
from apex_kernel_mesh.balancer import LatencyBudgetPartitioner
from apex_kernel_mesh.benchmark import run_benchmarks


def _make_cap(name: str, desc: str = "", tags: tuple[str, ...] = (), tokens: int = 100) -> CapabilityDescriptor:
    return CapabilityDescriptor(
        name=name,
        description=desc or f"{name} solver",
        domain_tags=tags,
        estimated_tokens=tokens,
        typical_latency_us=50.0,
    )


def _make_kernel(kid: str, caps: list[CapabilityDescriptor]) -> KernelDescriptor:
    all_tags = tuple(set(t for c in caps for t in c.domain_tags))
    return KernelDescriptor(
        kernel_id=kid,
        display_name=kid.replace("-", " ").title(),
        capabilities=tuple(caps),
        domain_tags=all_tags,
    )


def _build_test_registry() -> KernelRegistry:
    """Build a small test registry with 4 kernels, 12 capabilities."""
    registry = KernelRegistry()

    registry.add_kernel(_make_kernel("dc-kernel", [
        _make_cap("bin_packing", "Multi-dimensional bin packing", ("datacenter", "packing")),
        _make_cap("thermal_opt", "Thermal floorplanning optimization", ("datacenter", "thermal")),
        _make_cap("flow_shop", "Job flow shop scheduling", ("datacenter", "scheduling")),
    ]))

    registry.add_kernel(_make_kernel("hft-kernel", [
        _make_cap("arbitrage", "Cross-exchange triangular arbitrage", ("finance", "arbitrage")),
        _make_cap("liquidation", "Almgren-Chriss optimal liquidation", ("finance", "execution")),
        _make_cap("matching", "LOB matching engine", ("finance", "matching")),
    ]))

    registry.add_kernel(_make_kernel("grid-kernel", [
        _make_cap("opf", "Security-constrained optimal power flow", ("energy", "grid")),
        _make_cap("unit_commit", "24-hr unit commitment scheduling", ("energy", "scheduling")),
        _make_cap("vpp_arb", "VPP energy arbitrage", ("energy", "trading")),
    ]))

    registry.add_kernel(_make_kernel("cyber-kernel", [
        _make_cap("mincut", "Attack graph multi-terminal min-cut", ("security", "graph")),
        _make_cap("honeynet", "Stackelberg honeynet allocator", ("security", "deception")),
        _make_cap("cfi", "Binary CFI verifier", ("security", "verification")),
    ]))

    return registry


class TestKernelRegistry(unittest.TestCase):
    """Tests for kernel registration and capability search."""

    def test_registry_counts(self):
        registry = _build_test_registry()
        self.assertEqual(registry.kernel_count, 4)
        self.assertEqual(registry.total_capabilities, 12)

    def test_find_capabilities_by_query(self):
        registry = _build_test_registry()
        caps = registry.find_capabilities("arbitrage scheduling")
        self.assertGreater(len(caps), 0)
        # Arbitrage and scheduling capabilities should be scored highest
        names = [c.name for _, c in caps[:3]]
        self.assertTrue(
            any("arbitrage" in n or "scheduling" in n or "flow_shop" in n for n in names),
            f"Expected arbitrage/scheduling in top results, got {names}",
        )

    def test_find_capabilities_by_domain_tag(self):
        registry = _build_test_registry()
        caps = registry.find_capabilities(domain_tags=("finance",))
        kernel_ids = set(kid for kid, _ in caps)
        self.assertEqual(kernel_ids, {"hft-kernel"})
        self.assertEqual(len(caps), 3)

    def test_healthy_kernel_filtering(self):
        registry = _build_test_registry()
        # Trip one kernel
        registry.set_status("hft-kernel", KernelStatus.TRIPPED)
        healthy = registry.healthy_kernels()
        healthy_ids = {k.kernel_id for k in healthy}
        self.assertNotIn("hft-kernel", healthy_ids)
        self.assertEqual(len(healthy), 3)

        # Healthy-only search should exclude tripped kernel
        caps = registry.find_capabilities("arbitrage", healthy_only=True)
        cap_kernels = {kid for kid, _ in caps}
        self.assertNotIn("hft-kernel", cap_kernels)

    def test_remove_kernel(self):
        registry = _build_test_registry()
        self.assertTrue(registry.remove_kernel("cyber-kernel"))
        self.assertEqual(registry.kernel_count, 3)
        self.assertFalse(registry.remove_kernel("nonexistent"))


class TestCrossRegistryRouter(unittest.TestCase):
    """Tests for submodular and exact routing."""

    def test_submodular_routing_respects_budget(self):
        registry = _build_test_registry()
        router = CrossRegistryRouter(registry)
        result = router.route(
            "optimize datacenter power",
            token_budget=250,
        )
        self.assertGreater(result.count, 0)
        self.assertLessEqual(result.total_tokens, 250)
        self.assertGreater(result.token_reduction_pct, 0)

    def test_exact_branch_and_bound(self):
        registry = _build_test_registry()
        router = CrossRegistryRouter(registry)
        result = router.route(
            "security graph analysis",
            token_budget=400,
            strategy="exact",
        )
        self.assertGreater(result.count, 0)
        self.assertLessEqual(result.total_tokens, 400)

    def test_canonical_ordering_is_deterministic(self):
        registry = _build_test_registry()
        router = CrossRegistryRouter(registry)
        r1 = router.route("power scheduling", token_budget=500)
        r2 = router.route("power scheduling", token_budget=500)
        self.assertEqual(r1.canonical_order, r2.canonical_order)
        # Verify sorted order
        self.assertEqual(r1.canonical_order, sorted(r1.canonical_order))

    def test_zero_budget_returns_empty(self):
        registry = _build_test_registry()
        router = CrossRegistryRouter(registry)
        result = router.route("anything", token_budget=0)
        self.assertEqual(result.count, 0)


class TestDAGComposer(unittest.TestCase):
    """Tests for DAG composition and scheduling."""

    def test_linear_pipeline(self):
        composer = DAGComposer()
        stages = [
            PipelineStage("s1", "k1", "c1", 50.0),
            PipelineStage("s2", "k2", "c2", 30.0, dependencies=("s1",)),
            PipelineStage("s3", "k3", "c3", 20.0, dependencies=("s2",)),
        ]
        dag = composer.compose(stages)
        self.assertEqual(dag.stage_count, 3)
        self.assertAlmostEqual(dag.makespan_us, 100.0)
        self.assertEqual(dag.topological_order, ["s1", "s2", "s3"])
        # All stages on critical path in a linear pipeline
        self.assertEqual(len(dag.critical_path), 3)

    def test_diamond_pipeline_parallelism(self):
        composer = DAGComposer()
        stages = [
            PipelineStage("root", "k1", "c1", 10.0),
            PipelineStage("left", "k2", "c2", 50.0, dependencies=("root",)),
            PipelineStage("right", "k3", "c3", 30.0, dependencies=("root",)),
            PipelineStage("join", "k4", "c4", 10.0, dependencies=("left", "right")),
        ]
        dag = composer.compose(stages)
        # Makespan = 10 + max(50, 30) + 10 = 70
        self.assertAlmostEqual(dag.makespan_us, 70.0)
        # Total work = 100, makespan = 70, parallelism > 1
        self.assertGreater(dag.parallelism_factor, 1.0)

        # Parallel groups: root alone, left+right together, join alone
        groups = composer.parallel_groups(dag)
        self.assertEqual(len(groups), 3)

    def test_cycle_detection(self):
        composer = DAGComposer()
        stages = [
            PipelineStage("a", "k1", "c1", 10.0, dependencies=("c",)),
            PipelineStage("b", "k2", "c2", 10.0, dependencies=("a",)),
            PipelineStage("c", "k3", "c3", 10.0, dependencies=("b",)),
        ]
        with self.assertRaises(ValueError) as ctx:
            composer.compose(stages)
        self.assertIn("cycle", str(ctx.exception).lower())

    def test_invalid_dependency_reference(self):
        composer = DAGComposer()
        stages = [
            PipelineStage("s1", "k1", "c1", 10.0, dependencies=("nonexistent",)),
        ]
        with self.assertRaises(ValueError):
            composer.compose(stages)


class TestPipelineExecutor(unittest.TestCase):
    """Tests for pipeline execution."""

    def test_execution_with_handlers(self):
        composer = DAGComposer()
        stages = [
            PipelineStage("s1", "k1", "solver_a", 10.0),
            PipelineStage("s2", "k2", "solver_b", 10.0, dependencies=("s1",)),
        ]
        dag = composer.compose(stages)

        executor = PipelineExecutor()
        results_log = []

        def handler(kid: str, cap: str, params: dict) -> dict:
            results_log.append((kid, cap))
            return {"computed": f"{kid}:{cap}"}

        executor.set_global_handler(handler)
        result = executor.execute(dag)

        self.assertTrue(result.success)
        self.assertEqual(result.stage_count, 2)
        self.assertEqual(len(results_log), 2)
        self.assertEqual(results_log[0], ("k1", "solver_a"))
        self.assertEqual(results_log[1], ("k2", "solver_b"))

    def test_execution_with_tripped_breaker(self):
        monitor = HealthMonitor()
        breaker = monitor.get_breaker("k1")
        breaker.force_trip()

        composer = DAGComposer()
        stages = [PipelineStage("s1", "k1", "c1", 10.0)]
        dag = composer.compose(stages)

        executor = PipelineExecutor(health_monitor=monitor)
        result = executor.execute(dag)

        self.assertFalse(result.success)
        self.assertIn("s1", result.error_stages)


class TestCircuitBreaker(unittest.TestCase):
    """Tests for circuit breaker state transitions."""

    def test_trips_after_threshold(self):
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker("test-kernel", config)

        self.assertEqual(breaker.state, BreakerState.CLOSED)
        self.assertTrue(breaker.allow_request())

        # Record 3 failures
        for _ in range(3):
            breaker.record_failure()

        self.assertEqual(breaker.state, BreakerState.OPEN)
        self.assertFalse(breaker.allow_request())
        self.assertEqual(breaker.metrics.trip_count, 1)

    def test_success_resets_consecutive_failures(self):
        config = CircuitBreakerConfig(failure_threshold=3)
        breaker = CircuitBreaker("test-kernel", config)

        breaker.record_failure()
        breaker.record_failure()
        breaker.record_success()  # Should reset consecutive failures
        breaker.record_failure()

        # Should NOT trip (only 1 consecutive failure after reset)
        self.assertEqual(breaker.state, BreakerState.CLOSED)

    def test_force_trip_and_close(self):
        breaker = CircuitBreaker("test-kernel")
        breaker.force_trip()
        self.assertEqual(breaker.state, BreakerState.OPEN)

        breaker.force_close()
        self.assertEqual(breaker.state, BreakerState.CLOSED)
        self.assertTrue(breaker.allow_request())


class TestByzantineVoting(unittest.TestCase):
    """Tests for Byzantine fault detection."""

    def test_majority_consensus(self):
        monitor = HealthMonitor()
        responses = [
            ("k1", 42),
            ("k2", 42),
            ("k3", 99),  # disagreeing
        ]
        consensus, disagreeing = monitor.byzantine_vote(responses)
        self.assertEqual(consensus, 42)
        self.assertEqual(disagreeing, ["k3"])

    def test_no_quorum_raises(self):
        monitor = HealthMonitor()
        responses = [
            ("k1", 1),
            ("k2", 2),
            ("k3", 3),
        ]
        with self.assertRaises(ValueError) as ctx:
            monitor.byzantine_vote(responses)
        self.assertIn("quorum", str(ctx.exception).lower())

    def test_unanimous_consensus(self):
        monitor = HealthMonitor()
        responses = [("k1", "ok"), ("k2", "ok"), ("k3", "ok")]
        consensus, disagreeing = monitor.byzantine_vote(responses)
        self.assertEqual(consensus, "ok")
        self.assertEqual(disagreeing, [])


class TestLatencyBudgetPartitioner(unittest.TestCase):
    """Tests for latency budget partitioning."""

    def test_water_filling_equalizes_utilization(self):
        partitioner = LatencyBudgetPartitioner()
        stages = [
            ("s1", "k1", 100.0),
            ("s2", "k2", 200.0),
            ("s3", "k3", 50.0),
        ]
        alloc = partitioner.partition(stages, total_budget_us=1000.0)

        self.assertEqual(alloc.stage_count, 3)
        # Budget should sum to approximately the total
        allocated_sum = sum(sb.allocated_budget_us for sb in alloc.stage_budgets)
        self.assertAlmostEqual(allocated_sum, 1000.0, delta=1.0)
        # Balance ratio should be close to 1.0 (well-balanced)
        self.assertGreater(alloc.balance_ratio, 0.5)

    def test_proportional_strategy(self):
        partitioner = LatencyBudgetPartitioner()
        stages = [
            ("s1", "k1", 100.0),
            ("s2", "k2", 300.0),
        ]
        alloc = partitioner.partition(
            stages, total_budget_us=800.0, strategy="proportional"
        )
        # s2 should get roughly 3x the budget of s1
        b1 = alloc.stage_budgets[0].allocated_budget_us
        b2 = alloc.stage_budgets[1].allocated_budget_us
        self.assertGreater(b2, b1)

    def test_load_balancing(self):
        partitioner = LatencyBudgetPartitioner()
        dist = partitioner.balance_load(
            ["inst-1", "inst-2", "inst-3"],
            [100.0, 200.0, 300.0],
        )
        # All instances should get equal load (200.0 each)
        for load in dist.instance_loads.values():
            self.assertAlmostEqual(load, 200.0)
        self.assertAlmostEqual(dist.imbalance_ratio, 0.0)


class TestBenchmarkHarness(unittest.TestCase):
    """Tests for the benchmark suite."""

    def test_benchmarks_run_without_error(self):
        results = run_benchmarks(iterations=3)
        self.assertGreaterEqual(len(results), 5)
        for r in results:
            self.assertGreater(r.p50_us, 0)
            self.assertGreater(len(r.result_summary), 0)


if __name__ == "__main__":
    unittest.main()
