"""Pipeline Executor — DAG Execution Engine with Simulated Dispatch.

Executes a composed DAG by walking stages in topological order,
resolving dependencies, invoking kernel capabilities, and
aggregating results.

In simulation mode (default), capabilities are resolved via
registered handler functions. In production, they would
dispatch to actual kernel processes via MCP/stdio/HTTP.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .composer import ExecutionDAG, ScheduledStage
from .health import HealthMonitor, BreakerState


@dataclass
class StageResult:
    """Result of executing a single pipeline stage."""
    stage_id: str
    kernel_id: str
    capability_name: str
    success: bool = True
    result: Any = None
    error: str | None = None
    latency_us: float = 0.0


@dataclass
class PipelineResult:
    """Aggregated result of executing an entire pipeline."""
    stage_results: list[StageResult] = field(default_factory=list)
    total_latency_us: float = 0.0
    success: bool = True
    error_stages: list[str] = field(default_factory=list)
    parallel_speedup: float = 1.0

    @property
    def stage_count(self) -> int:
        return len(self.stage_results)


# Type for a kernel handler function
KernelHandler = Callable[[str, str, dict[str, Any]], Any]


class PipelineExecutor:
    """Executes DAGs with dependency resolution and circuit breaking.

    Handler functions are registered per (kernel_id, capability_name)
    and invoked during execution. If no handler is registered, the
    stage produces a synthetic result.

    The executor integrates with HealthMonitor to skip stages
    from tripped kernels and record success/failure metrics.
    """

    def __init__(self, health_monitor: HealthMonitor | None = None) -> None:
        self._health = health_monitor or HealthMonitor()
        self._handlers: dict[tuple[str, str], KernelHandler] = {}
        self._global_handler: KernelHandler | None = None

    def register_handler(
        self,
        kernel_id: str,
        capability_name: str,
        handler: KernelHandler,
    ) -> None:
        """Register a handler for a specific (kernel, capability) pair."""
        self._handlers[(kernel_id, capability_name)] = handler

    def set_global_handler(self, handler: KernelHandler) -> None:
        """Set a fallback handler for all unregistered capabilities."""
        self._global_handler = handler

    def execute(
        self,
        dag: ExecutionDAG,
        initial_inputs: dict[str, Any] | None = None,
    ) -> PipelineResult:
        """Execute a DAG pipeline.

        Args:
            dag: Composed DAG from DAGComposer.
            initial_inputs: Optional inputs available to root stages.

        Returns:
            PipelineResult with all stage results.
        """
        if not dag.stages:
            return PipelineResult()

        results: dict[str, StageResult] = {}
        stage_outputs: dict[str, Any] = {}
        if initial_inputs:
            stage_outputs.update(initial_inputs)

        total_wall_time = 0.0
        total_serial_time = 0.0
        error_stages: list[str] = []

        for scheduled in dag.stages:
            stage = scheduled.stage
            t0 = time.perf_counter_ns()

            # Check circuit breaker
            breaker = self._health.get_breaker(stage.kernel_id)
            if not breaker.allow_request():
                sr = StageResult(
                    stage_id=stage.stage_id,
                    kernel_id=stage.kernel_id,
                    capability_name=stage.capability_name,
                    success=False,
                    error=f"Circuit breaker OPEN for {stage.kernel_id}",
                )
                results[stage.stage_id] = sr
                error_stages.append(stage.stage_id)
                continue

            # Gather dependency outputs
            dep_data: dict[str, Any] = {}
            dep_failed = False
            for dep_id in stage.dependencies:
                if dep_id in results and results[dep_id].success:
                    dep_data[dep_id] = stage_outputs.get(dep_id)
                elif dep_id in results and not results[dep_id].success:
                    dep_failed = True
                    break

            if dep_failed:
                sr = StageResult(
                    stage_id=stage.stage_id,
                    kernel_id=stage.kernel_id,
                    capability_name=stage.capability_name,
                    success=False,
                    error=f"Dependency {dep_id} failed",
                )
                results[stage.stage_id] = sr
                error_stages.append(stage.stage_id)
                breaker.record_failure()
                continue

            # Merge parameters with dependency data
            params = {**stage.parameters, "_deps": dep_data}

            # Invoke handler
            try:
                handler = self._handlers.get(
                    (stage.kernel_id, stage.capability_name)
                )
                if handler is None:
                    handler = self._global_handler

                if handler is not None:
                    output = handler(
                        stage.kernel_id, stage.capability_name, params
                    )
                else:
                    # Synthetic result for simulation
                    output = {
                        "stage_id": stage.stage_id,
                        "kernel_id": stage.kernel_id,
                        "capability": stage.capability_name,
                        "status": "simulated",
                    }

                t1 = time.perf_counter_ns()
                latency = (t1 - t0) / 1000  # nanoseconds to microseconds

                stage_outputs[stage.stage_id] = output
                sr = StageResult(
                    stage_id=stage.stage_id,
                    kernel_id=stage.kernel_id,
                    capability_name=stage.capability_name,
                    success=True,
                    result=output,
                    latency_us=latency,
                )
                results[stage.stage_id] = sr
                breaker.record_success()
                total_serial_time += latency

            except Exception as e:
                t1 = time.perf_counter_ns()
                latency = (t1 - t0) / 1000

                sr = StageResult(
                    stage_id=stage.stage_id,
                    kernel_id=stage.kernel_id,
                    capability_name=stage.capability_name,
                    success=False,
                    error=str(e),
                    latency_us=latency,
                )
                results[stage.stage_id] = sr
                error_stages.append(stage.stage_id)
                breaker.record_failure()
                total_serial_time += latency

        total_wall_time = total_serial_time  # Sequential execution

        all_results = [results[s.stage.stage_id] for s in dag.stages if s.stage.stage_id in results]

        # Parallel speedup = serial time / makespan
        speedup = total_serial_time / dag.makespan_us if dag.makespan_us > 0 else 1.0

        return PipelineResult(
            stage_results=all_results,
            total_latency_us=total_wall_time,
            success=len(error_stages) == 0,
            error_stages=error_stages,
            parallel_speedup=speedup,
        )
