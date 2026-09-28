"""Topological planning and critical-path analysis for precedence DAGs.

The composer validates dependencies, computes a topological order, and derives
a critical path assuming unlimited execution resources. It does not implement
general resource-constrained job-shop scheduling or execute stages.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PipelineStage:
    """A single stage in an execution pipeline.

    Attributes:
        stage_id: Unique identifier for this stage.
        kernel_id: Which kernel this stage uses.
        capability_name: Which capability within the kernel.
        estimated_latency_us: Expected execution time.
        dependencies: Stage IDs this stage depends on.
        parameters: Execution parameters dict.
    """
    stage_id: str
    kernel_id: str
    capability_name: str
    estimated_latency_us: float = 100.0
    dependencies: tuple[str, ...] = ()
    parameters: dict[str, Any] = field(default_factory=dict)


@dataclass
class ScheduledStage:
    """A stage with computed scheduling information."""
    stage: PipelineStage
    start_time_us: float = 0.0
    end_time_us: float = 0.0
    is_critical: bool = False
    slack_us: float = 0.0
    topological_order: int = 0


@dataclass
class ExecutionDAG:
    """A directed acyclic graph of pipeline stages with schedule.

    Attributes:
        stages: Ordered list of scheduled stages.
        makespan_us: Total execution time (critical path length).
        critical_path: Stage IDs on the critical path.
        parallelism_factor: Ratio of total work to makespan.
        topological_order: Valid topological ordering of stage IDs.
    """
    stages: list[ScheduledStage] = field(default_factory=list)
    makespan_us: float = 0.0
    critical_path: list[str] = field(default_factory=list)
    parallelism_factor: float = 1.0
    topological_order: list[str] = field(default_factory=list)

    @property
    def stage_count(self) -> int:
        return len(self.stages)

    @property
    def total_work_us(self) -> float:
        return sum(s.stage.estimated_latency_us for s in self.stages)


class DAGComposer:
    """Composes pipeline stages into an optimally scheduled DAG.

    Supports:
      - Topological sorting with cycle detection (Kahn's algorithm)
      - Critical Path Method (CPM) for makespan computation
      - Forward/backward pass for earliest/latest start times
      - Slack computation for non-critical stages
      - Parallel execution grouping
    """

    def compose(self, stages: list[PipelineStage]) -> ExecutionDAG:
        """Build and schedule an execution DAG.

        Args:
            stages: Unordered list of pipeline stages with dependencies.

        Returns:
            ExecutionDAG with optimal schedule.

        Raises:
            ValueError: If the dependency graph contains cycles.
        """
        if not stages:
            return ExecutionDAG()

        stage_map = {s.stage_id: s for s in stages}

        # Validate dependencies exist
        for s in stages:
            for dep in s.dependencies:
                if dep not in stage_map:
                    raise ValueError(
                        f"Stage '{s.stage_id}' depends on unknown stage '{dep}'"
                    )

        # Topological sort (Kahn's algorithm with cycle detection)
        topo_order = self._topological_sort(stages, stage_map)

        # Forward pass: compute earliest start/end times
        earliest_start: dict[str, float] = {}
        earliest_end: dict[str, float] = {}

        for sid in topo_order:
            stage = stage_map[sid]
            if stage.dependencies:
                es = max(earliest_end.get(d, 0.0) for d in stage.dependencies)
            else:
                es = 0.0
            earliest_start[sid] = es
            earliest_end[sid] = es + stage.estimated_latency_us

        makespan = max(earliest_end.values()) if earliest_end else 0.0

        # Backward pass: compute latest start/end times
        latest_end: dict[str, float] = {}
        latest_start: dict[str, float] = {}

        # Build reverse adjacency
        successors: dict[str, list[str]] = {s.stage_id: [] for s in stages}
        for s in stages:
            for dep in s.dependencies:
                successors[dep].append(s.stage_id)

        for sid in reversed(topo_order):
            stage = stage_map[sid]
            if successors[sid]:
                le = min(latest_start.get(succ, makespan) for succ in successors[sid])
            else:
                le = makespan
            latest_end[sid] = le
            latest_start[sid] = le - stage.estimated_latency_us

        # Compute slack and identify critical path
        scheduled: list[ScheduledStage] = []
        critical_path: list[str] = []
        total_work = 0.0

        for i, sid in enumerate(topo_order):
            stage = stage_map[sid]
            slack = latest_start[sid] - earliest_start[sid]
            is_critical = abs(slack) < 1e-9  # Float tolerance

            if is_critical:
                critical_path.append(sid)

            total_work += stage.estimated_latency_us

            scheduled.append(
                ScheduledStage(
                    stage=stage,
                    start_time_us=earliest_start[sid],
                    end_time_us=earliest_end[sid],
                    is_critical=is_critical,
                    slack_us=max(0.0, slack),
                    topological_order=i,
                )
            )

        parallelism = total_work / makespan if makespan > 0 else 1.0

        return ExecutionDAG(
            stages=scheduled,
            makespan_us=makespan,
            critical_path=critical_path,
            parallelism_factor=parallelism,
            topological_order=topo_order,
        )

    def _topological_sort(
        self,
        stages: list[PipelineStage],
        stage_map: dict[str, PipelineStage],
    ) -> list[str]:
        """Kahn's algorithm for topological sorting with cycle detection.

        Returns:
            List of stage IDs in topological order.

        Raises:
            ValueError: If the graph contains a cycle.
        """
        # Compute in-degrees
        in_degree: dict[str, int] = {s.stage_id: 0 for s in stages}
        for s in stages:
            for dep in s.dependencies:
                # dep -> s (dep must come before s)
                in_degree[s.stage_id] = in_degree.get(s.stage_id, 0) + 1

        # Initialize queue with zero in-degree nodes
        queue: list[str] = [
            sid for sid, deg in in_degree.items() if deg == 0
        ]
        # Sort for deterministic output
        queue.sort()

        result: list[str] = []

        while queue:
            # Pick lexicographically smallest (deterministic)
            queue.sort()
            current = queue.pop(0)
            result.append(current)

            # Find successors (stages that depend on current)
            for s in stages:
                if current in s.dependencies:
                    in_degree[s.stage_id] -= 1
                    if in_degree[s.stage_id] == 0:
                        queue.append(s.stage_id)

        if len(result) != len(stages):
            # Cycle detected — find the cycle members
            cycle_members = [
                sid for sid, deg in in_degree.items() if deg > 0
            ]
            raise ValueError(
                f"Dependency cycle detected among stages: {cycle_members}"
            )

        return result

    def parallel_groups(self, dag: ExecutionDAG) -> list[list[str]]:
        """Group stages that can execute in parallel.

        Returns list of groups, where stages within each group
        have no dependencies on each other and can run concurrently.
        """
        if not dag.stages:
            return []

        groups: list[list[str]] = []
        current_group: list[str] = []
        current_start = -1.0

        for s in sorted(dag.stages, key=lambda x: x.start_time_us):
            if abs(s.start_time_us - current_start) < 1e-9:
                current_group.append(s.stage.stage_id)
            else:
                if current_group:
                    groups.append(current_group)
                current_group = [s.stage.stage_id]
                current_start = s.start_time_us

        if current_group:
            groups.append(current_group)

        return groups
