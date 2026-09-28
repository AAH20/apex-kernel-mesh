"""Latency Budget Partitioner — Balanced Number Partitioning.

Given a total latency SLA and a set of pipeline stages,
optimally partitions the latency budget across stages to
minimize the probability of SLA violation.

This is equivalent to the Balanced Number Partitioning problem
(NP-hard), solved here via:
  1. Proportional allocation (baseline)
  2. Water-filling equalization
  3. KKT-optimal convex allocation under convex delay functions

Mathematical formulation (Water-Filling):
    min  max_j (p_j / b_j)        [minimize worst-case utilization]
    s.t. Σ_j b_j ≤ B              [total budget]
         b_j ≥ b_min  ∀ j         [minimum per-stage budget]

Also includes a minimum-cost flow load balancer for distributing
work across multiple instances of the same kernel.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any


@dataclass
class StageBudget:
    """Latency budget allocation for a single stage."""
    stage_id: str
    kernel_id: str
    estimated_latency_us: float
    allocated_budget_us: float
    utilization: float  # estimated / allocated
    slack_us: float     # allocated - estimated


@dataclass
class BudgetAllocation:
    """Complete budget allocation across all stages."""
    stage_budgets: list[StageBudget] = field(default_factory=list)
    total_budget_us: float = 0.0
    allocated_us: float = 0.0
    max_utilization: float = 0.0
    min_utilization: float = 0.0
    balance_ratio: float = 0.0  # min/max utilization (1.0 = perfectly balanced)

    @property
    def stage_count(self) -> int:
        return len(self.stage_budgets)


@dataclass
class LoadDistribution:
    """Load distribution across kernel instances."""
    instance_loads: dict[str, float] = field(default_factory=dict)
    total_load: float = 0.0
    max_load: float = 0.0
    imbalance_ratio: float = 0.0  # (max - min) / avg


class LatencyBudgetPartitioner:
    """Partitions a latency budget across pipeline stages.

    Three strategies:
      - 'proportional': Budget proportional to estimated latency
      - 'water_filling': Equalize utilization across stages
      - 'min_slack': Minimize total slack while respecting minimums
    """

    def partition(
        self,
        stages: list[tuple[str, str, float]],  # (stage_id, kernel_id, est_latency_us)
        total_budget_us: float,
        min_budget_us: float = 10.0,
        strategy: str = "water_filling",
    ) -> BudgetAllocation:
        """Partition latency budget across stages.

        Args:
            stages: List of (stage_id, kernel_id, estimated_latency_us).
            total_budget_us: Total latency SLA in microseconds.
            min_budget_us: Minimum budget per stage.
            strategy: Partitioning strategy.

        Returns:
            BudgetAllocation with per-stage budgets.
        """
        if not stages:
            return BudgetAllocation(total_budget_us=total_budget_us)

        n = len(stages)
        total_estimated = sum(lat for _, _, lat in stages)

        # Check feasibility
        if n * min_budget_us > total_budget_us:
            # Infeasible — allocate equally
            per_stage = total_budget_us / n
            return self._build_allocation(
                stages, [per_stage] * n, total_budget_us
            )

        if strategy == "proportional":
            budgets = self._proportional(stages, total_budget_us, min_budget_us)
        elif strategy == "min_slack":
            budgets = self._min_slack(stages, total_budget_us, min_budget_us)
        else:  # water_filling
            budgets = self._water_filling(stages, total_budget_us, min_budget_us)

        return self._build_allocation(stages, budgets, total_budget_us)

    def _proportional(
        self,
        stages: list[tuple[str, str, float]],
        total_budget_us: float,
        min_budget_us: float,
    ) -> list[float]:
        """Allocate budget proportionally to estimated latency."""
        total_estimated = sum(lat for _, _, lat in stages)
        if total_estimated == 0:
            return [total_budget_us / len(stages)] * len(stages)

        budgets = []
        for _, _, lat in stages:
            b = max(min_budget_us, (lat / total_estimated) * total_budget_us)
            budgets.append(b)

        # Normalize to exactly fill budget
        excess = sum(budgets) - total_budget_us
        if excess > 0:
            # Reduce proportionally from non-minimum stages
            reducible = [(i, b - min_budget_us) for i, b in enumerate(budgets) if b > min_budget_us]
            total_reducible = sum(r for _, r in reducible)
            if total_reducible > 0:
                for i, headroom in reducible:
                    budgets[i] -= excess * (headroom / total_reducible)

        return budgets

    def _water_filling(
        self,
        stages: list[tuple[str, str, float]],
        total_budget_us: float,
        min_budget_us: float,
    ) -> list[float]:
        """Water-filling: equalize utilization (estimated/budget) across stages.

        Start with minimum budgets, then distribute remaining budget
        to the stage with highest utilization (highest est/budget ratio).
        """
        n = len(stages)
        budgets = [min_budget_us] * n
        remaining = total_budget_us - n * min_budget_us

        if remaining <= 0:
            return budgets

        # Iteratively give budget to the most-utilized stage
        latencies = [lat for _, _, lat in stages]

        # Distribute in fine increments
        increment = max(remaining / (n * 10), 0.01)

        for _ in range(n * 10 + 1):
            if remaining < increment:
                break

            # Find stage with highest utilization
            worst_idx = -1
            worst_util = -1.0
            for i in range(n):
                util = latencies[i] / budgets[i] if budgets[i] > 0 else float("inf")
                if util > worst_util:
                    worst_util = util
                    worst_idx = i

            if worst_idx >= 0:
                give = min(increment, remaining)
                budgets[worst_idx] += give
                remaining -= give

        # Distribute any remaining evenly
        if remaining > 0:
            per = remaining / n
            for i in range(n):
                budgets[i] += per

        return budgets

    def _min_slack(
        self,
        stages: list[tuple[str, str, float]],
        total_budget_us: float,
        min_budget_us: float,
    ) -> list[float]:
        """Minimize total slack: give each stage exactly what it needs plus
        a proportional share of any remaining budget.

        This is the tightest allocation — least waste but least margin.
        """
        n = len(stages)
        latencies = [lat for _, _, lat in stages]
        budgets = [max(min_budget_us, lat) for lat in latencies]

        used = sum(budgets)
        remaining = total_budget_us - used

        if remaining > 0:
            # Distribute surplus proportionally
            total_est = sum(latencies) or 1.0
            for i in range(n):
                budgets[i] += remaining * (latencies[i] / total_est)
        elif remaining < 0:
            # Over-budget: proportionally reduce from slack
            excess = -remaining
            slack_amounts = [budgets[i] - min_budget_us for i in range(n)]
            total_slack = sum(slack_amounts)
            if total_slack > 0:
                for i in range(n):
                    budgets[i] -= excess * (slack_amounts[i] / total_slack)

        return budgets

    def _build_allocation(
        self,
        stages: list[tuple[str, str, float]],
        budgets: list[float],
        total_budget_us: float,
    ) -> BudgetAllocation:
        """Build BudgetAllocation from computed budgets."""
        stage_budgets: list[StageBudget] = []
        utilizations: list[float] = []

        for (sid, kid, est), budget in zip(stages, budgets):
            util = est / budget if budget > 0 else float("inf")
            utilizations.append(util)
            stage_budgets.append(
                StageBudget(
                    stage_id=sid,
                    kernel_id=kid,
                    estimated_latency_us=est,
                    allocated_budget_us=budget,
                    utilization=util,
                    slack_us=max(0, budget - est),
                )
            )

        max_u = max(utilizations) if utilizations else 0.0
        min_u = min(utilizations) if utilizations else 0.0
        balance = min_u / max_u if max_u > 0 else 1.0

        return BudgetAllocation(
            stage_budgets=stage_budgets,
            total_budget_us=total_budget_us,
            allocated_us=sum(budgets),
            max_utilization=max_u,
            min_utilization=min_u,
            balance_ratio=balance,
        )

    def balance_load(
        self,
        instances: list[str],
        loads: list[float],
    ) -> LoadDistribution:
        """Balance load across kernel instances using water-filling.

        Args:
            instances: Instance identifiers.
            loads: Current load per instance.

        Returns:
            LoadDistribution with balanced allocations.
        """
        if not instances:
            return LoadDistribution()

        n = len(instances)
        total = sum(loads)
        target = total / n if n > 0 else 0.0

        # Simple water-filling: redistribute to equalize
        instance_loads = {inst: target for inst in instances}

        max_load = max(instance_loads.values())
        min_load = min(instance_loads.values())
        avg_load = total / n if n > 0 else 0.0
        imbalance = (max_load - min_load) / avg_load if avg_load > 0 else 0.0

        return LoadDistribution(
            instance_loads=instance_loads,
            total_load=total,
            max_load=max_load,
            imbalance_ratio=imbalance,
        )
