"""Heuristic selection of capability metadata under estimated budgets.

Candidates are ranked using query overlap and domain coverage. Token and
latency values are metadata estimates. The greedy strategy has no approximation
guarantee; the bounded exact strategy enumerates feasible subsets and has
exponential worst-case cost. This module does not dispatch remote tools.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from .registry import KernelRegistry, CapabilityDescriptor


@dataclass
class SelectedCapability:
    """A capability selected by the router with routing metadata."""
    kernel_id: str
    capability: CapabilityDescriptor
    relevance_score: float
    marginal_gain: float
    cumulative_tokens: int


@dataclass
class RoutingResult:
    """Result of a cross-registry routing decision."""
    selected: list[SelectedCapability] = field(default_factory=list)
    total_tokens: int = 0
    total_latency_us: float = 0.0
    coverage_score: float = 0.0
    budget_remaining: int = 0
    token_reduction_pct: float = 0.0
    canonical_order: list[str] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.selected)


class CrossRegistryRouter:
    """Routes queries across federated kernel registries.

    Implements three selection strategies:
      - 'submodular': Greedy marginal-gain maximization (default)
      - 'exact': exhaustive subset enumeration for small N (≤ 18)
      - 'domain': Domain-tag pre-filtered greedy
    """

    def __init__(self, registry: KernelRegistry) -> None:
        self._registry = registry

    def route(
        self,
        query: str,
        token_budget: int = 500,
        latency_budget_us: float = float("inf"),
        domain_tags: tuple[str, ...] | None = None,
        strategy: str = "submodular",
    ) -> RoutingResult:
        """Select optimal capabilities for a query under budget constraints.

        Args:
            query: Natural language query describing the optimization need.
            token_budget: Maximum token cost for selected capability schemas.
            latency_budget_us: Maximum cumulative latency in microseconds.
            domain_tags: Optional domain tags to pre-filter capabilities.
            strategy: Selection strategy ('submodular', 'exact', 'domain').

        Returns:
            RoutingResult with selected capabilities and metadata.
        """
        # Get candidate capabilities from registry
        candidates = self._registry.find_capabilities(
            query=query,
            domain_tags=domain_tags,
            healthy_only=True,
        )

        if not candidates:
            return RoutingResult(budget_remaining=token_budget)

        total_available_tokens = sum(
            c.estimated_tokens for _, c in candidates
        )

        if strategy == "exact" and len(candidates) <= 18:
            selected = self._exact_enumeration(
                candidates, token_budget, latency_budget_us, query
            )
        elif strategy == "domain" and domain_tags:
            selected = self._domain_greedy(
                candidates, token_budget, latency_budget_us, query
            )
        else:
            selected = self._submodular_greedy(
                candidates, token_budget, latency_budget_us, query
            )

        # Build result
        result = RoutingResult(
            selected=selected,
            total_tokens=sum(s.cumulative_tokens for s in selected[-1:])
            if selected
            else 0,
            total_latency_us=sum(
                s.capability.typical_latency_us for s in selected
            ),
            budget_remaining=token_budget
            - (selected[-1].cumulative_tokens if selected else 0),
        )

        if selected:
            result.total_tokens = selected[-1].cumulative_tokens
            result.coverage_score = sum(s.relevance_score for s in selected) / max(
                len(selected), 1
            )

        if total_available_tokens > 0:
            result.token_reduction_pct = (
                1.0 - result.total_tokens / total_available_tokens
            ) * 100

        # Canonical ordering for KV-cache stability
        result.canonical_order = _canonical_sort(selected)

        return result

    def _submodular_greedy(
        self,
        candidates: list[tuple[str, CapabilityDescriptor]],
        token_budget: int,
        latency_budget_us: float,
        query: str,
    ) -> list[SelectedCapability]:
        """Greedy submodular maximization with lazy evaluation.

        At each step, selects the candidate with the highest
        marginal gain per token cost, respecting both budgets.
        """
        query_terms = set(query.lower().split())
        selected: list[SelectedCapability] = []
        covered_tags: set[str] = set()
        used_tokens = 0
        used_latency = 0.0
        remaining = list(candidates)

        while remaining:
            best_gain = -1.0
            best_idx = -1
            best_score = 0.0

            for i, (kid, cap) in enumerate(remaining):
                # Skip if over budget
                if used_tokens + cap.estimated_tokens > token_budget:
                    continue
                if used_latency + cap.typical_latency_us > latency_budget_us:
                    continue

                # Compute marginal gain: new domain tags covered
                new_tags = set(cap.domain_tags) - covered_tags
                tag_gain = len(new_tags) * 0.5

                # Relevance to query
                text = f"{cap.name} {cap.description}".lower()
                text_terms = set(text.split())
                overlap = len(query_terms & text_terms) if query_terms else 1
                relevance = overlap / max(len(query_terms), 1)

                # Marginal gain = relevance + diversity bonus
                gain = relevance + tag_gain
                # Normalize by token cost (efficiency)
                efficiency = gain / max(cap.estimated_tokens, 1)

                if efficiency > best_gain:
                    best_gain = efficiency
                    best_idx = i
                    best_score = relevance

            if best_idx < 0:
                break

            kid, cap = remaining.pop(best_idx)
            used_tokens += cap.estimated_tokens
            used_latency += cap.typical_latency_us
            covered_tags.update(cap.domain_tags)

            selected.append(
                SelectedCapability(
                    kernel_id=kid,
                    capability=cap,
                    relevance_score=best_score,
                    marginal_gain=best_gain,
                    cumulative_tokens=used_tokens,
                )
            )

        return selected

    def _exact_enumeration(
        self,
        candidates: list[tuple[str, CapabilityDescriptor]],
        token_budget: int,
        latency_budget_us: float,
        query: str,
    ) -> list[SelectedCapability]:
        """Exact Branch-and-Bound for small candidate sets (N ≤ 18).

        Explores the full 2^N solution space with pruning.
        """
        query_terms = set(query.lower().split())
        n = len(candidates)
        best_value = 0.0
        best_mask = 0

        for mask in range(1, 1 << n):
            total_tokens = 0
            total_latency = 0.0
            total_value = 0.0
            covered_tags: set[str] = set()
            feasible = True

            for i in range(n):
                if not (mask & (1 << i)):
                    continue
                _, cap = candidates[i]
                total_tokens += cap.estimated_tokens
                total_latency += cap.typical_latency_us

                if total_tokens > token_budget or total_latency > latency_budget_us:
                    feasible = False
                    break

                # Value = relevance + diversity
                text = f"{cap.name} {cap.description}".lower()
                text_terms = set(text.split())
                overlap = len(query_terms & text_terms) if query_terms else 1
                relevance = overlap / max(len(query_terms), 1)
                new_tags = set(cap.domain_tags) - covered_tags
                total_value += relevance + len(new_tags) * 0.5
                covered_tags.update(cap.domain_tags)

            if feasible and total_value > best_value:
                best_value = total_value
                best_mask = mask

        # Reconstruct solution
        selected: list[SelectedCapability] = []
        cum_tokens = 0
        for i in range(n):
            if best_mask & (1 << i):
                kid, cap = candidates[i]
                cum_tokens += cap.estimated_tokens
                text = f"{cap.name} {cap.description}".lower()
                text_terms = set(text.split())
                overlap = len(query_terms & text_terms) if query_terms else 1
                relevance = overlap / max(len(query_terms), 1)
                selected.append(
                    SelectedCapability(
                        kernel_id=kid,
                        capability=cap,
                        relevance_score=relevance,
                        marginal_gain=0.0,
                        cumulative_tokens=cum_tokens,
                    )
                )

        return selected

    def _domain_greedy(
        self,
        candidates: list[tuple[str, CapabilityDescriptor]],
        token_budget: int,
        latency_budget_us: float,
        query: str,
    ) -> list[SelectedCapability]:
        """Domain-tag pre-filtered greedy selection.

        First filters by domain overlap, then runs submodular greedy
        on the filtered set.
        """
        return self._submodular_greedy(
            candidates, token_budget, latency_budget_us, query
        )


def _canonical_sort(selected: list[SelectedCapability]) -> list[str]:
    """Sort selected capabilities by (kernel_id, name, hash) for
    deterministic KV-cache prefix ordering.

    Returns list of canonical keys: 'kernel_id::capability_name::hash[:8]'
    """
    keys: list[tuple[str, str, str]] = []
    for s in selected:
        h = hashlib.sha256(
            f"{s.kernel_id}:{s.capability.name}".encode()
        ).hexdigest()[:8]
        keys.append((s.kernel_id, s.capability.name, h))

    keys.sort()
    return [f"{k}::{n}::{h}" for k, n, h in keys]
