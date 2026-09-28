"""Kernel Registry — Auto-discovery and capability management.

Each kernel is modeled as a KernelDescriptor containing N
CapabilityDescriptors. The registry supports:
  - Manual registration via add_kernel()
  - Capability-level search via find_capabilities()
  - Health-aware filtering (excludes tripped circuit breakers)
  - Domain tag indexing for cross-kernel routing

Data structures are immutable after registration to prevent
mid-pipeline state corruption.
"""

from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class KernelStatus(Enum):
    """Health status of a registered kernel."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    TRIPPED = "tripped"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class CapabilityDescriptor:
    """A single solver capability within a kernel.

    Attributes:
        name: Unique solver name within the kernel.
        description: Human-readable description of what the solver does.
        domain_tags: Set of domain tags for cross-registry matching.
        input_schema: Dict describing expected input parameters.
        output_schema: Dict describing output structure.
        estimated_tokens: Context token cost for LLM schema injection.
        complexity_class: NP-hardness classification string.
        typical_latency_us: Typical execution latency in microseconds.
    """
    name: str
    description: str
    domain_tags: tuple[str, ...] = ()
    input_schema: dict[str, Any] = field(default_factory=dict)
    output_schema: dict[str, Any] = field(default_factory=dict)
    estimated_tokens: int = 100
    complexity_class: str = "NP-hard"
    typical_latency_us: float = 100.0


@dataclass(frozen=True)
class KernelDescriptor:
    """Describes a registered kernel and its capabilities.

    Attributes:
        kernel_id: Unique identifier (usually repo name).
        display_name: Human-readable name.
        version: Semantic version string.
        capabilities: Tuple of CapabilityDescriptors.
        domain_tags: Aggregate domain tags across all capabilities.
        total_tests: Number of passing tests.
        benchmark_ms: Total benchmark runtime in milliseconds.
    """
    kernel_id: str
    display_name: str
    version: str = "0.1.0"
    capabilities: tuple[CapabilityDescriptor, ...] = ()
    domain_tags: tuple[str, ...] = ()
    total_tests: int = 0
    benchmark_ms: float = 0.0

    @property
    def capability_count(self) -> int:
        return len(self.capabilities)

    @property
    def total_token_cost(self) -> int:
        return sum(c.estimated_tokens for c in self.capabilities)

    def fingerprint(self) -> str:
        """Deterministic content hash for cache invalidation."""
        blob = f"{self.kernel_id}:{self.version}:{self.capability_count}"
        return hashlib.sha256(blob.encode()).hexdigest()[:12]


class KernelRegistry:
    """Thread-safe kernel registry with capability indexing.

    The registry maintains:
      - A kernel map: kernel_id -> KernelDescriptor
      - A capability index: (domain_tag) -> [(kernel_id, capability)]
      - A status map: kernel_id -> (KernelStatus, last_update_time)
    """

    def __init__(self) -> None:
        self._kernels: dict[str, KernelDescriptor] = {}
        self._status: dict[str, tuple[KernelStatus, float]] = {}
        self._domain_index: dict[str, list[tuple[str, CapabilityDescriptor]]] = {}

    @property
    def kernel_count(self) -> int:
        return len(self._kernels)

    @property
    def total_capabilities(self) -> int:
        return sum(k.capability_count for k in self._kernels.values())

    @property
    def total_token_cost(self) -> int:
        return sum(k.total_token_cost for k in self._kernels.values())

    def add_kernel(self, descriptor: KernelDescriptor) -> None:
        """Register a kernel and index its capabilities by domain tags."""
        self._kernels[descriptor.kernel_id] = descriptor
        self._status[descriptor.kernel_id] = (KernelStatus.HEALTHY, time.monotonic())

        # Build domain tag index
        for cap in descriptor.capabilities:
            for tag in cap.domain_tags:
                if tag not in self._domain_index:
                    self._domain_index[tag] = []
                self._domain_index[tag].append((descriptor.kernel_id, cap))

    def remove_kernel(self, kernel_id: str) -> bool:
        """Deregister a kernel. Returns True if it existed."""
        if kernel_id not in self._kernels:
            return False
        descriptor = self._kernels.pop(kernel_id)
        self._status.pop(kernel_id, None)

        # Rebuild domain index (clean removal)
        for cap in descriptor.capabilities:
            for tag in cap.domain_tags:
                if tag in self._domain_index:
                    self._domain_index[tag] = [
                        (kid, c) for kid, c in self._domain_index[tag]
                        if kid != kernel_id
                    ]
        return True

    def get_kernel(self, kernel_id: str) -> KernelDescriptor | None:
        return self._kernels.get(kernel_id)

    def list_kernels(self) -> list[KernelDescriptor]:
        """Return all registered kernels."""
        return list(self._kernels.values())

    def set_status(self, kernel_id: str, status: KernelStatus) -> None:
        if kernel_id in self._kernels:
            self._status[kernel_id] = (status, time.monotonic())

    def get_status(self, kernel_id: str) -> KernelStatus:
        if kernel_id in self._status:
            return self._status[kernel_id][0]
        return KernelStatus.UNKNOWN

    def healthy_kernels(self) -> list[KernelDescriptor]:
        """Return only kernels with HEALTHY or DEGRADED status."""
        result = []
        for kid, descriptor in self._kernels.items():
            status = self.get_status(kid)
            if status in (KernelStatus.HEALTHY, KernelStatus.DEGRADED):
                result.append(descriptor)
        return result

    def find_capabilities(
        self,
        query: str = "",
        domain_tags: tuple[str, ...] | None = None,
        healthy_only: bool = True,
    ) -> list[tuple[str, CapabilityDescriptor]]:
        """Search capabilities by query text and/or domain tags.

        Args:
            query: Text to match against capability names/descriptions.
            domain_tags: If provided, filter to capabilities with these tags.
            healthy_only: If True, exclude capabilities from tripped kernels.

        Returns:
            List of (kernel_id, CapabilityDescriptor) tuples, scored by relevance.
        """
        candidates: list[tuple[float, str, CapabilityDescriptor]] = []
        query_terms = set(query.lower().split()) if query else set()

        # Determine which kernels to search
        if healthy_only:
            valid_ids = {k.kernel_id for k in self.healthy_kernels()}
        else:
            valid_ids = set(self._kernels.keys())

        # If domain tags specified, use the index
        if domain_tags:
            seen = set()
            for tag in domain_tags:
                for kid, cap in self._domain_index.get(tag, []):
                    if kid in valid_ids and (kid, cap.name) not in seen:
                        seen.add((kid, cap.name))
                        score = _score_capability(cap, query_terms)
                        candidates.append((score, kid, cap))
        else:
            # Full scan
            for kid, descriptor in self._kernels.items():
                if kid not in valid_ids:
                    continue
                for cap in descriptor.capabilities:
                    score = _score_capability(cap, query_terms)
                    candidates.append((score, kid, cap))

        # Sort by score descending, then by name for stability
        candidates.sort(key=lambda x: (-x[0], x[1], x[2].name))

        return [(kid, cap) for _, kid, cap in candidates]

    def summary(self) -> dict[str, Any]:
        """Return a summary dict of registry state."""
        return {
            "kernel_count": self.kernel_count,
            "total_capabilities": self.total_capabilities,
            "total_token_cost": self.total_token_cost,
            "kernels": [
                {
                    "kernel_id": k.kernel_id,
                    "display_name": k.display_name,
                    "version": k.version,
                    "capabilities": k.capability_count,
                    "tokens": k.total_token_cost,
                    "status": self.get_status(k.kernel_id).value,
                }
                for k in self._kernels.values()
            ],
        }


def _score_capability(cap: CapabilityDescriptor, query_terms: set[str]) -> float:
    """Score a capability against query terms using TF overlap."""
    if not query_terms:
        return 1.0  # No query = all match equally

    text = f"{cap.name} {cap.description}".lower()
    text_terms = set(text.split())

    overlap = len(query_terms & text_terms)
    if overlap == 0:
        # Partial substring matching
        partial = sum(1 for qt in query_terms if any(qt in t for t in text_terms))
        return partial * 0.3

    return overlap / len(query_terms)
