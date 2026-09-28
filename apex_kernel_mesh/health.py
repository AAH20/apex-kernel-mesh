"""In-process circuit-breaker state and a local majority-vote helper.

The vote helper compares supplied results in one process. It is not a
distributed consensus or Byzantine fault-tolerance protocol.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class BreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Configuration for a single circuit breaker.

    Attributes:
        failure_threshold: Number of failures before tripping.
        recovery_timeout_s: Seconds to wait before probing.
        half_open_max_calls: Max calls allowed in half-open state.
        success_threshold: Successes needed to close from half-open.
    """
    failure_threshold: int = 3
    recovery_timeout_s: float = 30.0
    half_open_max_calls: int = 1
    success_threshold: int = 2


@dataclass
class BreakerMetrics:
    """Runtime metrics for a circuit breaker."""
    total_calls: int = 0
    total_failures: int = 0
    total_successes: int = 0
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    last_failure_time: float = 0.0
    last_success_time: float = 0.0
    trip_count: int = 0
    half_open_calls: int = 0


class CircuitBreaker:
    """Per-kernel circuit breaker with exponential backoff recovery.

    Usage:
        breaker = CircuitBreaker("kernel-id")
        if breaker.allow_request():
            try:
                result = call_kernel(...)
                breaker.record_success()
            except Exception:
                breaker.record_failure()
        else:
            # Fast fail — kernel is tripped
            pass
    """

    def __init__(
        self,
        kernel_id: str,
        config: CircuitBreakerConfig | None = None,
    ) -> None:
        self.kernel_id = kernel_id
        self.config = config or CircuitBreakerConfig()
        self.state = BreakerState.CLOSED
        self.metrics = BreakerMetrics()
        self._last_trip_time: float = 0.0
        self._recovery_multiplier: int = 1

    def allow_request(self) -> bool:
        """Check if a request should be allowed through."""
        if self.state == BreakerState.CLOSED:
            return True

        if self.state == BreakerState.OPEN:
            # Check if recovery timeout has elapsed
            elapsed = time.monotonic() - self._last_trip_time
            timeout = self.config.recovery_timeout_s * self._recovery_multiplier
            if elapsed >= timeout:
                self.state = BreakerState.HALF_OPEN
                self.metrics.half_open_calls = 0
                return True
            return False

        if self.state == BreakerState.HALF_OPEN:
            return self.metrics.half_open_calls < self.config.half_open_max_calls

        return False

    def record_success(self) -> None:
        """Record a successful kernel call."""
        self.metrics.total_calls += 1
        self.metrics.total_successes += 1
        self.metrics.consecutive_successes += 1
        self.metrics.consecutive_failures = 0
        self.metrics.last_success_time = time.monotonic()

        if self.state == BreakerState.HALF_OPEN:
            self.metrics.half_open_calls += 1
            if self.metrics.consecutive_successes >= self.config.success_threshold:
                # Recovery confirmed — close the breaker
                self.state = BreakerState.CLOSED
                self._recovery_multiplier = 1

    def record_failure(self) -> None:
        """Record a failed kernel call."""
        self.metrics.total_calls += 1
        self.metrics.total_failures += 1
        self.metrics.consecutive_failures += 1
        self.metrics.consecutive_successes = 0
        self.metrics.last_failure_time = time.monotonic()

        if self.state == BreakerState.HALF_OPEN:
            # Failed during probe — re-trip with exponential backoff
            self.state = BreakerState.OPEN
            self._last_trip_time = time.monotonic()
            self._recovery_multiplier = min(self._recovery_multiplier * 2, 32)
            self.metrics.trip_count += 1
        elif self.state == BreakerState.CLOSED:
            if self.metrics.consecutive_failures >= self.config.failure_threshold:
                self._trip()

    def _trip(self) -> None:
        """Transition to OPEN state."""
        self.state = BreakerState.OPEN
        self._last_trip_time = time.monotonic()
        self.metrics.trip_count += 1

    def force_trip(self) -> None:
        """Manually trip the breaker (admin override)."""
        self._trip()

    def force_close(self) -> None:
        """Manually close the breaker (admin override)."""
        self.state = BreakerState.CLOSED
        self.metrics.consecutive_failures = 0
        self._recovery_multiplier = 1

    def summary(self) -> dict[str, Any]:
        return {
            "kernel_id": self.kernel_id,
            "state": self.state.value,
            "total_calls": self.metrics.total_calls,
            "failures": self.metrics.total_failures,
            "successes": self.metrics.total_successes,
            "trip_count": self.metrics.trip_count,
            "consecutive_failures": self.metrics.consecutive_failures,
        }


class HealthMonitor:
    """Manages circuit breakers across all registered kernels.

    Provides:
      - Automatic breaker creation per kernel
      - Aggregated health dashboards
      - Byzantine fault detection via N-of-M voting
    """

    def __init__(
        self,
        default_config: CircuitBreakerConfig | None = None,
    ) -> None:
        self._config = default_config or CircuitBreakerConfig()
        self._breakers: dict[str, CircuitBreaker] = {}

    def get_breaker(self, kernel_id: str) -> CircuitBreaker:
        """Get or create a circuit breaker for a kernel."""
        if kernel_id not in self._breakers:
            self._breakers[kernel_id] = CircuitBreaker(
                kernel_id, self._config
            )
        return self._breakers[kernel_id]

    def healthy_kernel_ids(self) -> list[str]:
        """Return kernel IDs with closed or half-open breakers."""
        return [
            kid
            for kid, b in self._breakers.items()
            if b.state != BreakerState.OPEN
        ]

    def tripped_kernel_ids(self) -> list[str]:
        """Return kernel IDs with open breakers."""
        return [
            kid
            for kid, b in self._breakers.items()
            if b.state == BreakerState.OPEN
        ]

    def dashboard(self) -> dict[str, Any]:
        """Return aggregated health dashboard."""
        total = len(self._breakers)
        healthy = len(self.healthy_kernel_ids())
        tripped = len(self.tripped_kernel_ids())
        return {
            "total_kernels": total,
            "healthy": healthy,
            "tripped": tripped,
            "availability_pct": (healthy / total * 100) if total > 0 else 100.0,
            "breakers": [b.summary() for b in self._breakers.values()],
        }

    def byzantine_vote(
        self,
        responses: list[tuple[str, Any]],
        quorum_fraction: float = 0.67,
    ) -> tuple[Any, list[str]]:
        """Byzantine fault detection via majority voting.

        Given responses from N replicated kernel calls, determines
        the consensus result and identifies disagreeing kernels.

        Args:
            responses: List of (kernel_id, result) tuples.
            quorum_fraction: Fraction needed for consensus (default 2/3).

        Returns:
            (consensus_result, list_of_disagreeing_kernel_ids)

        Raises:
            ValueError: If no quorum can be reached.
        """
        if not responses:
            raise ValueError("No responses to vote on")

        # Group by result (using repr for hashability)
        groups: dict[str, list[str]] = {}
        result_map: dict[str, Any] = {}

        for kid, result in responses:
            key = repr(result)
            if key not in groups:
                groups[key] = []
                result_map[key] = result
            groups[key].append(kid)

        # Find majority
        quorum_size = int(len(responses) * quorum_fraction)
        for key, voters in sorted(
            groups.items(), key=lambda x: len(x[1]), reverse=True
        ):
            if len(voters) >= quorum_size:
                disagreeing = [
                    kid
                    for other_key, kids in groups.items()
                    if other_key != key
                    for kid in kids
                ]
                return result_map[key], disagreeing

        # No quorum
        raise ValueError(
            f"No quorum reached: {len(responses)} responses, "
            f"need {quorum_size}, largest group has "
            f"{max(len(v) for v in groups.values())} members"
        )
