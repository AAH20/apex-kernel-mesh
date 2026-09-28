"""Apex Kernel Mesh — Federated Kernel Orchestration Hypervisor.

Composes N independent NP-hard solver kernels into a unified
computational platform via DAG-scheduled pipelines, submodular
cross-registry capability selection, and Byzantine-tolerant
circuit breaking.

Zero external dependencies. Pure Python 3.10+ standard library.
"""

__version__ = "0.1.0"

from .registry import KernelRegistry, KernelDescriptor, CapabilityDescriptor
from .router import CrossRegistryRouter
from .composer import DAGComposer, PipelineStage, ExecutionDAG
from .health import CircuitBreaker, HealthMonitor
from .executor import PipelineExecutor
from .balancer import LatencyBudgetPartitioner

__all__ = [
    "KernelRegistry",
    "KernelDescriptor",
    "CapabilityDescriptor",
    "CrossRegistryRouter",
    "DAGComposer",
    "PipelineStage",
    "ExecutionDAG",
    "CircuitBreaker",
    "HealthMonitor",
    "PipelineExecutor",
    "LatencyBudgetPartitioner",
]
