# Architecture and execution contract

Apex Kernel Mesh is currently a local Python prototype for composing capability metadata and planning small dependency graphs. It has no network, MCP, process, or remote-kernel transport implementation.

## Data flow

1. `KernelRegistry` stores kernel and capability metadata in memory and supports local search.
2. `CrossRegistryRouter` ranks matching metadata using string overlap and domain coverage, then filters candidates against estimated token and latency budgets. These estimates are metadata, not measured request costs.
3. `PipelineComposer` topologically orders stages and computes a critical path for a precedence DAG. It assumes unlimited resources; it does not solve resource-constrained job-shop scheduling.
4. `LatencyBudgetPartitioner` allocates a supplied total budget over stage estimates. Its output is an allocation proposal, not an SLA or latency prediction.
5. `PipelineExecutor` walks the planned stages sequentially. A registered Python handler can be invoked in-process. Without a handler, it returns a synthetic `status=simulated` result.
6. `HealthMonitor` maintains in-process circuit-breaker state and includes a majority-vote helper for supplied results. The vote helper is not a Byzantine fault-tolerant consensus protocol.

## Current boundaries

- Registry and routing state live in process memory; there is no persistence or distributed coordination.
- No MCP client/server, stdio, HTTP, RPC, sandbox, authentication, or authorization adapters are included.
- Pipeline execution is sequential, even when the DAG has independent stages. The reported parallelism factor is a DAG estimate, not observed speedup.
- Routing uses heuristic scores. The greedy strategy has no approximation guarantee claimed by this project. The `exact` strategy enumerates feasible subsets for a bounded candidate count; its worst-case work is exponential.
- The benchmark fixture is synthetic and measures local Python operations. Timings vary by machine and runtime and do not predict service performance.
- The package has zero runtime third-party dependencies. Building from source requires the build backend declared in `pyproject.toml`.

## Adapter boundary for future integrations

A future adapter should declare stable kernel/capability identifiers, input and output schemas, transport and timeout behavior, idempotency/retry semantics, health signals, and measured cost/latency provenance. Execution should distinguish planned, simulated, dispatched, succeeded, failed, and timed-out states. Remote integrations need explicit authentication, secret handling, cancellation, observability, and failure-isolation designs before production claims are made.
