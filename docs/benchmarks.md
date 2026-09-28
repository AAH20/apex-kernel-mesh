# Benchmark methodology and interpretation

Run the synthetic microbenchmark with:

```bash
python -m apex_kernel_mesh.cli benchmark
```

The harness creates an in-memory sample registry, runs local capability search and planning operations, and reports timings from the current process. It does not connect to any external solver, MCP server, model provider, or production workload.

Results depend on Python version, hardware, operating-system load, and sample count. With the current small default sample, the displayed upper percentile is effectively the largest measured observation; it should not be interpreted as a statistically stable p99. The token-reduction figure compares estimated token costs for selected candidates against the benchmark's filtered candidate set, using metadata estimates rather than a tokenizer or provider billing data.

Treat output as a development smoke benchmark only. Before comparing releases or making performance claims, record the machine and runtime, use representative versioned datasets, warm up the process, collect enough independent repetitions for the desired percentile, report uncertainty, compare against a correctness-checked baseline, and benchmark real adapter end-to-end latency and cost separately.
