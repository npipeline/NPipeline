---
title: "Frequently Asked Questions"
description: "Frequently asked questions about NPipeline design, deployment, and usage."
order: 102
---

# Frequently Asked Questions

## General

### What is NPipeline?

NPipeline is a composable data orchestration library for .NET. It processes data through connected nodes (sources, transforms, sinks) with built-in support for parallelism, resilience, observability, and extensibility.

### What .NET versions are supported?

NPipeline targets **.NET 8.0** and later. Use the latest LTS release for production.

### Is NPipeline free?

Yes, NPipeline is distributed under the [MIT License](https://github.com/Stewie435/NPipeline/blob/main/LICENSE).

### How does NPipeline compare to other tools?

| Feature | NPipeline | Apache Airflow | Azure Data Factory |
|---------|-----------|----------------|-------------------|
| Programming | C# code-first | Python DAGs | Visual designer |
| Infrastructure | In-process | Distributed | Managed cloud |
| Type safety | Strong | Dynamic | Limited |
| Unit testing | Simple | Requires framework | Complex |
| Cost | Free (MIT) | Free (self-hosted) | Per-operation |

Choose NPipeline when you build .NET applications, need lightweight in-process pipelines, and want strong type safety with code-based configuration.

## Deployment

### Can I use NPipeline in Azure Functions?

Yes, but consider startup time and memory constraints. Works well for moderate data volumes. Cold starts may be slow for large pipelines - pre-warm functions for production.

### Can I use NPipeline in AWS Lambda?

Possible for small, fast pipelines. Not ideal for long-running operations (15-minute timeout). Consider Step Functions for orchestration of larger workloads.

### Can I use NPipeline in Kubernetes?

Excellent choice. Deploy as containerized console apps with the Worker pattern:

```csharp
var host = Host.CreateDefaultBuilder()
    .ConfigureServices((ctx, services) =>
    {
        services.AddNPipeline(Assembly.GetExecutingAssembly());
    })
    .Build();

await host.RunAsync();
```

Horizontal scaling works naturally with multiple pod instances processing independent data partitions.

## Pipeline Design

### How many nodes should a pipeline have?

- **Sweet spot:** 3–10 nodes for most pipelines
- **Complex:** 10–50 nodes are manageable with good organization
- **50+:** Break into multiple pipelines using [composition](guides/pipeline-composition.md)

### Can I have multiple sources or sinks?

Yes. Connect multiple sources to downstream transforms, and fan out to multiple sinks:

```csharp
var source1 = builder.AddSource<DbSource, Order>("db");
var source2 = builder.AddSource<ApiSource, Order>("api");
var transform = builder.AddTransform<Enrich, Order, Order>("enrich");
var sink1 = builder.AddSink<FileSink, Order>("file");
var sink2 = builder.AddSink<DbSink, Order>("db-out");

builder.Connect(source1, transform);
builder.Connect(source2, transform);
builder.Connect(transform, sink1);
builder.Connect(transform, sink2);
```

### Should transforms be stateless?

**Stateless is preferred** - easier to test, thread-safe, and compatible with parallel execution. Stateful transforms (running totals, caches) are allowed but require synchronization if used with parallel execution. See [Thread Safety](guides/parallel-execution.md#thread-safety).

### When should I use composition vs a flat pipeline?

Use [composition](guides/pipeline-composition.md) when:

- You have reusable sub-workflows (validation, enrichment) shared across pipelines
- A section of your pipeline is complex enough to warrant independent testing
- You want clear separation of concerns

Use a flat pipeline when:

- The workflow is simple and linear
- Performance overhead of sub-pipeline context creation matters (high-throughput, CPU-bound)

## Performance

### How do I make my pipeline faster?

1. **Profile first** - enable [observability](observability/metrics-and-monitoring.md) to find the bottleneck
2. **Use parallel execution** for CPU-bound transforms - see [Parallel Execution](guides/parallel-execution.md)
3. **Stream data** - use `DataStream<T>` instead of materializing entire datasets
4. **Override `ExecuteValueTaskAsync`** - avoid Task allocations for synchronous transforms (see [Synchronous Fast Paths](performance/synchronous-fast-paths.md))
5. **Batch I/O operations** - use [batching](guides/batching-and-windowing.md) for database writes and API calls
6. **Avoid LINQ in hot paths** - analyzer NP9103 catches this

### How many pipelines can run concurrently?

Depends on resource usage per pipeline. Lightweight in-memory pipelines can run dozens to hundreds concurrently. I/O-heavy pipelines are typically limited by connection pool sizes and external service capacity.

### What if I only need to process data once?

Use a console app - no background service or long-lived host needed:

```csharp
var services = new ServiceCollection();
services.AddNPipeline(Assembly.GetExecutingAssembly());
var runner = services.BuildServiceProvider().GetRequiredService<IPipelineRunner>();
await runner.RunAsync<MyPipeline>(new PipelineContext());
```

## Next Steps

- [Your First Pipeline](getting-started/your-first-pipeline.md) - hands-on tutorial
- [Key Concepts](getting-started/key-concepts.md) - core model
- [Samples](samples/index.md) - runnable examples
