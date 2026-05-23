---
title: "Build-Time Analyzers"
description: "Roslyn analyzers that catch pipeline bugs, performance issues, and configuration mistakes before your code runs."
order: 100
---

# Build-Time Analyzers

NPipeline ships with Roslyn analyzers that catch bugs **at build time** - before your pipeline ever runs. Misconfigured retry policies, blocking calls in async nodes, LINQ in hot paths, sinks that silently drop data - the analyzers flag all of these as compiler warnings or errors with automatic code fixes.

This is a key differentiator: most data pipeline libraries only fail at runtime. NPipeline catches entire categories of mistakes during `dotnet build`.

## Installation

The analyzers are distributed as separate NuGet packages and must be added explicitly:

```bash
dotnet add package NPipeline.Analyzers
```

Connector-specific analyzers are also separate packages:

```bash
dotnet add package NPipeline.Connectors.Postgres.Analyzers
dotnet add package NPipeline.Connectors.SqlServer.Analyzers
```

## NP90xx - Configuration and Setup

| Rule | Severity | Title | Fix |
|------|----------|-------|-----|
| NP9001 | Warning | RestartNode decision requires complete resilience configuration | Review resilience configuration when `ResilienceDecision.RestartNode` can be returned. Verify that `MaxItemRetries`, `MaxNodeRestartAttempts`, and `MaxMaterializedItems` are all properly configured. |
| NP9002 | **Error** | Unbounded materialization configuration | Set `MaxMaterializedItems` on `PipelineRetryOptions` to prevent out-of-memory crashes. |
| NP9003 | Warning | Inappropriate parallelism configuration | Reduce `DegreeOfParallelism` or disable `PreserveOrdering` when parallelism is high. |
| NP9004 | Warning | Batching configuration mismatch | Verify `BatchSize` and `BatchTimeout` are consistent with the node's expected throughput. |
| NP9005 | Warning | Inappropriate timeout configuration | Fix negative, zero, or excessively low timeout values. |

## NP91xx - Performance and Optimization

| Rule | Severity | Title | Fix |
|------|----------|-------|-----|
| NP9101 | Warning | Blocking calls in async methods | Replace `.Result`, `.Wait()`, `GetAwaiter().GetResult()`, `Thread.Sleep()` with async equivalents. |
| NP9102 | Warning | Synchronous over async anti-patterns | Avoid wrapping synchronous code in `Task.Run()` inside nodes. Use `ValueTask` fast paths instead. |
| NP9103 | Warning | LINQ operation detected in hot path | Replace LINQ (`Where`, `Select`, `ToList`) in `TransformAsync` with `foreach` loops to avoid allocations. |
| NP9104 | Warning | Inefficient string operation detected | Replace string concatenation with `+` in loops with `StringBuilder`. |
| NP9105 | Warning | Anonymous object allocation in hot path | Replace anonymous objects in `TransformAsync` with records or structs. |
| NP9106 | Info | Consider overriding ExecuteValueTaskAsync for synchronous operations | Override `ExecuteValueTaskAsync` when `TransformAsync` uses `Task.FromResult`. See [Synchronous Fast Paths](../performance/synchronous-fast-paths.md). |
| NP9107 | Warning | Use streaming patterns in SourceNode implementations | Return `new DataStream<T>(asyncEnumerable)` instead of materializing everything with `.ToList()`. |
| NP9108 | Info | Add parameterless constructor for better performance | Add a parameterless constructor for faster node activation. |

## NP92xx - Reliability and Error Handling

| Rule | Severity | Title | Fix |
|------|----------|-------|-----|
| NP9201 | Warning | Do not swallow OperationCanceledException | Don't catch and suppress `OperationCanceledException`; let it propagate for proper cancellation handling. |
| NP9202 | Warning | Inefficient exception handling | Avoid `catch (Exception)` with rethrow in tight loops. Use specific exception types. |
| NP9203 | Warning | Method should respect cancellation token | Pass `CancellationToken` to async methods that accept it. |

## NP93xx - Data Integrity and Correctness

| Rule | Severity | Title | Fix |
|------|----------|-------|-----|
| NP9301 | **Error** | SinkNode not consuming input | The `ConsumeAsync` method must use its `input` parameter. A sink that ignores its input is always a bug. |
| NP9302 | Warning | Unsafe PipelineContext access | Avoid concurrent writes to `PipelineContext.Items` from parallel nodes. Use thread-safe patterns. |

## NP94xx - Design and Architecture

| Rule | Severity | Title | Fix |
|------|----------|-------|-----|
| NP9401 | Info | Consider IStreamTransformNode | For transforms that operate on entire streams rather than individual items, implement `IStreamTransformNode<TIn, TOut>`. |
| NP9402 | Warning | IStreamTransformNode should use IStreamExecutionStrategy | `IStreamTransformNode` requires `IStreamExecutionStrategy`. Don't combine with per-item strategies. |
| NP9403 | Warning | Node missing public parameterless constructor | Node types resolved by the framework need a public parameterless constructor or DI registration. |
| NP9404 | Warning | Dependency injection anti-pattern | Avoid service locator patterns; use constructor injection instead. |

## NP95xx - Connector-Specific

| Rule | Severity | Package | Title | Fix |
|------|----------|---------|-------|-----|
| NP9501 | Warning | Postgres | PostgreSQL source with checkpointing requires ORDER BY clause | PostgreSQL sources using checkpointing must include an `ORDER BY` clause for deterministic replay. |
| NP9502 | Warning | SQL Server | SQL Server source with checkpointing requires ORDER BY clause | SQL Server sources using checkpointing must include an `ORDER BY` clause for deterministic replay. |

## Automatic Code Fixes

Most analyzer rules include automatic code fix providers. Apply fixes individually via the lightbulb menu, or use **Fix All** to batch-apply across a project.

### Key Code Fix Examples

**NP9002 - Add `MaxMaterializedItems`:**

```csharp
// Before
new PipelineRetryOptions(MaxItemRetries: 3, MaxNodeRestartAttempts: 3)

// After (code fix applied)
new PipelineRetryOptions(MaxItemRetries: 3, MaxNodeRestartAttempts: 3, MaxMaterializedItems: 10000)
```

**NP9101 - Replace blocking call with await:**

```csharp
// Before
var data = httpClient.GetAsync(url).Result;

// After
var data = await httpClient.GetAsync(url);
```

**NP9107 - Use streaming source:**

```csharp
// Before
public override IDataStream<Record> OpenStream(PipelineContext ctx, CancellationToken ct)
{
    var records = db.GetAll().ToList(); // NP9107 fires here — materializing into List
    return new InMemoryDataStream<Record>(records, "records");
}

// After (manually refactored — wrap the async enumerable directly)
public override IDataStream<Record> OpenStream(PipelineContext ctx, CancellationToken ct)
    => new DataStream<Record>(db.GetAllAsync(ct), "records");
```

**NP9301 - Consume sink input:**

```csharp
// Before
public override async Task ConsumeAsync(IDataStream<Order> input, PipelineContext ctx, CancellationToken ct)
{
    await db.SaveAsync(ct); // input never consumed — bug!
}

// After (manually implemented — always enumerate the input stream)
public override async Task ConsumeAsync(IDataStream<Order> input, PipelineContext ctx, CancellationToken ct)
{
    await foreach (var order in input.WithCancellation(ct))
    {
        await db.InsertAsync(order, ct);
    }
}
```

**NP9404 - Replace service locator with constructor injection:**

```csharp
// Before
public class MyNode : TransformNode<In, Out>
{
    public override Task<Out> TransformAsync(In item, PipelineContext ctx, CancellationToken ct)
    {
        var service = ctx.Properties["ServiceProvider"] as IServiceProvider;
        var dep = service.GetRequiredService<IMyService>();
        ...
    }
}

// After (manually refactored — use constructor injection)
public class MyNode : TransformNode<In, Out>
{
    private readonly IMyService _dep;

    public MyNode(IMyService dep) => _dep = dep;

    public override Task<Out> TransformAsync(In item, PipelineContext ctx, CancellationToken ct)
    {
        ...
    }
}
```

## Suppressing Rules

Suppress individual diagnostics when needed:

```csharp
#pragma warning disable NP9103 // LINQ is acceptable here - called once, not per-item
var lookup = items.ToDictionary(x => x.Id);
#pragma warning restore NP9103
```

Or in `.editorconfig`:

```ini
[*.cs]
dotnet_diagnostic.NP9106.severity = none  # disable ValueTask suggestion
```

## Next Steps

- [Error Codes](../reference/error-codes.md) - runtime error code catalog
- [Performance Best Practices](../performance/best-practices.md) - optimization guidance
- [Coding Conventions](../contributing/coding-conventions.md) - project coding standards
