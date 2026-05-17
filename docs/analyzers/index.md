---
title: "Build-Time Analyzers"
description: "Roslyn analyzers that catch pipeline bugs, performance issues, and configuration mistakes before your code runs."
order: 100
---

# Build-Time Analyzers

NPipeline ships with Roslyn analyzers that catch bugs **at build time** - before your pipeline ever runs. Misconfigured retry policies, blocking calls in async nodes, LINQ in hot paths, sinks that silently drop data - the analyzers flag all of these as compiler warnings or errors with automatic code fixes.

This is a key differentiator: most data pipeline libraries only fail at runtime. NPipeline catches entire categories of mistakes during `dotnet build`.

## Installation

The analyzers are included automatically when you reference `NPipeline`. Connector-specific analyzers ship with their respective packages.

## NP90xx - Configuration and Setup

| Rule | Severity | Title | Fix |
|------|----------|-------|-----|
| NP9001 | Warning | Resilient execution requires complete configuration | Ensure `MaxItemRetries`, `MaxNodeRestartAttempts`, and `MaxMaterializedItems` are all configured when using `RestartNode`. |
| NP9002 | **Error** | Unbounded materialization configuration | Set `MaxMaterializedItems` on `PipelineRetryOptions` to prevent out-of-memory crashes. |
| NP9003 | Warning | Inappropriate parallelism configuration | Reduce `DegreeOfParallelism` or disable `PreserveOrdering` when parallelism is high. |
| NP9004 | Warning | Batching configuration mismatch | Verify `BatchSize` and `BatchTimeout` are consistent with the node's expected throughput. |
| NP9005 | Warning | Inappropriate timeout configuration | Fix negative, zero, or excessively low timeout values. |

## NP91xx - Performance and Optimization

| Rule | Severity | Title | Fix |
|------|----------|-------|-----|
| NP9101 | Warning | Blocking calls in async methods | Replace `.Result`, `.Wait()`, `GetAwaiter().GetResult()`, `Thread.Sleep()` with async equivalents. |
| NP9102 | Warning | Synchronous over async anti-patterns | Avoid wrapping synchronous code in `Task.Run()` inside nodes. Use `ValueTask` fast paths instead. |
| NP9103 | Warning | LINQ in hot paths | Replace LINQ (`Where`, `Select`, `ToList`) in `TransformAsync` with `foreach` loops to avoid allocations. |
| NP9104 | Warning | Inefficient string operations | Replace string concatenation with `+` in loops with `StringBuilder`. |
| NP9105 | Warning | Anonymous object allocation in hot path | Replace anonymous objects in `TransformAsync` with records or structs. |
| NP9106 | Info | Missing ValueTask fast path | Override `ExecuteValueTaskAsync` when `TransformAsync` uses `Task.FromResult`. See [Synchronous Fast Paths](../performance/synchronous-fast-paths.md). |
| NP9107 | Warning | Non-streaming source node | Use `DataStream.FromAsyncEnumerable` instead of materializing all items in `OpenStream`. |
| NP9108 | Info | Missing parameterless constructor | Add a parameterless constructor for faster node activation. |

## NP92xx - Reliability and Error Handling

| Rule | Severity | Title | Fix |
|------|----------|-------|-----|
| NP9201 | Warning | Swallowing OperationCanceledException | Don't catch and suppress `OperationCanceledException`; let it propagate for proper cancellation handling. |
| NP9202 | Warning | Inefficient exception handling | Avoid `catch (Exception)` with rethrow in tight loops. Use specific exception types. |
| NP9203 | Warning | Ignoring CancellationToken | Pass `CancellationToken` to async methods that accept it. |

## NP93xx - Data Integrity and Correctness

| Rule | Severity | Title | Fix |
|------|----------|-------|-----|
| NP9301 | **Error** | SinkNode not consuming input | The `ConsumeAsync` method must use its `input` parameter. A sink that ignores its input is always a bug. |
| NP9302 | Warning | Unsafe PipelineContext access | Avoid concurrent writes to `PipelineContext.Items` from parallel nodes. Use thread-safe patterns. |

## NP94xx - Design and Architecture

| Rule | Severity | Title | Fix |
|------|----------|-------|-----|
| NP9401 | Info | Consider IStreamTransformNode | For transforms that operate on entire streams rather than individual items, implement `IStreamTransformNode<TIn, TOut>`. |
| NP9402 | Warning | Wrong execution strategy for IStreamTransformNode | `IStreamTransformNode` requires `IStreamExecutionStrategy`. Don't combine with per-item strategies. |
| NP9403 | Warning | Missing public parameterless constructor | Node types resolved by the framework need a public parameterless constructor or DI registration. |
| NP9404 | Warning | Dependency injection anti-pattern | Avoid service locator patterns; use constructor injection instead. |

## NP95xx - Connector-Specific

| Rule | Severity | Package | Title | Fix |
|------|----------|---------|-------|-----|
| NP9501 | Warning | Postgres | Missing ORDER BY with checkpointing | PostgreSQL sources using checkpointing must include an `ORDER BY` clause for deterministic replay. |
| NP9502 | Warning | SQL Server | Missing ORDER BY with checkpointing | SQL Server sources using checkpointing must include an `ORDER BY` clause for deterministic replay. |

## Automatic Code Fixes

Most analyzer rules include automatic code fix providers. Apply fixes individually via the lightbulb menu, or use **Fix All** to batch-apply across a project.

### Key Code Fix Examples

**NP9002 - Add `MaxMaterializedItems`:**

```csharp
// Before
new PipelineRetryOptions { MaxItemRetries = 3, MaxNodeRestartAttempts = 3 }

// After (code fix applied)
new PipelineRetryOptions { MaxItemRetries = 3, MaxNodeRestartAttempts = 3, MaxMaterializedItems = 10000 }
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
public override DataStream<Record> OpenStream(PipelineContext ctx, CancellationToken ct)
    => DataStream.FromEnumerable(db.GetAll().ToList());

// After
public override DataStream<Record> OpenStream(PipelineContext ctx, CancellationToken ct)
    => DataStream.FromAsyncEnumerable(db.GetAllAsync(ct));
```

**NP9301 - Consume sink input:**

```csharp
// Before
public override async Task ConsumeAsync(DataStream<Order> input, PipelineContext ctx, CancellationToken ct)
{
    await db.SaveAsync(ct); // input never consumed - silent data loss!
}

// After
public override async Task ConsumeAsync(DataStream<Order> input, PipelineContext ctx, CancellationToken ct)
{
    await foreach (var item in input.WithCancellation(ct))
        await db.InsertAsync(item, ct);
}
```

**NP9404 - Replace service locator with constructor injection:**

```csharp
// Before
public override Task<Out> TransformAsync(In item, PipelineContext ctx, CancellationToken ct)
{
    var service = ctx.Properties["ServiceProvider"] as IServiceProvider;
    var dep = service.GetRequiredService<IMyService>();
    ...
}

// After
private readonly IMyService _dep;
public MyNode(IMyService dep) => _dep = dep;
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
