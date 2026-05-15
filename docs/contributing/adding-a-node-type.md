---
title: "Adding a Node Type"
description: "Step-by-step guide to implementing, registering, and testing a new node."
order: 2
---

# Adding a Node Type

This guide walks through adding a new node to the NPipeline framework — from interface choice to tests.

## 1. Choose the Right Interface

| Interface | Use When | Key Method |
|-----------|----------|------------|
| `ISourceNode<TOut>` | Produces a data stream | `OpenStream(context, ct)` → `IDataStream<TOut>` |
| `ITransformNode<TIn, TOut>` | Maps one item to one output | `TransformAsync(item, context, ct)` → `Task<TOut>` |
| `IStreamTransformNode<TIn, TOut>` | Transforms the full stream (filtering, batching, windowing) | `TransformAsync(items, context, ct)` → `IAsyncEnumerable<TOut>` |
| `ISinkNode<TIn>` | Consumes a stream (final destination) | `ConsumeAsync(input, context, ct)` → `Task` |
| `ICustomMergeNode<TIn>` | Merges multiple upstream streams | `MergeAsync(pipes, ct)` → `Task<IDataStream<TIn>>` |

For specialized patterns, NPipeline also provides `IAggregateNode`, `IJoinNode`, and base classes like `LookupNode<TIn, TKey, TValue, TOut>` and `KeyedJoinNode<TKey, TIn1, TIn2, TOut>`.

## 2. Extend the Base Class

Always extend the base class rather than implementing the interface directly. Base classes provide `INodeTypeMetadata`, default disposal, and (for transforms) default `ExecutionStrategy`.

### Example: Transform Node

```csharp
public sealed class UpperCaseNode : TransformNode<string, string>
{
    public override Task<string> TransformAsync(
        string item, PipelineContext context, CancellationToken cancellationToken)
    {
        return Task.FromResult(item.ToUpperInvariant());
    }
}
```

### Example: Source Node

```csharp
public sealed class CounterSource : SourceNode<int>
{
    public override IDataStream<int> OpenStream(
        PipelineContext context, CancellationToken cancellationToken)
    {
        return new DataStream<int>(GenerateAsync(cancellationToken), "counter");
    }

    private static async IAsyncEnumerable<int> GenerateAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        for (var i = 0; i < 100; i++)
        {
            ct.ThrowIfCancellationRequested();
            yield return i;
        }
    }
}
```

### Example: Sink Node

```csharp
public sealed class ConsoleSink<T> : SinkNode<T>
{
    public override async Task ConsumeAsync(
        IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine(item);
        }
    }
}
```

## 3. Register in the Pipeline

```csharp
public class MyPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<CounterSource, int>("counter");
        var upper  = builder.AddTransform<UpperCaseNode, string, string>("upper");
        var sink   = builder.AddSink<ConsoleSink<string>, string>("console");

        builder.Connect(source, upper);
        builder.Connect(upper, sink);
    }
}
```

The `AddSource`, `AddTransform`, and `AddSink` methods create a `NodeDefinition` with the appropriate `NodeKind` and store it in the `PipelineGraph`.

## 4. NodeKind Reference

If you're adding a new node category to the framework itself (not just a user node), you need to add a value to `NodeKind`:

```csharp
public enum NodeKind
{
    Source,          Transform,       StreamTransform,
    Tap,             Branch,          Lookup,
    Batch,           Sink,            Join,
    Aggregate,       Composite,       CompositeInput,
    CompositeOutput,
}
```

You'll also need a corresponding `Add*` method on `PipelineBuilder` or an extension method, plus executor support in the orchestration layer.

## 5. Resource Disposal

Override `DisposeAsync()` if your node holds resources:

```csharp
public sealed class DbSink<T>(IDbConnection connection) : SinkNode<T>
{
    public override async Task ConsumeAsync(
        IDataStream<T> input, PipelineContext context, CancellationToken ct)
    {
        // consume items...
    }

    public override async ValueTask DisposeAsync()
    {
        if (connection is IAsyncDisposable disposable)
            await disposable.DisposeAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }
}
```

> ⚠️ **Warning:** Always call `base.DisposeAsync()` when overriding disposal.

## 6. Write Tests

Place tests in the corresponding test project. Follow the naming convention `MethodName_Condition_ExpectedBehavior`:

```csharp
public sealed class UpperCaseNodeTests
{
    [Fact]
    public async Task TransformAsync_LowercaseInput_ReturnsUppercase()
    {
        // Arrange
        var node = new UpperCaseNode();
        var context = new PipelineContext();
        
        // Act
        var result = await node.TransformAsync("hello", context, CancellationToken.None);
        
        // Assert
        result.Should().Be("HELLO");
    }
}
```

Use `AwesomeAssertions` (`.Should()`) for assertions and `FakeItEasy` (`A.Fake<T>()`) for mocking dependencies. See `tests/NPipeline.Tests.Common/` for shared helpers like `TransformNodeTestExtensions` and `InMemoryDataStream`.

## 7. Checklist

- [ ] Extends the appropriate base class (`SourceNode<T>`, `TransformNode<TIn, TOut>`, `SinkNode<T>`)
- [ ] Has a public parameterless constructor (or uses DI)
- [ ] Forwards `CancellationToken` to all async calls
- [ ] Calls `.WithCancellation(ct)` on `IAsyncEnumerable<T>` enumerations
- [ ] Overrides `DisposeAsync()` if holding resources
- [ ] Has XML documentation on public members
- [ ] Tests cover happy path, edge cases, and cancellation

## Next Steps

- [Adding a Connector](adding-a-connector.md) — package a node pair as a connector
- [Cancellation](../advanced-topics/cancellation.md) — cancellation token rules
- [Coding Conventions](coding-conventions.md) — style and analyzer compliance
