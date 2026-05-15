---
title: "Composition"
description: "Build hierarchical pipelines by treating entire pipelines as transform nodes."
order: 4
---

# Composition

The `NPipeline.Extensions.Composition` package lets you treat an entire pipeline as a single transform node within a parent pipeline. This enables modular, hierarchical pipeline architectures where complex logic is encapsulated in reusable sub-pipelines.

## Installation

```bash
dotnet add package NPipeline.Extensions.Composition
```

## Quick Start

```csharp
// Parent pipeline
public class OrderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(new CsvSourceNode<RawOrder>(uri), "source");
        var enrich = builder.AddComposite<RawOrder, EnrichedOrder, EnrichmentPipeline>("enrich");
        var sink = builder.AddSink(new CsvSinkNode<EnrichedOrder>(outUri), "sink");

        builder.Connect(source, enrich);
        builder.Connect(enrich, sink);
    }
}

// Sub-pipeline (runs once per input item)
public class EnrichmentPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddCompositeInput<RawOrder>("input");
        var transform = builder.AddTransform<EnrichNode, RawOrder, EnrichedOrder>("enrich");
        var output = builder.AddCompositeOutput<EnrichedOrder>("output");

        builder.Connect(input, transform);
        builder.Connect(transform, output);
    }
}
```

## Key Types

### CompositeTransformNode\<TIn, TOut, TDefinition>

The core type — wraps a sub-pipeline as a `TransformNode<TIn, TOut>`. For each input item, it:

1. Creates an isolated child `PipelineContext`
2. Passes the input via `PipelineInputSource<TIn>`
3. Runs the sub-pipeline
4. Returns the output via `PipelineOutputSink<TOut>`

### PipelineInputSource\<T> / PipelineOutputSink\<T>

Special source/sink nodes used inside sub-pipelines. `PipelineInputSource<T>` reads the item passed from the parent. `PipelineOutputSink<T>` captures the result for the parent to collect.

### Node Kinds

| Kind | Description |
|------|-------------|
| `Composite` | The composite transform node in the parent pipeline |
| `CompositeInput` | Source node inside the sub-pipeline that receives parent input |
| `CompositeOutput` | Sink node inside the sub-pipeline that sends output to parent |

## Builder Extensions

```csharp
// Add composite node (using default context config)
builder.AddComposite<TIn, TOut, TDefinition>(name?, contextConfig?, serviceProvider?)

// Add composite node (configure context inline)
builder.AddComposite<TIn, TOut, TDefinition>(configureContext, name?, serviceProvider?)

// Inside sub-pipeline definitions
builder.AddCompositeInput<T>(name?)     // source that receives parent input
builder.AddCompositeOutput<T>(name?)    // sink that sends output to parent
```

## Context Inheritance

`CompositeContextConfiguration` controls what the child pipeline inherits from its parent. By default, sub-pipelines are **fully isolated** — they share observability but not data.

### Context Components

| Component | Content | Inherited by Default |
|-----------|---------|---------------------|
| **Parameters** | Configuration values, connection strings, processing settings | No |
| **Items** | Request-scoped state, temporary data, service instances | No |
| **Properties** | Metadata, environment settings, feature flags | No |
| **Run Identity** | Pipeline run tracking | Yes |
| **Lineage Sink** | Lineage tracking | Yes |
| **Execution Observer** | Metrics collection | Yes |
| **Dead Letter Decorator** | Error routing | Yes |

### Configuration Properties

| Property | Default | Description |
|----------|---------|-------------|
| `InheritParentParameters` | `false` | Pass parent parameters to child |
| `InheritParentItems` | `false` | Share parent context items |
| `InheritParentProperties` | `false` | Copy parent context properties |
| `InheritRunIdentity` | `true` | Share run identity |
| `InheritLineageSink` | `true` | Share lineage sink |
| `InheritExecutionObserver` | `true` | Share execution observer |
| `InheritDeadLetterDecorator` | `true` | Share dead letter sink |

### Presets

```csharp
// Default — inherits observability only (recommended)
builder.AddComposite<TIn, TOut, TDef>(
    contextConfiguration: CompositeContextConfiguration.Default);

// Inherit everything
builder.AddComposite<TIn, TOut, TDef>(
    contextConfiguration: CompositeContextConfiguration.InheritAll);

// Custom — selective inheritance
builder.AddComposite<TIn, TOut, TDef>(config =>
{
    config.InheritParentParameters = true;   // pass config down
    config.InheritParentItems = false;       // isolate state
});
```

### Isolation Guarantees

- The **parent context is never modified** by sub-pipeline execution
- When inheritance is enabled, the child receives **copies** — changes to child context don't affect the parent
- Context copying occurs at sub-pipeline creation time (before execution)

### Common Patterns

**Configuration inheritance** — pass connection strings and settings:

```csharp
config.InheritParentParameters = true;
```

**Isolated testing** — test sub-pipelines independently with no shared state:

```csharp
CompositeContextConfiguration.Default  // no data inheritance
```

## Error Handling

Errors in sub-pipelines follow NPipeline's standard model:

### Error Propagation

```
Sub-Pipeline Error → CompositeTransformNode → Parent Pipeline Error Handler
```

Unhandled exceptions in a sub-pipeline propagate to the composite node in the parent, which then follows the parent's error handling strategy (resilience policy, dead-letter, etc.).

### Strategies

**Catch in sub-pipeline** — handle expected errors internally:

```csharp
// Sub-pipeline with its own resilience policy
public class EnrichmentPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddCompositeInput<RawOrder>();
        var enrich = builder.AddTransform<EnrichNode, RawOrder, EnrichedOrder>();
        var output = builder.AddCompositeOutput<EnrichedOrder>();

        builder.Connect(input, enrich);
        builder.Connect(enrich, output);

        // Handle API failures within the sub-pipeline
        builder.WithResiliencePolicy(enrich, new RetryPolicy(maxRetries: 3));
    }
}
```

**Let errors propagate** — parent handles all errors:

```csharp
// Parent pipeline catches sub-pipeline failures
builder.WithResiliencePolicy(compositeNode, new RetryPolicy(maxRetries: 2));
```

**Hybrid** — handle expected errors in sub-pipeline, let critical ones propagate.

### Cancellation

Cancellation tokens propagate from parent to child. When the parent is cancelled, sub-pipelines are cancelled too. Always check the cancellation token in long-running sub-pipeline nodes.

## Nested Composition

Composite nodes can contain other composite nodes, creating multi-level hierarchies:

```csharp
// Level 1: Main pipeline
public class MainPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(...);
        var process = builder.AddComposite<Raw, Processed, ProcessingPipeline>("process");
        var sink = builder.AddSink(...);
        builder.Connect(source, process);
        builder.Connect(process, sink);
    }
}

// Level 2: Processing pipeline (itself contains a composite)
public class ProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddCompositeInput<Raw>();
        var validate = builder.AddComposite<Raw, Validated, ValidationPipeline>("validate");
        var transform = builder.AddTransform<TransformNode, Validated, Processed>();
        var output = builder.AddCompositeOutput<Processed>();
        builder.Connect(input, validate);
        builder.Connect(validate, transform);
        builder.Connect(transform, output);
    }
}
```

### Nesting Guidelines

| Depth | Overhead per Item | Recommendation |
|-------|-------------------|----------------|
| 1 level | ~2–3 μs | Common, recommended |
| 2 levels | ~4–6 μs | Good for layered architectures |
| 3+ levels | ~6+ μs | Consider flattening |

Context inheritance is configurable at each level — inner levels don't automatically inherit from outer levels.

## Performance

### Overhead Breakdown

| Component | Cost per Item |
|-----------|--------------|
| Context creation | ~1–2 μs |
| Input/output transfer | ~0.5 μs |
| **Total per level** | **~2–3 μs** |

### When to Use Composition

| Scenario | Recommendation |
|----------|----------------|
| Complex business logic needing modularity | ✅ Use composition |
| Low-throughput pipelines (< 1,000 items/sec) | ✅ Composition overhead negligible |
| Reusable sub-pipelines across multiple parents | ✅ Good fit |
| Ultra-high throughput (millions/sec) | ❌ Use flat pipeline |
| Simple linear processing | ❌ Unnecessary overhead |
| Deep nesting (5+ levels) | ❌ Consider flattening |

### Optimization Tips

1. **Minimize context inheritance** — `Default` (no data inheritance) is fastest
2. **Limit nesting depth** to 2–3 levels
3. **Reuse pipeline definitions** — avoid recreating definitions per item
4. **Use async operations** — non-blocking I/O in sub-pipelines

## Testing

### Test Sub-Pipelines Independently

```csharp
[Fact]
public async Task EnrichmentPipeline_EnrichesCorrectly()
{
    var result = await new PipelineTestHarness<EnrichmentPipeline>()
        .WithParameter(CompositeContextKeys.InputItem, new RawOrder { Id = 1 })
        .RunAsync();

    result.ShouldBeSuccessful();
}
```

### Test Parent with Mock Sub-Pipelines

Replace the composite node with a mock to test parent pipeline structure without running sub-pipelines.

### Test Context Inheritance

```csharp
[Fact]
public async Task SubPipeline_InheritsParameters_WhenConfigured()
{
    var result = await new PipelineTestHarness<ParentPipeline>()
        .WithParameter("connectionString", "Server=test;...")
        .RunAsync();

    // Verify sub-pipeline received the parameter
    result.ShouldBeSuccessful();
}
```

## Parent-Child Correlation

The composition extension automatically sets properties on the child context for observability:

| Key | Description |
|-----|-------------|
| `ParentNodeId` | ID of the composite node in the parent pipeline |
| `ParentPipelineId` | GUID of the parent pipeline execution |
| `ParentPipelineName` | Name of the parent pipeline |

Child graphs are accessible via `PipelineGraph.ChildGraphs` for inspection and tooling.

## Node ID Namespacing

Node IDs in sub-pipelines are automatically namespaced to avoid collisions. Use `CompositeNaming` helpers to work with namespaced IDs.

## Dependency Injection

```csharp
services.AddComposition();
```

When using DI, sub-pipeline definitions are resolved from the container. Set `fallbackToParameterlessWhenServiceMissing = true` to fall back to parameterless construction.

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| Sub-pipeline missing parameters | Context inheritance disabled | Enable `InheritParentParameters` |
| Parent context modified by child | Bug — should not happen | File an issue |
| High overhead per item | Deep nesting or heavy inheritance | Reduce nesting, use `Default` config |
| Sub-pipeline errors not handled | No resilience policy | Add policy on composite node or inside sub-pipeline |

## See Also

- [Pipeline Composition Guide](../guides/pipeline-composition.md) — step-by-step walkthrough
- [Extensions Overview](index.md)
