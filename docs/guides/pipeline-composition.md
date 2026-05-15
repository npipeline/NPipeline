---
title: "Pipeline Composition"
description: "Embed reusable sub-pipelines as transform nodes for modular pipeline design."
order: 9
---

# Pipeline Composition

> **Prerequisites:** [Defining Pipelines](defining-pipelines.md), [Custom Nodes](custom-nodes.md)

Composition lets you embed a complete pipeline as a single transform [node](../reference/glossary.md#node) inside a parent pipeline. Each input item runs through the sub-pipeline and produces one output item.

## How Composition Works

A composite node wraps an `IPipelineDefinition`. For each item the parent pipeline sends to it:

1. The item is placed into a sub-pipeline context as the input
2. A `PipelineInputSource<T>` feeds the item into the sub-pipeline
3. The sub-pipeline executes its full graph
4. A `PipelineOutputSink<T>` captures the result
5. The result is returned to the parent pipeline

## Defining a Sub-Pipeline

A sub-pipeline is a regular `IPipelineDefinition` that uses `PipelineInputSource<T>` and `PipelineOutputSink<T>`:

```csharp
public class ValidationPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Customer>, Customer>("input");
        var validate = builder.AddTransform<CustomerValidator, Customer, ValidatedCustomer>("validate");
        var output = builder.AddSink<PipelineOutputSink<ValidatedCustomer>, ValidatedCustomer>("output");

        builder.Connect(input, validate);
        builder.Connect(validate, output);
    }
}
```

## Using a Composite Node

Register it in the parent pipeline with `AddComposite`:

```csharp
public class MainPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<CustomerSource, Customer>("customers");

        var validate = builder.AddComposite<Customer, ValidatedCustomer, ValidationPipeline>(
            "validation", CompositeContextConfiguration.Default);

        var enrich = builder.AddComposite<ValidatedCustomer, EnrichedCustomer, EnrichmentPipeline>(
            "enrichment", CompositeContextConfiguration.InheritAll);

        var sink = builder.AddSink<ConsoleSink<EnrichedCustomer>, EnrichedCustomer>("output");

        builder.Connect(source, validate);
        builder.Connect(validate, enrich);
        builder.Connect(enrich, sink);
    }
}
```

## Context Inheritance

`CompositeContextConfiguration` controls what the sub-pipeline inherits from the parent:

| Property | Default | `InheritAll` |
|----------|---------|-------------|
| `InheritParentParameters` | `false` | `true` |
| `InheritParentItems` | `false` | `true` |
| `InheritParentProperties` | `false` | `true` |
| `InheritRunIdentity` | `true` | `true` |
| `InheritLineageSink` | `true` | `true` |
| `InheritExecutionObserver` | `true` | `true` |
| `InheritDeadLetterDecorator` | `true` | `true` |

Use `CompositeContextConfiguration.Default` to isolate sub-pipelines. Use `InheritAll` when the sub-pipeline needs access to parent state.

> ⚠️ **Warning:** Inheriting `Items` or `Parameters` means the sub-pipeline can read and modify parent state. This creates coupling — use it deliberately.

## When to Use Composition

| Use Composition When | Use Separate Pipelines When |
|---------------------|----------------------------|
| Reusable validation/enrichment logic | Independent, parallel workflows |
| Complex transforms that are a pipeline in themselves | Different schedules or triggers |
| Clean separation of concerns within one pipeline | No data dependency between pipelines |

## Error Handling in Sub-Pipelines

Errors in sub-pipelines follow three strategies:

1. **Catch inside the sub-pipeline** — configure resilience in the sub-pipeline definition so errors don't reach the parent
2. **Propagate to parent** — let the sub-pipeline throw; the parent's error handler decides what to do with the composite node's failure
3. **Hybrid** — handle transient errors in the sub-pipeline (retries), propagate fatal errors to the parent

When `InheritDeadLetterDecorator` is `true` (the default), dead-lettered items from the sub-pipeline appear in the parent's dead letter queue.

## Nested Composition

Composite nodes can contain other composite nodes. Context flows through each level:

```
Parent Pipeline
  └─ CompositeNode (level 1)
       └─ CompositeNode (level 2)
            └─ Transform nodes
```

Each nesting level adds context creation overhead. Keep nesting to 2-3 levels for best performance.

## Performance Considerations

Each item creates a new `PipelineContext` and runs a complete sub-pipeline. Typical overhead per item:

- Context creation: ~1-2μs
- I/O transfer: ~0.5μs per level
- Total for a 2-level pipeline: ~3-5μs per item

This is negligible for I/O-bound pipelines (10K items/sec) but can become significant for high-throughput, CPU-bound workloads (1M+ items/sec). Use composition for logical modularity, not for performance.

**Optimization tips:**

- Minimize context inheritance — inheriting all parameters adds lookup cost proportional to parameter count
- Keep nesting shallow — each level adds ~2-3μs per item
- Use `CompositeContextConfiguration.Default` unless you need parent state

> 💡 **Tip:** Sub-pipeline definitions are resolved via `Activator.CreateInstance` by default. If your sub-pipeline has constructor dependencies, pass a `serviceProvider` to `AddComposite`.

## Testing Composite Pipelines

Test sub-pipelines independently before composing them:

```csharp
[Fact]
public async Task ValidationSubPipeline_RejectsInvalidCustomer()
{
    var result = await new PipelineTestHarness<ValidationPipeline>()
        .RunAsync();
    result.AssertSuccess();
}

[Fact]
public async Task FullPipeline_ComposesCorrectly()
{
    var result = await new PipelineTestHarness<MainPipeline>()
        .RunAsync();
    result.AssertSuccess();
}
```

## Next Steps

- [Composition Extension Reference](../extensions/composition.md) — context configuration, builder extensions, and parent-child correlation
- [Pipeline Context](pipeline-context.md) — how context flows between nodes and sub-pipelines
- [Dependency Injection](dependency-injection.md) — resolving sub-pipeline definitions from DI
- [Custom Nodes](custom-nodes.md) — simpler alternatives when full composition is overkill
