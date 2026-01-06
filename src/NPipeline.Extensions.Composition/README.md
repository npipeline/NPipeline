# NPipeline.Extensions.Composition

High-performance pipeline composition extension for NPipeline - enables treating entire pipelines as nodes within larger pipelines.

## Overview

The Composition extension provides the ability to create modular, hierarchical pipelines by treating complete pipelines as transform nodes. This enables:

- **Modular Design**: Break complex pipelines into reusable, composable components
- **Pipeline Hierarchies**: Nest pipelines within pipelines for better organization
- **Code Reuse**: Define sub-pipelines once and reuse them across multiple parent pipelines
- **Isolation**: Sub-pipelines have isolated contexts with optional parent inheritance
- **Type Safety**: Full compile-time type checking for all pipeline connections

## Installation

```bash
dotnet add package NPipeline.Extensions.Composition
```

## Quick Start

### Basic Composition

```csharp
using NPipeline.Extensions.Composition;

// Define a sub-pipeline for data enrichment
public class DataEnrichmentPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Customer>, Customer>("input");
        var enrich = builder.AddTransform<CustomerEnricher, Customer, EnrichedCustomer>("enrich");
        var output = builder.AddSink<PipelineOutputSink<EnrichedCustomer>, EnrichedCustomer>("output");

        builder.Connect(input, enrich);
        builder.Connect(enrich, output);
    }
}

// Use in parent pipeline
public class MainPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<CustomerSource, Customer>("customers");
        var composite = builder.AddComposite<Customer, EnrichedCustomer, DataEnrichmentPipeline>("enrichment");
        var sink = builder.AddSink<DatabaseSink, EnrichedCustomer>("database");

        builder.Connect(source, composite);
        builder.Connect(composite, sink);
    }
}
```

### Context Inheritance

Control what data from the parent pipeline context is inherited by the sub-pipeline:

```csharp
// Inherit all parent context data
var composite = builder.AddComposite<Customer, EnrichedCustomer, DataEnrichmentPipeline>(
    name: "enrichment",
    contextConfiguration: CompositeContextConfiguration.InheritAll);

// Custom inheritance
var composite = builder.AddComposite<Customer, EnrichedCustomer, DataEnrichmentPipeline>(
    configureContext: config =>
    {
        config.InheritParentParameters = true;
        config.InheritParentItems = false;
        config.InheritParentProperties = true;
    },
    name: "enrichment");
```

### Nested Composition

Composite nodes can contain other composite nodes, enabling deep pipeline hierarchies:

```csharp
public class ValidationEnrichmentPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Customer>, Customer>("input");

        // Use another composite node for validation
        var validate = builder.AddComposite<Customer, ValidatedCustomer, ValidationPipeline>("validation");

        var enrich = builder.AddTransform<CustomerEnricher, ValidatedCustomer, EnrichedCustomer>("enrich");
        var output = builder.AddSink<PipelineOutputSink<EnrichedCustomer>, EnrichedCustomer>("output");

        builder.Connect(input, validate);
        builder.Connect(validate, enrich);
        builder.Connect(enrich, output);
    }
}
```

## Core Components

### CompositeTransformNode<TIn, TOut, TDefinition>

The main transform node that executes a sub-pipeline for each input item.

- **TIn**: Input item type
- **TOut**: Output item type
- **TDefinition**: Sub-pipeline definition type (must implement `IPipelineDefinition`)

### PipelineInputSource\<T\>

A source node that retrieves input from the parent pipeline context via `CompositeContextKeys.InputItem`.

### PipelineOutputSink\<T\>

A sink node that captures output to the parent pipeline context via `CompositeContextKeys.OutputItem`.

### CompositeContextConfiguration

Configuration options for controlling sub-pipeline context inheritance:

- **InheritParentParameters**: Copy parent `PipelineContext.Parameters` to sub-context
- **InheritParentItems**: Copy parent `PipelineContext.Items` to sub-context
- **InheritParentProperties**: Copy parent `PipelineContext.Properties` to sub-context

### CompositeContextKeys

Well-known keys used for composite node context data:

- **InputItem**: Key for storing input item in sub-pipeline context
- **OutputItem**: Key for storing output item in sub-pipeline context

## Data Flow

The composite node processes data as follows:

1. Parent pipeline passes an item to the composite node
2. Composite node creates an isolated sub-pipeline context (optionally inheriting parent context data)
3. Input item is stored in sub-context via `CompositeContextKeys.InputItem`
4. `PipelineInputSource<T>` retrieves the input item
5. Sub-pipeline executes, processing the item through its nodes
6. `PipelineOutputSink<T>` captures the output item
7. Output item is stored in sub-context via `CompositeContextKeys.OutputItem`
8. Composite node retrieves and returns the output item to parent pipeline

## Error Handling

Composite nodes leverage NPipeline's built-in error handling:

- Sub-pipeline errors are handled by the sub-pipeline's error handler
- Composite node errors are handled by the parent pipeline's error handler
- Errors propagate through the standard NPipeline error handling chain
- No special error handling logic is required for composite nodes

## Performance Considerations

### Single-Item Transfer

Composite nodes process one item at a time, consistent with NPipeline's transform node pattern:

- **No Buffering**: Items are not buffered or accumulated
- **Minimal Memory**: Only input and output items exist in memory at any time
- **Predictable Execution**: Easy to reason about and debug
- **Same Performance**: Identical performance characteristics to other transform nodes

### Context Creation Overhead

The only per-item overhead is creating the sub-pipeline context:

- **Dictionary Copy**: Only occurs when inheritance is enabled
- **Zero-Copy Possible**: Disable inheritance for zero-copy overhead
- **Optimization**: Future versions may implement context pooling

## Dependency Injection

Register composition services with Microsoft.Extensions.DependencyInjection:

```csharp
using NPipeline.Extensions.Composition;

// Register composition services
var services = new ServiceCollection();
services.AddComposition();

// Use with NPipeline
services.AddNPipeline();

var serviceProvider = services.BuildServiceProvider();
```

## Advanced Usage

### Custom Context Keys

Extend `CompositeContextKeys` for custom context data:

```csharp
public static class CustomCompositeContextKeys
{
    public const string Metadata = "__Custom_Metadata";
    public const string Metrics = "__Custom_Metrics";
}
```

### Custom Input/Output Nodes

Implement custom nodes for specialized data transfer:

```csharp
public class CustomPipelineInputSource<T> : ISourceNode<T>
{
    public IDataPipe<T> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        // Custom input retrieval logic
    }
}

public class CustomPipelineOutputSink<T> : ISinkNode<T>
{
    public async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        // Custom output capture logic
    }
}
```

## Best Practices

1. **Keep Sub-Pipelines Focused**: Each sub-pipeline should have a single, well-defined responsibility
2. **Use Type Safety**: Leverage generic type parameters for compile-time verification
3. **Minimize Context Inheritance**: Only inherit parent context data when necessary
4. **Test Independently**: Test sub-pipelines in isolation before using them in composite nodes
5. **Document Sub-Pipelines**: Clearly document the input/output contracts of sub-pipelines

## Examples

See the [samples](../../samples/) directory for complete examples of composition usage:

- Basic composition with simple sub-pipelines
- Context inheritance patterns
- Nested composition scenarios
- Error handling in composite pipelines
- Performance optimization techniques

## License

MIT License - see [LICENSE](../../LICENSE) for details.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](../../CONTRIBUTING.md) for guidelines.
