# Pipeline Composition Extension

## Overview

The NPipeline.Extensions.Composition extension enables creating hierarchical, modular pipelines by treating entire pipelines as reusable transform nodes. This powerful capability allows you to build complex data processing workflows from simpler, well-tested building blocks.

## Key Features

- **Modular Design**: Break complex pipelines into smaller, reusable sub-pipelines
- **Type Safety**: Full compile-time type checking across pipeline boundaries
- **Context Control**: Fine-grained control over what data flows between parent and sub-pipelines
- **Isolation**: Sub-pipelines execute in isolated contexts, preventing unintended side effects
- **Nested Composition**: Unlimited nesting depth for hierarchical pipeline structures
- **High Performance**: Minimal overhead with shared runner and optimized context creation

## Installation

```bash
dotnet add package NPipeline.Extensions.Composition
```

## Quick Start

### Basic Composition

```csharp
using NPipeline.Extensions.Composition;
using NPipeline.Pipeline;

// Define a sub-pipeline for validation
public class ValidationPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<Customer>, Customer>("input");
        var validate = builder.AddTransform<ValidatorNode, Customer, ValidatedCustomer>("validate");
        var output = builder.AddSink<PipelineOutputSink<ValidatedCustomer>, ValidatedCustomer>("output");
        
        builder.Connect(input, validate);
        builder.Connect(validate, output);
    }
}

// Use in parent pipeline
public class DataProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<CustomerSource, Customer>("customers");
        
        // Add validation as a composite node
        var validate = builder.AddComposite<Customer, ValidatedCustomer, ValidationPipeline>("validate");
        
        var sink = builder.AddSink<DatabaseSink, ValidatedCustomer>("database");
        
        builder.Connect(source, validate);
        builder.Connect(validate, sink);
    }
}
```

## Core Concepts

### Composite Transform Node

A composite node is a special transform node that executes an entire sub-pipeline for each input item. It:

1. Receives an input item from the parent pipeline
2. Creates an isolated sub-pipeline context
3. Passes the input to the sub-pipeline
4. Executes the sub-pipeline
5. Retrieves the output from the sub-pipeline
6. Returns the output to the parent pipeline

### Sub-Pipeline Structure

Sub-pipelines must follow a specific structure:

- **Input**: Use `PipelineInputSource<T>` to receive data from the parent
- **Processing**: Use any standard NPipeline nodes (transforms, filters, etc.)
- **Output**: Use `PipelineOutputSink<T>` to return data to the parent

```csharp
public class MySubPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Input node - receives from parent
        var input = builder.AddSource<PipelineInputSource<TInput>, TInput>("input");
        
        // Processing nodes
        var transform = builder.AddTransform<MyTransform, TInput, TOutput>("process");
        
        // Output node - returns to parent
        var output = builder.AddSink<PipelineOutputSink<TOutput>, TOutput>("output");
        
        builder.Connect(input, transform);
        builder.Connect(transform, output);
    }
}
```

### Context Configuration

Control what data the sub-pipeline inherits from the parent:

```csharp
// No inheritance (default)
builder.AddComposite<TIn, TOut, SubPipeline>(
    contextConfiguration: CompositeContextConfiguration.Default);

// Inherit everything
builder.AddComposite<TIn, TOut, SubPipeline>(
    contextConfiguration: CompositeContextConfiguration.InheritAll);

// Custom inheritance
builder.AddComposite<TIn, TOut, SubPipeline>(
    contextConfiguration: new CompositeContextConfiguration
    {
        InheritParentParameters = true,
        InheritParentItems = false,
        InheritParentProperties = true
    });

// Using configuration action
builder.AddComposite<TIn, TOut, SubPipeline>(
    configureContext: config =>
    {
        config.InheritParentParameters = true;
        config.InheritParentItems = true;
    });
```

## Architecture

### Data Flow

```
Parent Pipeline:
  [Source] → [Composite Node] → [Sink]
                    ↓
            Sub-Pipeline:
              [PipelineInputSource] → [Transform] → [PipelineOutputSink]
```

### Context Isolation

Sub-pipelines execute in isolated contexts:

- **Isolated by Default**: Changes to sub-pipeline context don't affect parent
- **Optional Inheritance**: Parent context data can be copied to sub-pipeline
- **Thread-Safe**: Multiple composite nodes can execute concurrently
- **Resource Management**: Sub-pipeline resources are properly disposed

### Performance Characteristics

- **Single-Item Processing**: Each item is processed independently
- **Minimal Overhead**: Shared pipeline runner for all composite nodes
- **Memory Efficient**: Only input/output items in memory at once
- **No Buffering**: Items flow directly through the pipeline hierarchy

## Advanced Topics

See the following guides for detailed information:

- [Context Inheritance](context-inheritance.md) - Detailed guide on context configuration
- [Nested Composition](nested-composition.md) - Building deep pipeline hierarchies
- [Error Handling](error-handling.md) - Managing errors across pipeline boundaries
- [Performance Optimization](performance.md) - Best practices for high-performance scenarios
- [Testing Strategies](testing.md) - How to test composite pipelines effectively

## Examples

Complete examples are available in the [samples directory](../../../samples/Sample_Composition/):

- Basic composition with simple sub-pipelines
- Context inheritance patterns
- Nested composition scenarios
- Error handling across boundaries
- Complex multi-stage processing

## Best Practices

### 1. Keep Sub-Pipelines Focused

Each sub-pipeline should have a single, well-defined responsibility:

```csharp
✅ Good: ValidationPipeline, EnrichmentPipeline, TransformationPipeline
❌ Bad: DoEverythingPipeline
```

### 2. Use Meaningful Names

Name composite nodes and sub-pipelines descriptively:

```csharp
✅ Good:
builder.AddComposite<Customer, ValidatedCustomer, ValidationPipeline>("validate-customer");

❌ Bad:
builder.AddComposite<Customer, ValidatedCustomer, ValidationPipeline>("node1");
```

### 3. Minimize Context Inheritance

Only inherit what you need:

```csharp
✅ Good:
new CompositeContextConfiguration
{
    InheritParentParameters = true  // Only parameters needed
}

❌ Bad:
CompositeContextConfiguration.InheritAll  // Unless you really need everything
```

### 4. Test Sub-Pipelines Independently

Test each sub-pipeline in isolation before composing:

```csharp
[Fact]
public async Task ValidationPipeline_WithInvalidData_ShouldProduceErrors()
{
    var runner = PipelineRunner.Create();
    var context = new PipelineContext();
    
    // Test the sub-pipeline directly
    await runner.RunAsync<ValidationPipeline>(context);
    
    // Assert expected behavior
}
```

### 5. Document Input/Output Contracts

Clearly document what each sub-pipeline expects and produces:

```csharp
/// <summary>
/// Validates customer data and returns validation results.
/// </summary>
/// <remarks>
/// Input: Customer with Id, Name, Email
/// Output: ValidatedCustomer with IsValid flag and error list
/// </remarks>
public class ValidationPipeline : IPipelineDefinition
{
    // ...
}
```

## API Reference

### Extension Methods

#### `AddComposite<TIn, TOut, TDefinition>`

Adds a composite node to the pipeline.

```csharp
public static TransformNodeHandle<TIn, TOut> AddComposite<TIn, TOut, TDefinition>(
    this PipelineBuilder builder,
    string? name = null,
    CompositeContextConfiguration? contextConfiguration = null)
    where TDefinition : IPipelineDefinition, new()
```

**Parameters:**

- `builder`: The pipeline builder
- `name`: Optional node name (defaults to type name)
- `contextConfiguration`: Optional context configuration

**Returns:** Handle to the composite node

#### `AddComposite<TIn, TOut, TDefinition>` (with configuration action)

Adds a composite node with a configuration action.

```csharp
public static TransformNodeHandle<TIn, TOut> AddComposite<TIn, TOut, TDefinition>(
    this PipelineBuilder builder,
    Action<CompositeContextConfiguration> configureContext,
    string? name = null)
    where TDefinition : IPipelineDefinition, new()
```

**Parameters:**

- `builder`: The pipeline builder
- `configureContext`: Action to configure context inheritance
- `name`: Optional node name

**Returns:** Handle to the composite node

### Classes

#### `CompositeContextConfiguration`

Configuration for sub-pipeline context inheritance.

**Properties:**

- `InheritParentParameters`: Copy parent Parameters dictionary
- `InheritParentItems`: Copy parent Items dictionary
- `InheritParentProperties`: Copy parent Properties dictionary

**Static Properties:**

- `Default`: No inheritance (all flags false)
- `InheritAll`: Full inheritance (all flags true)

#### `PipelineInputSource<T>`

Source node that retrieves input from parent context.

**Type Parameters:**

- `T`: Type of input item

#### `PipelineOutputSink<T>`

Sink node that stores output in parent context.

**Type Parameters:**

- `T`: Type of output item

#### `CompositeContextKeys`

Well-known context keys for composite nodes.

**Constants:**

- `InputItem`: Key for input item storage
- `OutputItem`: Key for output item storage

## Troubleshooting

### Common Issues

#### "Sub-pipeline did not produce an output item"

**Cause:** Sub-pipeline is missing `PipelineOutputSink` or it received no data.

**Solution:** Ensure your sub-pipeline has:

1. A `PipelineOutputSink<T>` as the final node
2. Data flowing through the pipeline to the sink

```csharp
// ✅ Correct
public void Define(PipelineBuilder builder, PipelineContext context)
{
    var input = builder.AddSource<PipelineInputSource<T>, T>("input");
    var output = builder.AddSink<PipelineOutputSink<T>, T>("output");
    builder.Connect(input, output);
}
```

#### "No input item found in pipeline context"

**Cause:** Sub-pipeline is missing `PipelineInputSource` or accessing context incorrectly.

**Solution:** Always use `PipelineInputSource<T>` as the first node in sub-pipelines.

#### Type Mismatch Errors

**Cause:** Sub-pipeline output type doesn't match composite node's TOut type parameter.

**Solution:** Ensure type consistency:

```csharp
// ✅ Correct - types match
builder.AddComposite<Customer, ValidatedCustomer, ValidationPipeline>(...);

// In ValidationPipeline:
var output = builder.AddSink<PipelineOutputSink<ValidatedCustomer>, ValidatedCustomer>("output");
```
