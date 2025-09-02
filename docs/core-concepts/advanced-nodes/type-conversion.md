---
title: Type Conversion Nodes
description: Convert data types seamlessly within your NPipeline using the fluent API design of Type Conversion Nodes.
sidebar_position: 6
---

# Type Conversion Nodes

In data pipelines, it's common to receive data in one format or type and need to convert it to another for downstream processing. NPipeline's Type Conversion Nodes streamline this process with a fluent API design that allows for flexible and robust type transformations between different stages of your pipeline.

## [`TypeConversionNode<TIn, TOut>`](src/NPipeline/Nodes/TypeConversion/TypeConversionNode.cs)

The [`TypeConversionNode<TIn, TOut>`](src/NPipeline/Nodes/TypeConversion/TypeConversionNode.cs) is a transform node designed to convert items from an input type `TIn` to an output type `TOut` using a fluent mapping API. It provides both automatic property mapping and explicit property mapping capabilities.

### Fluent API Design

The `TypeConversionNode` uses a fluent API that allows you to configure mappings through method chaining:

* [`Map()`](src/NPipeline/Nodes/TypeConversion/TypeConversionNode.cs:20): Explicitly map a source property to a destination property with optional conversion
* [`AutoMap()`](src/NPipeline/Nodes/TypeConversion/TypeConversionNode.cs:75): Automatically map properties with matching names (case-insensitive)

### Example: Basic Type Conversion with AutoMap

Let's say you have a stream of `SourceData` objects and need to convert them to `TargetData` objects:

```csharp
using NPipeline;
using NPipeline.Nodes;

// Define input and output types
public sealed record SourceData(string Name, string Value, int Count);
public sealed record TargetData(string Identifier, int NumericValue, string Description);

public sealed class SourceDataSource : SourceNode<SourceData>
{
    public async IAsyncEnumerable<SourceData> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        yield return new SourceData("Item1", "123", 5);
        yield return new SourceData("Item2", "456", 10);
        yield return new SourceData("Item3", "789", 15);
        await Task.CompletedTask;
    }
}

public sealed class TargetDataSink : SinkNode<TargetData>
{
    public async Task ExecuteAsync(IAsyncEnumerable<TargetData> input, CancellationToken cancellationToken = default)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Sink received: {item}");
        }
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        var context = PipelineContext.Default;
        var runner = new PipelineRunner();

        Console.WriteLine("Starting type conversion pipeline...");
        await runner.RunAsync<TypeConversionPipelineDefinition>(context);
        Console.WriteLine("Type conversion pipeline finished.");
    }
}

public sealed class TypeConversionPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceHandle = builder.AddSource<SourceDataSource, SourceData>("source");
        var transformHandle = builder.AddTransform<TypeConversionNode<SourceData, TargetData>, SourceData, TargetData>("conversion");
        var sinkHandle = builder.AddSink<TargetDataSink, TargetData>("sink");

        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);
    }
}
```

**Expected Output:**

```text
Starting type conversion pipeline...
Sink received: TargetData { Identifier = "Item1", NumericValue = 123, Description = "Processed: Item1" }
Sink received: TargetData { Identifier = "Item2", NumericValue = 456, Description = "Processed: Item2" }
Sink received: TargetData { Identifier = "Item3", NumericValue = 789, Description = "Processed: Item3" }
Type conversion pipeline finished.
```

### Mapping Methods

#### `AutoMap(StringComparer? comparer = null)`

Automatically maps properties from `TIn` to `TOut` by matching names (case-insensitive by default) and applying registered type converters.

* **comparer**: Optional string comparer for property name matching. Defaults to case-insensitive comparison.

```csharp
// AutoMap with custom comparer
var conversionNode = new TypeConversionNode<SourceData, TargetData>()
    .AutoMap(StringComparer.Ordinal); // Case-sensitive matching
```

#### `Map<TSrc, TDest>(Expression<Func<TIn, TSrc>> source, Expression<Func<TOut, TDest>> destination, Func<TSrc, TDest> convert)`

Maps a source property to a destination property using a converter that takes only the source value.

```csharp
// Simple property mapping with type conversion
.Map(src => src.Price, dest => dest.Cost, price => price * 0.9m) // Apply 10% discount
```

#### `Map<TSrc, TDest>(Expression<Func<TIn, TSrc>> source, Expression<Func<TOut, TDest>> destination, Func<TIn, TSrc, TDest> convert)`

Maps a source property to a destination property using a converter that can inspect the whole input object.

```csharp
// Complex mapping using entire input object
.Map(src => src.DiscountCode, dest => dest.FinalPrice,
    (input, code) => input.Price * GetDiscountMultiplier(code))
```

#### `Map<TDest>(Expression<Func<TOut, TDest>> destination, Func<TIn, TDest> convert)`

Maps a destination property using a converter over the whole input object (no single source property).

```csharp
// Generate a value based on multiple source properties
.Map(dest => dest.Summary, input => $"{input.Name}: {input.Count} items")
```

### Advanced Example: Complex Object Transformation

```csharp
// Source and target types with different structures
public sealed record OrderInput(
    string OrderId,
    string CustomerName,
    decimal Subtotal,
    string DiscountCode,
    DateTime OrderDate
);

public sealed record OrderOutput(
    string Id,
    CustomerInfo Customer,
    PricingInfo Pricing,
    DateTime ProcessedAt
);

public sealed record CustomerInfo(string Name, bool IsPremium);
public sealed record PricingInfo(decimal Subtotal, decimal Discount, decimal Total);

// Complex transformation using fluent API
var orderTransform = new TypeConversionNode<OrderInput, OrderOutput>()
    .Map(src => src.OrderId, dest => dest.Id, id => id)
    .Map(dest => dest.Customer, input =>
        new CustomerInfo(
            Name: input.CustomerName,
            IsPremium: IsPremiumCustomer(input.CustomerName)
        ))
    .Map(dest => dest.Pricing, input =>
        new PricingInfo(
            Subtotal: input.Subtotal,
            Discount: CalculateDiscount(input.DiscountCode, input.Subtotal),
            Total: CalculateTotal(input.Subtotal, input.DiscountCode)
        ))
    .Map(dest => dest.ProcessedAt, _ => DateTime.UtcNow);
```

### Error Handling

The `TypeConversionNode` handles conversion errors gracefully:

* Invalid type conversions throw exceptions that can be caught by pipeline error handling
* Missing mappings don't cause errors - only explicitly mapped properties are transformed
* You can provide custom error handling through the pipeline's error handling mechanisms

### Type Conversion Factory

The node internally uses [`TypeConverterFactory`](src/NPipeline/Nodes/TypeConversion/TypeConverterFactory.cs) to discover and create appropriate type converters for various type pairs. This factory handles the complexities of finding the right conversion logic for automatic mappings.

## Considerations for Type Conversion Nodes

* **Performance**: For high-throughput scenarios, the fluent mapping expressions are compiled to efficient delegates, but complex transformations may still impact performance.
* **Null Handling**: Be explicit about null handling in your conversion functions to avoid unexpected runtime errors.
* **Immutable Types**: The API works best with immutable types (like records) as it creates new instances for each transformation.
* **Circular References**: The node doesn't handle circular references in object graphs - avoid mapping types with circular references.

Type Conversion Nodes with their fluent API provide a powerful and flexible way to transform data between different types in your NPipelines.

## Next Steps

* **[Advanced Error Handling Patterns](../resilience/error-handling-guide.md#advanced-patterns)**: Learn more about handling errors, especially during conversions.

