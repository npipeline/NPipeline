---
title: Lookup Nodes
description: Enrich your data streams by performing lookups against external data sources using NPipeline's Lookup Nodes.
sidebar_position: 7
---

# Lookup Nodes

Lookup nodes are specialized transforms that enrich incoming data items by querying an external data source (e.g., a database, an in-memory cache, an API) to retrieve additional information. This is a common pattern in ETL and data processing pipelines where raw data needs to be augmented with reference data.

NPipeline provides an abstract [`LookupNode<TIn, TKey, TValue, TOut>`](src/NPipeline/Nodes/Lookup/LookupNode.cs) base class and a concrete [`InMemoryLookupNode<TIn, TKey, TValue, TOut>`](src/NPipeline/Nodes/Lookup/InMemoryLookupNode.cs) for in-memory lookups.

## `LookupNode<TIn, TKey, TValue, TOut>`

This abstract base class allows you to define custom lookup logic. You need to specify:

* `TIn`: The type of the input item to be enriched.
* `TKey`: The type of the key used to perform the lookup (extracted from `TIn`).
* `TValue`: The type of the value retrieved from the lookup source.
* `TOut`: The type of the enriched output item.

To implement a custom lookup, you typically override methods to:

* [`ExtractKey(TIn input, PipelineContext context)`](src/NPipeline/Nodes/Lookup/LookupNode.cs:22): Extracts the lookup key from the input item.
* [`LookupAsync(TKey key, PipelineContext context, CancellationToken cancellationToken)`](src/NPipeline/Nodes/Lookup/LookupNode.cs:33): Asynchronously performs the actual lookup operation and returns the `TValue`.
* [`CreateOutput(TIn input, TValue? lookupValue, PipelineContext context)`](src/NPipeline/Nodes/Lookup/LookupNode.cs:46): Combines the original input item with the retrieved lookup value to form the enriched output.

## `InMemoryLookupNode<TIn, TKey, TValue, TOut>`

`InMemoryLookupNode` is a concrete implementation of `LookupNode` that performs lookups against an in-memory dictionary. It's useful for small to medium-sized reference datasets that can be loaded entirely into memory.

### Example: Enriching Product Orders with Product Details

Let's say we have a stream of `OrderLine` items containing a `ProductId` and a static, in-memory collection of `Product` details. We want to enrich each `OrderLine` with the `ProductName`.

```csharp
using NPipeline;
using NPipeline.Nodes;

// Define input and lookup data structures
public sealed record OrderLine(int OrderLineId, int ProductId, int Quantity);
public sealed record Product(int ProductId, string ProductName, decimal UnitPrice);
public sealed record EnrichedOrderLine(int OrderLineId, int ProductId, string ProductName, int Quantity);

public sealed class ProductLookupNode : InMemoryLookupNode<OrderLine, int, Product, EnrichedOrderLine>
{
    public ProductLookupNode(IReadOnlyDictionary<int, Product> lookupData) : base(lookupData) { }

    protected override int ExtractKey(OrderLine input, PipelineContext context) => input.ProductId;

    protected override EnrichedOrderLine CreateOutput(OrderLine input, Product? lookupValue, PipelineContext context)
    {
        if (lookupValue is null)
        {
            Console.WriteLine($"Warning: Product with ID {input.ProductId} not found for OrderLine {input.OrderLineId}. Skipping enrichment.");
            return new EnrichedOrderLine(input.OrderLineId, input.ProductId, "Unknown Product", input.Quantity);
        }

        return new EnrichedOrderLine(
            input.OrderLineId,
            input.ProductId,
            lookupValue.ProductName,
            input.Quantity
        );
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        var productCatalog = new Dictionary<int, Product>
        {
            { 1, new Product(1, "Laptop", 1200.00m) },
            { 2, new Product(2, "Mouse", 25.00m) },
            { 3, new Product(3, "Keyboard", 75.00m) }
        };

        var orderLineSource = new InMemorySourceNode<OrderLine>(
            new OrderLine(1, 1, 1),
            new OrderLine(2, 3, 2),
            new OrderLine(3, 99, 1) // Product 99 does not exist
        );

        var context = PipelineContext.Default;
        var runner = PipelineRunner.Create();
        
        Console.WriteLine("Starting lookup pipeline...");
        await runner.RunAsync<LookupPipelineDefinition>(context);
        Console.WriteLine("Lookup pipeline finished.");
    }
}

public sealed class LookupPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceHandle = builder.AddSource<InMemorySourceNode<OrderLine>, OrderLine>("source");
        var transformHandle = builder.AddTransform<ProductLookupNode, OrderLine, EnrichedOrderLine>("lookup");
        var sinkHandle = builder.AddSink<ConsoleSink<EnrichedOrderLine>, EnrichedOrderLine>("sink");

        builder.Connect(sourceHandle, transformHandle);
        builder.Connect(transformHandle, sinkHandle);
    }
}
```

**Expected Output:**

```text
Starting lookup pipeline...
Sink received: EnrichedOrderLine { OrderLineId = 1, ProductId = 1, ProductName = "Laptop", Quantity = 1 }
Sink received: EnrichedOrderLine { OrderLineId = 2, ProductId = 3, ProductName = "Keyboard", Quantity = 2 }
Warning: Product with ID 99 not found for OrderLine 3. Skipping enrichment.
Sink received: EnrichedOrderLine { OrderLineId = 3, ProductId = 99, ProductName = "Unknown Product", Quantity = 1 }
Lookup pipeline finished.
```

## `PipelineBuilderLookupExtensions`

The [`PipelineBuilderLookupExtensions`](src/NPipeline/Pipeline/PipelineBuilderLookupExtensions.cs) provide convenient extension methods for integrating lookup nodes into your pipelines, often simplifying the syntax for common lookup scenarios.

```csharp
// Example using lookup extension
public sealed class LookupWithExtensionPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceHandle = builder.AddSource<CustomerIdSource, int>("source");
        var sinkHandle = builder.AddSink<EnrichedCustomerSink, EnrichedCustomer>("sink");

        // Use the lookup extension for concise syntax
        var lookupHandle = builder.AddLookup<int, EnrichedCustomer>("lookup", 
            keySelector: (customerId) => customerId,
            lookupFunction: async (key, ct) => await GetCustomerDetailsAsync(key, ct),
            combineFunction: (input, customerValue) => new EnrichedCustomer(input, customerValue)
        );

        builder.Connect(sourceHandle, lookupHandle);
        builder.Connect(lookupHandle, sinkHandle);
    }

    private async Task<CustomerDetails> GetCustomerDetailsAsync(int customerId, CancellationToken ct)
    {
        // Implementation
        return new CustomerDetails(customerId, "Customer Name");
    }
}
```

## Configuration Pattern

The lookup nodes use a configuration pattern where the lookup data, key extraction logic, and output creation logic are encapsulated in a configuration object. This pattern allows for flexible setup while maintaining clean separation of concerns.

```csharp
// The configuration pattern used internally
var config = new InMemoryLookupNode<TIn, TKey, TValue, TOut>.Configuration(
    lookupData: dictionary,           // The data to lookup against
    keyExtractor: input => input.Id,  // How to extract the key from input
    outputCreator: (input, value) =>  // How to create the output
        new EnrichedInput(input, value)
);
```

## Considerations for Lookup Nodes

* **Lookup Source Performance:** The performance of your lookup node is heavily dependent on the underlying lookup source. Optimize your data access (e.g., indexing, caching) for frequently accessed lookup data.
* **Memory vs. External Calls:** For large lookup datasets, consider whether an `InMemoryLookupNode` is feasible or if an external lookup (e.g., database query, API call) is more appropriate.
* **Error Handling:** Implement robust error handling for lookup failures (e.g., key not found, external service unavailable). Decide whether to skip the item, return a default value, or halt the pipeline.
* **Asynchronous Lookups:** Ensure your [`LookupAsync`](/docs-final/api/NPipeline.Nodes.Lookup.LookupNode#lookupAsync) implementation is truly asynchronous to avoid blocking the pipeline.

Lookup nodes are powerful for creating rich, contextual data streams within your NPipelines.

## Next Steps

* **[Branch Nodes](branch.md)**: Learn about duplicating data streams to multiple downstream paths.
