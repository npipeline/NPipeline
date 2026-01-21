---
title: Common Patterns
description: Practical patterns and recipes for building effective NPipeline pipelines.
sidebar_position: 12
---

## Common Patterns

This guide provides practical code recipes and examples for solving real-world scenarios with NPipeline. Each pattern demonstrates a complete, working solution you can adapt to your needs.

## Pattern 1: ETL Pipeline

Extract, Transform, Load pipelines are a fundamental use case for NPipeline. This pattern demonstrates reading data from a source, transforming it, and writing to a destination.

### Scenario (ETL Pipeline)

Extract customer data from a CSV file, enrich it with regional information, and load it into a database.

```csharp
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Connectors;
using NPipeline.Connectors.Csv;
using NPipeline.Nodes;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

// Define your data models
public sealed record RawCustomer(int Id, string Name, string Email, string City);
public sealed record EnrichedCustomer(int Id, string Name, string Email, string City, string Region);

public sealed class RegionEnricher : TransformNode<RawCustomer, EnrichedCustomer>
{
    private readonly Dictionary<string, string> _cityToRegion = new()
    {
        ["New York"] = "Northeast",
        ["Los Angeles"] = "West",
        ["Chicago"] = "Midwest",
        ["Houston"] = "South"
    };

    public override Task<EnrichedCustomer> ExecuteAsync(
        RawCustomer item,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        var region = _cityToRegion.TryGetValue(item.City, out var r) ? r : "Unknown";
        var enriched = new EnrichedCustomer(item.Id, item.Name, item.Email, item.City, region);
        return Task.FromResult(enriched);
    }
}

public sealed class DatabaseSink : SinkNode<EnrichedCustomer>
{
    public override async Task ExecuteAsync(
        IDataPipe<EnrichedCustomer> input,
        PipelineContext context,
        IPipelineActivity parentActivity,
        CancellationToken cancellationToken = default)
    {
        var customerCount = 0;
        await foreach (var customer in input.WithCancellation(cancellationToken))
        {
            // In real application, insert into database
            Console.WriteLine($"Saving: {customer.Name} ({customer.Region})");
            customerCount++;
        }
        Console.WriteLine($"Loaded {customerCount} customers");
    }
}

// Define the pipeline
public sealed class EtlPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(new CsvSourceNode<RawCustomer>(
            StorageUri.FromFilePath("customers.csv"),
            row => new RawCustomer(
                row.Get<int>("Id") ?? 0,
                row.Get<string>("Name") ?? string.Empty,
                row.Get<string>("Email") ?? string.Empty,
                row.Get<string>("City") ?? string.Empty)));
        var transform = builder.AddTransform<RegionEnricher, RawCustomer, EnrichedCustomer>();
        var sink = builder.AddSink<DatabaseSink, EnrichedCustomer>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

// Execute the pipeline
public static class Program
{
    public static async Task Main()
    {
        var runner = PipelineRunner.Create();
        await runner.RunAsync<EtlPipeline>();
    }
}
```

## Pattern 2: Data Validation with Error Handling

This pattern demonstrates validating data and routing invalid items to a separate error stream.

### Scenario (Validation)

Validate product prices and separate invalid items for review.

```csharp
public sealed record Product(int Id, string Name, decimal Price);
public sealed record ValidationError(int ProductId, string Reason);

public sealed class PriceValidator : TransformNode<Product, Product>
{
    public override Task<Product> ExecuteAsync(
        Product item,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        if (item.Price < 0)
            throw new InvalidOperationException($"Product {item.Id} has negative price: {item.Price}");

        if (item.Price > 100000)
            throw new InvalidOperationException($"Product {item.Id} has suspicious price: {item.Price}");

        return Task.FromResult(item);
    }
}

public sealed class ValidationPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<ProductSource, Product>();
        var validator = builder.AddTransform<PriceValidator, Product, Product>();
        var validSink = builder.AddSink<ValidProductSink, Product>();

        builder.Connect(source, validator);
        builder.Connect(validator, validSink);
    }
}
```

**Key Points:**

- Use try-catch in transforms to catch validation errors
- Implement error handlers at node level for fine-grained control
- Route errors to separate error sinks using multiple outputs (if supported)
- Log validation errors for audit and debugging

## Pattern 3: Branch (fan-out) Processing

Process data through multiple independent transformations in parallel.

### Scenario (Branching)

Calculate different metrics (sum, average, count) on the same data stream.

```csharp
public sealed record SalesData(int Id, decimal Amount);
public sealed record Metrics(decimal Total, decimal Average, int Count);

public sealed class MetricsAggregator : SinkNode<SalesData>
{
    private decimal _total;
    private int _count;

    public override async Task ExecuteAsync(
        IDataPipe<SalesData> input,
        PipelineContext context,
        IPipelineActivity parentActivity,
        CancellationToken cancellationToken = default)
    {
        _total = 0;
        _count = 0;

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            _total += item.Amount;
            _count++;
        }

        var average = _count > 0 ? _total / _count : 0;
        var metrics = new Metrics(_total, average, _count);
        Console.WriteLine($"Metrics: Total={metrics.Total}, Average={metrics.Average}, Count={metrics.Count}");
    }
}
```

**Key Points:**

- Create separate sink nodes for different outputs
- Use PipelineContext to pass shared state between nodes
- Consider using aggregation nodes for complex calculations

## Pattern 4: Batch Processing

Process data in batches rather than individual items.

### Scenario (Batching)

Save records in batches of 100 to improve database performance.

```csharp
public sealed class BatchDatabaseSink : SinkNode<Customer>
{
    private const int BatchSize = 100;

    public override async Task ExecuteAsync(
        IDataPipe<Customer> input,
        PipelineContext context,
        IPipelineActivity parentActivity,
        CancellationToken cancellationToken = default)
    {
        var batch = new List<Customer>(BatchSize);

        await foreach (var customer in input.WithCancellation(cancellationToken))
        {
            batch.Add(customer);

            if (batch.Count >= BatchSize)
            {
                await SaveBatchAsync(batch, cancellationToken);
                batch.Clear();
            }
        }

        // Save remaining items
        if (batch.Count > 0)
        {
            await SaveBatchAsync(batch, cancellationToken);
        }
    }

    private async Task SaveBatchAsync(List<Customer> batch, CancellationToken cancellationToken)
    {
        // In real application, save batch to database
        Console.WriteLine($"Saving batch of {batch.Count} customers");
        await Task.CompletedTask;
    }
}
```

## Pattern 5: Conditional Routing

Route items to different sinks based on conditions.

### Scenario (Conditional Routing)

Send high-value orders for expedited processing and normal orders to standard processing.

```csharp
public sealed record Order(int Id, decimal Total);

public sealed class OrderRouter : TransformNode<Order, Order>
{
    public override Task<Order> ExecuteAsync(
        Order item,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        // Mark the order with priority info in context
        var priority = item.Total > 1000 ? "High" : "Normal";
        Console.WriteLine($"Order {item.Id}: {priority} priority");
        return Task.FromResult(item);
    }
}
```

**Key Points:**

- Use context flags to mark items for different processing paths
- Implement logic in sink nodes to route based on item properties
- Consider multiple pipeline instances for complex routing scenarios

## Pattern 6: Data Merging

Merge data from multiple sources into a single stream.

### Scenario (Data Merging)

Combine customer data from multiple CSV files.

```csharp
public sealed class MultiSourcePipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Create multiple source nodes
        var source1 = builder.AddSource<CsvSource1, Customer>();
        var source2 = builder.AddSource<CsvSource2, Customer>();
        var source3 = builder.AddSource<CsvSource3, Customer>();

        // Create merge point
        var deduplicator = builder.AddTransform<CustomerDeduplicator, Customer, Customer>();
        var sink = builder.AddSink<MergedSink, Customer>();

        // Connect all sources to the same transform
        builder.Connect(source1, deduplicator);
        builder.Connect(source2, deduplicator);
        builder.Connect(source3, deduplicator);
        builder.Connect(deduplicator, sink);
    }
}
```

**Key Points:**

- Multiple sources can connect to the same transform
- Implement deduplication logic in the transform if needed
- Use PipelineContext to track which source items came from

## Best Practices

1. **Keep transforms focused** - Each transform should do one thing well
2. **Handle errors explicitly** - Use error handlers or separate error streams
3. **Monitor performance** - Profile your pipeline to identify bottlenecks
4. **Use dependency injection** - Inject services like loggers and databases
5. **Test each node** - Use InMemorySourceNode and InMemorySinkNode for testing
6. **Consider memory usage** - Stream data; don't load everything into memory
7. **Document assumptions** - Make clear what input data shapes each node expects

## Next Steps

- **[Dependency Injection](../extensions/dependency-injection.md)**: Manage complex dependencies pipeline components
- **[Parallelism](../extensions/parallelism/index.md)**: Speed up item processing
- **[Testing Pipelines](../extensions/testing/index.md)**: Test pipeline components
