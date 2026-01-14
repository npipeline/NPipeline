---
title: Best Practices
description: Design principles and recommendations for building robust NPipeline pipelines.
sidebar_position: 11
---

# Best Practices

This guide provides design principles and recommendations for building robust, maintainable, and performant NPipeline pipelines.

> **Looking for implementation examples?** See [Common Patterns](common-patterns.md) for practical code recipes demonstrating these principles in action.

> **Principle-focused.** This guide answers the "why" - the reasoning and principles behind building good pipelines.  
> **Implementation-focused** guides like [Common Patterns](common-patterns.md) show you the "how" with working code examples.

## Principle 1: Single Responsibility

Each node should have a single, well-defined responsibility. This makes nodes:

- Easier to understand and maintain
- More reusable across pipelines
- Simpler to test

### Good Example

```csharp
// Focused transform that only validates prices
public sealed class PriceValidator : TransformNode<Product, Product>
{
    public override Task<Product> ExecuteAsync(Product item, PipelineContext context, CancellationToken cancellationToken)
    {
        if (item.Price < 0)
            throw new InvalidOperationException("Price cannot be negative");
        return Task.FromResult(item);
    }
}

// Separate transform for tax calculations
public sealed class TaxCalculator : TransformNode<Product, Product>
{
    public override Task<Product> ExecuteAsync(Product item, PipelineContext context, CancellationToken cancellationToken)
    {
        item.Tax = item.Price * 0.08m;
        return Task.FromResult(item);
    }
}
```

### Avoid: God Nodes

```csharp
// BAD: Node doing validation, tax calculation, formatting, and logging
public sealed class MegaTransform : TransformNode<Product, Product>
{
    public override async Task<Product> ExecuteAsync(Product item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Validation logic
        if (item.Price < 0) throw new InvalidOperationException();

        // Tax calculation
        item.Tax = item.Price * 0.08m;

        // Formatting
        item.FormattedName = FormatName(item.Name);

        // Logging
        await LogAsync(item);

        return item;
    }
}
```

## Principle 2: Dependency Injection

Use dependency injection to manage external dependencies like services, loggers, and configuration.

### Good Example

```csharp
public sealed class EnrichedTransform : TransformNode<Customer, EnrichedCustomer>
{
    private readonly IEmailService _emailService;
    private readonly ILogger<EnrichedTransform> _logger;

    // Dependencies injected via constructor
    public EnrichedTransform(IEmailService emailService, ILogger<EnrichedTransform> logger)
    {
        _emailService = emailService;
        _logger = logger;
    }

    public override async Task<EnrichedCustomer> ExecuteAsync(Customer item, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation($"Processing customer: {item.Id}");
        var isValid = await _emailService.ValidateEmailAsync(item.Email, cancellationToken);
        return new EnrichedCustomer(item.Id, item.Name, item.Email, isValid);
    }
}

// Register with Dependency Injection (DI) container
var services = new ServiceCollection();
services.AddNPipeline(Assembly.GetExecutingAssembly());
services.AddSingleton<IEmailService, EmailService>();
services.AddLogging();
```

## Principle 3: Handle Errors Explicitly

Don't let errors propagate silently. Handle them explicitly or route them appropriately.

### Good Example

```csharp
public sealed class ResilientTransform : TransformNode<Order, ProcessedOrder>
{
    public override async Task<ProcessedOrder> ExecuteAsync(Order item, PipelineContext context, CancellationToken cancellationToken)
    {
        try
        {
            // Attempt the operation
            return await ProcessOrderAsync(item, cancellationToken);
        }
        catch (OperationCanceledException)
        {
            // Re-throw cancellation - don't swallow it
            throw;
        }
        catch (InvalidOperationException ex)
        {
            // Log validation errors
            Console.WriteLine($"Validation failed for order {item.Id}: {ex.Message}");
            throw;
        }
        catch (Exception ex)
        {
            // Log unexpected errors
            Console.WriteLine($"Unexpected error processing order {item.Id}: {ex}");
            throw;
        }
    }

    private async Task<ProcessedOrder> ProcessOrderAsync(Order item, CancellationToken cancellationToken)
    {
        // Actual processing logic
        await Task.CompletedTask;
        return new ProcessedOrder(item.Id, item.Total);
    }
}
```

## Principle 4: Stream Data Efficiently

Process data as it flows; don't load entire datasets into memory.

### Good Example

```csharp
public sealed class StreamingSourceNode : SourceNode<Customer>
{
    public override IDataPipe<Customer> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        static IAsyncEnumerable<Customer> GetCustomersAsync(string connectionString, CancellationToken ct)
        {
            return Get();

            // Use async enumerable to stream data from database
            async IAsyncEnumerable<Customer> Get()
            {
                using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync(ct);

                using var command = connection.CreateCommand();
                command.CommandText = "SELECT * FROM Customers";

                using var reader = await command.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    yield return new Customer(
                        (int)reader["Id"],
                        (string)reader["Name"]);
                }
            }
        }

        return new StreamingDataPipe<Customer>(
            GetCustomersAsync(_connectionString, cancellationToken));
    }
}
```

### Avoid: Loading All Data Into Memory

```csharp
// BAD: Loading all data before streaming
public sealed class BadSourceNode : SourceNode<Customer>
{
    public override IDataPipe<Customer> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        // This loads ALL customers into memory at once!
        var allCustomers = new List<Customer>();
        using var connection = new SqlConnection(_connectionString);
        // Note: This is still BAD synchronously - don't block on async I/O!
        // connection.Open();

        // ... read all customers into list ...

        return new StreamingDataPipe<Customer>(
            allCustomers.ToAsyncEnumerable());
    }
}
```

## Principle 5: Make Nodes Testable

Design nodes that are easy to unit test using NPipeline's testing utilities.

### Good Example

```csharp
public sealed class DiscountCalculator : TransformNode<Order, Order>
{
    private readonly decimal _discountRate;

    public DiscountCalculator(decimal discountRate = 0.1m)
    {
        _discountRate = discountRate;
    }

    public override Task<Order> ExecuteAsync(Order item, PipelineContext context, CancellationToken cancellationToken)
    {
        item.DiscountAmount = item.Total * _discountRate;
        return Task.FromResult(item);
    }
}

// Test it easily
public class DiscountCalculatorTests
{
    [Fact]
    public async Task AppliesCorrectDiscountRate()
    {
        // Arrange
        var transform = new DiscountCalculator(discountRate: 0.1m);
        var order = new Order(1, 100m);
        var context = PipelineContext.Default;

        // Act
        var result = await transform.ExecuteAsync(order, context, CancellationToken.None);

        // Assert
        Assert.Equal(10m, result.DiscountAmount);
    }
}
```

## Principle 6: Use Appropriate Execution Strategies

Choose the right execution strategy (sequential, parallel, batched) for each node based on its nature.

### Good Example

```csharp
// CPU-bound work - good candidate for parallelism
public sealed class ExpensiveTransform : TransformNode<Data, Result>
{
    public override Task<Result> ExecuteAsync(Data item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Heavy computation
        var result = PerformExpensiveCalculation(item);
        return Task.FromResult(result);
    }
}

// Use parallelism extension for CPU-bound work
var pipeline = new PipelineBuilder()
    .AddSource<DataSource, Data>()
    .AddTransform<ExpensiveTransform, Data, Result>()
    .WithParallelism(degreeOfParallelism: Environment.ProcessorCount)
    .AddSink<ResultSink, Result>();
```

## Principle 6b: Optimize Synchronous Transforms with ValueTask

For synchronous transforms in hot paths, override `ExecuteValueTaskAsync` to eliminate Task allocation overhead.

### Synchronous Transform Optimization

```csharp
// Synchronous transform optimized with ValueTask
public sealed class FastValidation : TransformNode<Order, Order>
{
    public override Task<Order> ExecuteAsync(Order item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Delegate to ValueTask implementation
        return FromValueTask(ExecuteValueTaskAsync(item, context, cancellationToken));
    }

    protected internal override ValueTask<Order> ExecuteValueTaskAsync(Order item, PipelineContext context, CancellationToken cancellationToken)
    {
        // No Task allocation - synchronous work
        if (item.Total < 0)
            throw new ArgumentException("Total must be non-negative");

        return ValueTask.FromResult(item);
    }
}

// Execution strategies automatically detect and use the ValueTask path
var pipeline = new PipelineBuilder()
    .AddSource<OrderSource, Order>()
    .AddTransform<FastValidation, Order, Order>()  // Uses ValueTask path automatically
    .AddSink<ResultSink, Order>();
```

See [Synchronous Fast Paths](../advanced-topics/synchronous-fast-paths.md) for detailed guidance on when and how to use this optimization.

## Principle 7: Monitor and Log Appropriately

Instrument your pipelines with logging at appropriate levels.

### Good Example

```csharp
public sealed class MonitoredTransform : TransformNode<Order, ProcessedOrder>
{
    private readonly ILogger<MonitoredTransform> _logger;

    public MonitoredTransform(ILogger<MonitoredTransform> logger)
    {
        _logger = logger;
    }

    public override async Task<ProcessedOrder> ExecuteAsync(Order item, PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogDebug("Starting processing for order {OrderId}", item.Id);

        try
        {
            var result = await ProcessAsync(item, cancellationToken);
            _logger.LogInformation("Successfully processed order {OrderId}", item.Id);
            return result;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to process order {OrderId}", item.Id);
            throw;
        }
    }

    private async Task<ProcessedOrder> ProcessAsync(Order item, CancellationToken cancellationToken)
    {
        await Task.CompletedTask;
        return new ProcessedOrder(item.Id, item.Total);
    }
}
```

## Principle 8: Design for Failure

Anticipate failures and design recovery mechanisms.

### Good Example

```csharp
public sealed class ResilientSink : SinkNode<Data>
{
    private readonly ILogger<ResilientSink> _logger;

    public ResilientSink(ILogger<ResilientSink> logger)
    {
        _logger = logger;
    }

    public override async Task ExecuteAsync(IDataPipe<Data> input, PipelineContext context, CancellationToken cancellationToken)
    {
        var successCount = 0;
        var failureCount = 0;

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            try
            {
                await SaveItemAsync(item, cancellationToken);
                successCount++;
            }
            catch (Exception ex)
            {
                failureCount++;
                _logger.LogError(ex, "Failed to save item, continuing with next item");
                // Continue processing rather than failing completely
            }
        }

        _logger.LogInformation("Processing complete. Successful: {Success}, Failed: {Failed}", successCount, failureCount);
    }

    private async Task SaveItemAsync(Data item, CancellationToken cancellationToken)
    {
        await Task.CompletedTask;
    }
}
```

## Principle 9: Document Your Pipelines

Clearly document the purpose, inputs, and outputs of your pipelines and nodes.

### Good Example

```csharp
/// <summary>
/// Processes customer orders by validating prices, calculating taxes, and enriching with regional data.
/// </summary>
/// <remarks>
/// Input: Raw order data from CSV
/// Output: Processed orders with tax and region information
/// Error handling: Invalid prices are logged and skipped
/// </remarks>
public sealed class OrderProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Implementation
    }
}

/// <summary>
/// Validates order prices are within acceptable ranges.
/// </summary>
/// <exception cref="InvalidOperationException">Thrown when price is negative or exceeds limit</exception>
public sealed class PriceValidator : TransformNode<Order, Order>
{
    public override Task<Order> ExecuteAsync(Order item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Implementation
    }
}
```

## Principle 10: Profile and Optimize

Identify bottlenecks before optimizing.

### Performance Optimization Checklist

- Use profiling tools to identify actual bottlenecks
- Measure before and after optimizations
- Consider memory usage, not just speed
- Evaluate parallelism trade-offs
- Use appropriate batch sizes for I/O operations
- Minimize allocations in hot paths
- Cache expensive computations appropriately

## Summary of Best Practices

| Practice | Benefit |
|----------|---------|
| Single Responsibility | Easier to understand, test, and maintain |
| Dependency Injection | Flexible, testable, decoupled code |
| Explicit Error Handling | Reliable, debuggable pipelines |
| Efficient Streaming | Lower memory usage, higher throughput |
| Testability | Confidence in correctness |
| Appropriate Execution Strategy | Optimal performance |
| ValueTask Optimization | Reduced allocations in hot paths |
| Monitoring & Logging | Visibility and debuggability |
| Failure Design | Resilient, production-ready pipelines |
| Documentation | Easier adoption and maintenance |
| Profiling | Data-driven optimization |

## Next Steps

- **[Common Patterns](common-patterns.md)**: See these practices in action with practical examples
- **[Testing Pipelines](../extensions/testing/index.md)**: Learn how to test your pipelines effectively
- **[Error Handling](resilience/error-handling.md)**: Implement robust error handling in your pipelines
