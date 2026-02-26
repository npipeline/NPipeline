---
title: Getting Started with Lineage
description: Installation and basic setup guide for NPipeline Lineage extension.
---

# Getting Started with Lineage

This guide covers installation and basic setup for NPipeline Lineage extension.

## Installation

Install the Lineage extension via NuGet:

```bash
dotnet add package NPipeline.Extensions.Lineage
```

The extension requires:

- `NPipeline` (core package)
- `Microsoft.Extensions.DependencyInjection.Abstractions` (10.0.1 or later)
- `Microsoft.Extensions.Logging.Abstractions` (10.0.1 or later)

## Basic Setup

### Quick Start with Dependency Injection

The simplest way to enable lineage tracking is through dependency injection:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Lineage.DependencyInjection;

var services = new ServiceCollection();

// Add lineage services with default logging sink
services.AddNPipelineLineage();

// Add NPipeline core services
services.AddNPipeline(Assembly.GetExecutingAssembly());

var serviceProvider = services.BuildServiceProvider();
```

### Enable Lineage in Pipeline

Configure lineage tracking on your pipeline builder:

```csharp
using NPipeline.Lineage;
using NPipeline.Pipeline;

var builder = new PipelineBuilder("MyPipeline");

// Enable item-level lineage tracking with default options
builder.EnableItemLevelLineage();

// Add pipeline-level lineage sink
builder.UseLoggingPipelineLineageSink();
```

## Complete Example

Here's a fully working example demonstrating basic lineage setup:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Lineage;
using NPipeline.Lineage.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public class OrderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<OrderSource, Order>();
        var validate = builder.AddTransform<OrderValidationTransform, Order, ValidatedOrder>();
        var sink = builder.AddSink<OrderSink, ValidatedOrder>();

        builder.Connect(source, validate);
        builder.Connect(validate, sink);
    }
}

public sealed class OrderSource : SourceNode<Order>
{
    public IDataPipe<Order> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken = default)
    {
        static async IAsyncEnumerable<Order> GenerateOrders()
        {
            for (int i = 1; i <= 100; i++)
            {
                yield return new Order(i, $"Customer_{i}", 100 * i);
            }
        }

        return new StreamingDataPipe<Order>(GenerateOrders());
    }
}

public sealed class OrderValidationTransform : TransformNode<Order, ValidatedOrder>
{
    public override Task<ValidatedOrder> ExecuteAsync(Order item, PipelineContext context, CancellationToken cancellationToken = default)
    {
        var isValid = item.Amount > 0 && item.CustomerId.Length > 0;
        return Task.FromResult(new ValidatedOrder(item, isValid));
    }
}

public sealed class OrderSink : SinkNode<ValidatedOrder>
{
    public async Task ExecuteAsync(IDataPipe<ValidatedOrder> input, PipelineContext context, IPipelineActivity parentActivity, CancellationToken cancellationToken = default)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Processed Order {item.OrderId}: Valid={item.IsValid}");
        }
    }
}

public class Program
{
    public static async Task Main(string[] args)
    {
        var host = Host.CreateDefaultBuilder()
            .ConfigureServices((context, services) =>
            {
                services.AddLogging(loggingBuilder => loggingBuilder.AddConsole());
                services.AddNPipeline(typeof(Program).Assembly);
                services.AddNPipelineLineage();
            })
            .Build();

        await using var scope = host.Services.CreateAsyncScope();
        var runner = scope.ServiceProvider.GetRequiredService<IPipelineRunner>();
        var collector = scope.ServiceProvider.GetRequiredService<ILineageCollector>();

        // Create context with lineage tracking
        var builder = new PipelineBuilder("OrderPipeline");
        builder.EnableItemLevelLineage();
        builder.UseLoggingPipelineLineageSink();

        var pipeline = new OrderPipeline();
        pipeline.Define(builder, new PipelineContext());

        // Run pipeline - lineage is automatically collected
        await runner.RunAsync(builder.Build(), new PipelineContext());

        // Access collected lineage data
        Console.WriteLine($"\nCollected lineage for {collector.GetAllLineageInfo().Count()} items");
    }
}
```

## What Gets Tracked

With default configuration, lineage tracking captures:

**For each item:**

- Unique lineage ID
- Complete traversal path (all node IDs)
- Per-hop details (node, outcome, cardinality, counts)
- Original data (unless redacted)

**For the pipeline:**

- All nodes and their connections
- Execution summary statistics
- Run metadata (ID, timestamps)

## Next Steps

- **[Configuration Guide](./configuration.md)** - Learn about configuration options for sampling, redaction, and overflow policies
- **[Architecture](./architecture.md)** - Understand the internal architecture and design decisions
- **[Performance](./performance.md)** - Learn about performance characteristics and optimization strategies
- **[Use Cases](./use-cases.md)** - Explore common use cases and advanced examples
- **[Extension Samples](../../samples/index.md)** - Sample applications for all extensions including lineage
