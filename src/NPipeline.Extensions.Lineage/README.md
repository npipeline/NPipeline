# NPipeline.Extensions.Lineage

Comprehensive data lineage tracking and provenance capabilities for NPipeline pipelines.

## Overview

The `NPipeline.Extensions.Lineage` extension provides production-ready lineage tracking for data flowing through NPipeline pipelines. Track the complete journey
of each data item from source to destination, enabling data governance, debugging, audit trails, and data discovery.

### Key Features

- **Item-level lineage tracking**: Trace individual data items as they flow through each node in the pipeline
- **Pipeline-level reports**: Generate high-level reports showing pipeline structure, nodes, edges, and data flow patterns
- **Configurable sampling**: Reduce overhead with deterministic or random sampling strategies
- **Thread-safe collection**: Lineage data is collected safely across parallel and concurrent pipeline executions
- **Flexible sinks**: Built-in logging sinks with support for custom lineage sinks (e.g., databases, file systems, external services)
- **Dependency injection integration**: Seamlessly integrates with Microsoft.Extensions.DependencyInjection
- **Data redaction**: Optionally exclude actual data from lineage records to reduce memory usage and improve security
- **Comprehensive metadata**: Capture hop timestamps, decision flags, observed cardinality, and ancestry information

## Installation

```bash
dotnet add package NPipeline.Extensions.Lineage
```

The extension requires:

- `NPipeline` (core package)
- `Microsoft.Extensions.DependencyInjection.Abstractions` (10.0.1 or later)
- `Microsoft.Extensions.Logging.Abstractions` (10.0.1 or later)

## Quick Start

### Basic Setup with DI

The simplest way to enable lineage tracking is to use the `AddNPipelineLineage` extension method:

```csharp
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Lineage.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;

var services = new ServiceCollection();

// Add NPipeline core services
services.AddNPipeline(Assembly.GetExecutingAssembly());

// Add lineage services with default logging sink
services.AddNPipelineLineage();

var serviceProvider = services.BuildServiceProvider();
```

### Using with PipelineBuilder

Configure lineage tracking directly on your pipeline builder:

```csharp
using NPipeline.Pipeline;
using NPipeline.Lineage;

var builder = new PipelineBuilder("MyPipeline");

// Enable item-level lineage tracking
builder.EnableItemLevelLineage();

// Add a logging sink for pipeline-level lineage reports
builder.UseLoggingPipelineLineageSink();

// Build and execute the pipeline
var pipeline = builder.Build();
await pipeline.ExecuteAsync(serviceProvider);
```

### Complete Example

Here's a fully working example with a pipeline definition and nodes:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Lineage.DependencyInjection;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Pipeline;
using NPipeline.Lineage;

public class OrderPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<OrderSource, OrderEvent>();
        var transform = builder.AddTransform<ValidationTransform, OrderEvent, ValidatedOrder>();
        var sink = builder.AddSink<OrderSink, ValidatedOrder>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

public sealed class OrderSource : SourceNode<OrderEvent>
{
    public IDataPipe<OrderEvent> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken = default)
    {
        static async IAsyncEnumerable<OrderEvent> Generate()
        {
            for (int i = 1; i <= 100; i++)
            {
                yield return new OrderEvent(i, 100, 200, 1, 99.99m, DateTime.UtcNow);
            }
        }

        return new StreamingDataPipe<OrderEvent>(Generate());
    }
}

public sealed class ValidationTransform : TransformNode<OrderEvent, ValidatedOrder>
{
    public override Task<ValidatedOrder> ExecuteAsync(OrderEvent item, PipelineContext context, CancellationToken cancellationToken = default)
    {
        return Task.FromResult(new ValidatedOrder(item, true, Array.Empty<string>()));
    }
}

public sealed class OrderSink : SinkNode<ValidatedOrder>
{
    public async Task ExecuteAsync(IDataPipe<ValidatedOrder> input, PipelineContext context, IPipelineActivity parentActivity, CancellationToken cancellationToken = default)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Processed: {item.Order.OrderId}");
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
        var contextFactory = scope.ServiceProvider.GetRequiredService<IPipelineContextFactory>();
        var collector = scope.ServiceProvider.GetRequiredService<ILineageCollector>();

        // Create context
        await using var pipelineContext = contextFactory.Create();

        // Run pipeline
        await runner.RunAsync<OrderPipeline>(pipelineContext);

        // Access lineage information
        var allLineage = collector.GetAllLineageInfo();
        Console.WriteLine($"Collected lineage for {allLineage.Count} items");
    }
}
```

## Configuration

### Lineage Options

Configure lineage tracking behavior using [`LineageOptions`](LineageOptions.cs):

```csharp
builder.EnableItemLevelLineage(options =>
{
    // Sampling configuration
    options.SampleEvery = 10; // Sample 1 in 10 items
    options.DeterministicSampling = true; // Use deterministic sampling

    // Data configuration
    options.RedactData = true; // Don't include actual data in lineage records

    // Metadata capture
    options.CaptureHopTimestamps = true;
    options.CaptureDecisions = true;
    options.CaptureObservedCardinality = true;

    // Memory management
    options.MaterializationCap = 10000; // Max items to materialize
    options.OverflowPolicy = LineageOverflowPolicy.Degrade; // Switch to streaming
});
```

### Sampling Strategies

Control lineage collection overhead with configurable sampling:

**Deterministic Sampling** (default) - same items always sampled:

```csharp
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 100; // 1% sampling rate
    options.DeterministicSampling = true;
});
```

Deterministic sampling ensures consistent behavior across runs, making it ideal for debugging and compliance scenarios.

**Random Sampling** - non-deterministic random sampling:

```csharp
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 100;
    options.DeterministicSampling = false;
});
```

Random sampling provides a representative sample with minimal overhead, suitable for monitoring and analytics.

### Data Redaction

Exclude actual data from lineage records to reduce memory usage and improve security:

```csharp
builder.EnableItemLevelLineage(options =>
{
    options.RedactData = true; // Data field will be null in LineageInfo
});
```

## Dependency Injection

### Basic Registration

```csharp
services.AddNPipelineLineage();
```

### Custom Sink Type

```csharp
services.AddNPipelineLineage<DatabaseLineageSink>();
```

### Factory Delegate

```csharp
services.AddNPipelineLineage(sp =>
{
    var logger = sp.GetRequiredService<ILogger<CustomLineageSink>>();
    return new CustomLineageSink(logger);
});
```

### Custom Collector

```csharp
services.AddNPipelineLineage<CustomLineageCollector, CustomLineageSink>();
```

## Custom Lineage Sinks

Implement your own lineage sinks for custom storage/analysis:

### Item-Level Sink

```csharp
public class DatabaseLineageSink : ILineageSink
{
    private readonly IDbConnection _connection;

    public DatabaseLineageSink(IDbConnection connection)
    {
        _connection = connection;
    }

    public async Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
    {
        // Store lineage information in your database
        await _connection.ExecuteAsync(
            "INSERT INTO Lineage (LineageId, Data, TraversalPath) VALUES (@LineageId, @Data, @TraversalPath)",
            new { lineageInfo.LineageId, lineageInfo.Data, lineageInfo.TraversalPath },
            cancellationToken);
    }
}

// Register with DI
services.AddNPipelineLineage<DatabaseLineageSink>();
```

### Pipeline-Level Sink

```csharp
public class JsonFileLineageSink : IPipelineLineageSink
{
    private readonly string _filePath;

    public JsonFileLineageSink(string filePath) => _filePath = filePath;

    public async Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken)
    {
        var json = JsonSerializer.Serialize(report, new JsonSerializerOptions { WriteIndented = true });
        await File.WriteAllTextAsync(_filePath, json, cancellationToken);
    }
}

// Use with PipelineBuilder
builder.AddPipelineLineageSink(new JsonFileLineageSink("lineage-report.json"));
```

## API Reference

### ILineageCollector

Main interface for collecting lineage information during pipeline execution.

```csharp
public interface ILineageCollector
{
    /// <summary>
    /// Creates a new lineage packet for a data item entering the pipeline.
    /// </summary>
    LineagePacket<T> CreateLineagePacket<T>(T item, string sourceNodeId);

    /// <summary>
    /// Records a hop in the lineage trail for an item.
    /// </summary>
    void RecordHop(Guid lineageId, LineageHop hop);

    /// <summary>
    /// Determines if lineage should be collected for a given item based on sampling settings.
    /// </summary>
    bool ShouldCollectLineage(Guid lineageId, LineageOptions? options);

    /// <summary>
    /// Gets the lineage information for a specific item.
    /// </summary>
    LineageInfo? GetLineageInfo(Guid lineageId);

    /// <summary>
    /// Gets all collected lineage information.
    /// </summary>
    IReadOnlyList<LineageInfo> GetAllLineageInfo();

    /// <summary>
    /// Clears all collected lineage information.
    /// </summary>
    void Clear();
}
```

### ILineageSink

Receives completed lineage records for individual data items.

```csharp
public interface ILineageSink
{
    /// <summary>
    /// Asynchronously records a completed lineage record.
    /// </summary>
    Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken);
}
```

### IPipelineLineageSink

Receives pipeline-level lineage reports.

```csharp
public interface IPipelineLineageSink
{
    /// <summary>
    /// Asynchronously records a pipeline lineage report.
    /// </summary>
    Task RecordAsync(PipelineLineageReport report, CancellationToken cancellationToken);
}
```

### LineageInfo

Represents a completed lineage record for a single data item.

```csharp
public sealed record LineageInfo(
    object? Data,                          // Final data (nullable when redacted)
    Guid LineageId,                         // Unique identifier
    IReadOnlyList<string> TraversalPath,   // Node IDs passed through
    IReadOnlyList<LineageHop> LineageHops   // Per-hop details
);
```

### LineageHop

Represents a single hop through a node.

```csharp
public sealed record LineageHop(
    string NodeId,
    HopDecisionFlags Outcome,
    ObservedCardinality Cardinality,
    int? InputContributorCount,
    int? OutputEmissionCount,
    IReadOnlyList<int>? AncestryInputIndices,
    bool Truncated
);
```

### LineageOptions

Configuration options for lineage tracking.

```csharp
public sealed class LineageOptions
{
    /// <summary>
    /// Gets or sets the sampling rate (1 in N items). Default is 1 (all items).
    /// </summary>
    public int SampleEvery { get; set; } = 1;

    /// <summary>
    /// Gets or sets whether to use deterministic sampling. Default is true.
    /// </summary>
    public bool DeterministicSampling { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to redact actual data from lineage records. Default is false.
    /// </summary>
    public bool RedactData { get; set; } = false;

    /// <summary>
    /// Gets or sets whether to capture hop timestamps. Default is true.
    /// </summary>
    public bool CaptureHopTimestamps { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to capture decision flags. Default is true.
    /// </summary>
    public bool CaptureDecisions { get; set; } = true;

    /// <summary>
    /// Gets or sets whether to capture observed cardinality. Default is true.
    /// </summary>
    public bool CaptureObservedCardinality { get; set; } = true;

    /// <summary>
    /// Gets or sets the maximum number of items to materialize. Default is 10000.
    /// </summary>
    public int MaterializationCap { get; set; } = 10000;

    /// <summary>
    /// Gets or sets the overflow policy when materialization cap is reached. Default is Degrade.
    /// </summary>
    public LineageOverflowPolicy OverflowPolicy { get; set; } = LineageOverflowPolicy.Degrade;
}
```

## Performance Considerations

### Sampling

Use sampling to reduce overhead in high-throughput scenarios:

```csharp
// Low overhead for production
options.SampleEvery = 1000; // 0.1% sampling

// Higher sampling for debugging/development
options.SampleEvery = 10; // 10% sampling
```

**Why Sampling Matters**: Lineage tracking has a non-zero cost. Each hop requires allocation, synchronization, and storage. Sampling allows you to maintain
visibility into pipeline behavior while minimizing performance impact.

### Memory Management

Configure materialization caps to prevent memory issues:

```csharp
options.MaterializationCap = 10000; // Max items to materialize
options.OverflowPolicy = LineageOverflowPolicy.Degrade; // Switch to streaming
```

**Materialization Strategies**:

- **Materialize**: Store all lineage data in memory for complete analysis
- **Degrade**: Switch to streaming mode when cap is reached
- **Drop**: Stop collecting lineage when cap is reached

### Data Redaction

Exclude actual data from lineage records to reduce memory usage:

```csharp
options.RedactData = true; // Data field will be null in LineageInfo
```

**When to Redact**:

- When data objects are large
- When data contains sensitive information
- When you only care about flow patterns, not actual data values

### Thread Safety

The [`LineageCollector`](LineageCollector.cs) is thread-safe and can be used across parallel pipeline executions. It uses `ConcurrentDictionary` for storage and
locks only when necessary for individual trail updates.

## Architecture

```text
┌─────────────────────────────────────────┐
│         Pipeline Execution              │
└──────────────┬──────────────────────────┘
               │
               ├─> Lineage Tracking (per item)
               │   - CreateLineagePacket
               │   - RecordHop
               │   - ShouldCollectLineage
               │
               ↓
        ┌─────────────────────────┐
        │   ILineageCollector     │
        │  (Thread-safe)          │
        └──────────┬──────────────┘
                   │
                   ├─> LineageInfo (per item)
                   │   - TraversalPath
                   │   - LineageHops
                   │   - Data (optional)
                   │
                   ↓
        ┌──────────────────────┐
        │   ILineageSink       │
        │ IPipelineLineageSink │
        └──────────┬───────────┘
                   │
                   ├─> Logging
                   ├─> Database
                   ├─> File System
                   └─> Custom Sinks
```

## Common Use Cases

1. **Data Governance**: Track data provenance and transformation history for compliance requirements (GDPR, HIPAA, SOX)
2. **Debugging**: Quickly identify which node introduced data quality issues or transformation errors
3. **Audit Trails**: Prove data integrity and maintain complete transformation history
4. **Data Discovery**: Find what data depends on a particular source or transformation
5. **Root Cause Analysis**: Trace issues back to their origin in complex pipelines
6. **Performance Monitoring**: Understand data flow patterns and identify bottlenecks
7. **Testing**: Verify that data flows through the expected path in complex pipelines

## Troubleshooting

### No Lineage Data Collected

**Problem**: Lineage information is empty or not being collected.

**Solutions**:

- Ensure lineage is enabled: `builder.EnableItemLevelLineage()`
- Verify sampling rate: `options.SampleEvery` should be >= 1
- Check that `ShouldCollectLineage` returns true for your items
- Ensure [`ILineageCollector`](LineageCollector.cs) is registered in DI

### High Memory Usage

**Problem**: Memory usage increases significantly with lineage enabled.

**Solutions**:

- Enable sampling: `options.SampleEvery = 100` or higher
- Enable data redaction: `options.RedactData = true`
- Set materialization cap: `options.MaterializationCap = 10000`
- Use overflow policy: `options.OverflowPolicy = LineageOverflowPolicy.Degrade`

### Performance Degradation

**Problem**: Pipeline execution slows down with lineage enabled.

**Solutions**:

- Increase sampling rate to reduce overhead
- Disable unnecessary metadata capture: `options.CaptureHopTimestamps = false`
- Use deterministic sampling for consistent performance
- Consider async lineage sinks to avoid blocking

### Missing Pipeline Reports

**Problem**: Pipeline-level lineage reports are not being generated.

**Solutions**:

- Register a pipeline lineage sink: `builder.UseLoggingPipelineLineageSink()`
- Ensure [`IPipelineLineageSink`](LoggingPipelineLineageSink.cs) is registered in DI
- Check sink implementation for errors (logging sink never throws)

## Examples

See the [Sample_LineageExtension](../../samples/Sample_LineageExtension/) project for complete working examples demonstrating:

- Basic lineage tracking
- Deterministic and random sampling
- Complex join pipelines
- Branching with lineage
- Error handling with lineage
- Custom lineage sinks

## Documentation

- **[Extension Overview](../../docs/extensions/lineage.md)** - High-level overview and use cases
- **[Sample Documentation](../../docs/samples/lineage.md)** - Sample application walkthrough
- **[NPipeline Core Concepts](../../docs/core-concepts/index.md)** - Core pipeline concepts
- **[NPipeline Extensions](../../docs/extensions/index.md)** - Other available extensions

## License

MIT License - see [LICENSE](../../LICENSE) for details.
