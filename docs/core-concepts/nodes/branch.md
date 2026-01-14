---
title: Branch Nodes
description: Duplicate data streams to multiple downstream paths for branching logic or monitoring using NPipeline's Branch Nodes.
sidebar_position: 8
---

# Branch Nodes

Branch nodes allow you to duplicate (fan-out) an incoming data stream and send identical copies of each item to multiple downstream paths. This is incredibly useful for scenarios where you need to process the same data in different ways concurrently, or for parallel processing of the same data across multiple independent branches.

```mermaid
graph TD
    A[Source Data Stream] --> B[BranchNode]
    B --> C[Branch 1: Main Processing]
    B --> D[Branch 2: Processing]
    B --> E[Branch 3: Processing]

    C --> F[Sink]
    D --> G[Sink]
    E --> H[Sink]

    style B fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    style C fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    style D fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    style E fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
```

*Figure: BranchNode fan-out pattern showing how a single input stream is duplicated to multiple independent processing branches.*

NPipeline provides the [`BranchNode<T>`](src/NPipeline/Nodes/BranchNode.cs) for general-purpose stream duplication. For non-intrusive monitoring and side-channel processing, see [Tap Nodes](tap.md).

## [`BranchNode<T>`](src/NPipeline/Nodes/BranchNode.cs): Duplicating Streams

The [`BranchNode<T>`](src/NPipeline/Nodes/BranchNode.cs) takes a single input stream of type `T` and produces multiple output streams of the same type `T`. Each item that enters the `BranchNode` is sent to all connected downstream nodes.

### Example: Processing Data in Multiple Ways

Imagine a scenario where you receive a stream of sensor readings. You might want to:

1. Store the raw readings in a database.
2. Analyze the readings for anomalies in real-time.
3. Aggregate the readings for hourly reports.

A `BranchNode` allows you to achieve this by fanning out the raw readings to three different processing branches.

```csharp
using NPipeline;
using NPipeline.Nodes;

public sealed record SensorReading(DateTime Timestamp, double Value);

public sealed class SensorReadingSource : SourceNode<SensorReading>
{
    public async IAsyncEnumerable<SensorReading> ExecuteAsync(CancellationToken cancellationToken = default)
    {
        for (int i = 0; i < 5; i++)
        {
            if (cancellationToken.IsCancellationRequested) yield break;
            var reading = new SensorReading(DateTime.UtcNow.AddSeconds(i), i * 10.0);
            Console.WriteLine($"Source: Producing {reading}");
            yield return reading;
            await Task.Delay(100, cancellationToken);
        }
    }
}

public sealed class RawDataSink : SinkNode<SensorReading>
{
    public async Task ExecuteAsync(IAsyncEnumerable<SensorReading> input, CancellationToken cancellationToken = default)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"RawDataSink: Stored {item}");
        }
    }
}

public sealed class AnomalyDetector : ITransformNode<SensorReading, string>
{
    public async IAsyncEnumerable<string> ExecuteAsync(IAsyncEnumerable<SensorReading> input, CancellationToken cancellationToken = default)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            if (cancellationToken.IsCancellationRequested) yield break;
            if (item.Value > 30.0 && item.Value < 60.0) // Simple anomaly detection
            {
                yield return $"Anomaly Detected: Reading {item.Value} at {item.Timestamp}";
            }
        }
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        var context = PipelineContext.Default;
        var runner = PipelineRunner.Create();
        
        Console.WriteLine("Starting branching pipeline...");
        await runner.RunAsync<BranchingPipelineDefinition>(context);
        Console.WriteLine("Branching pipeline finished.");
    }
}

public sealed class BranchingPipelineDefinition : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var sourceHandle = builder.AddSource<SensorReadingSource, SensorReading>("source");
        var branchHandle = builder.AddBranch<BranchNode<SensorReading>>("branch");
        var rawDataSinkHandle = builder.AddSink<RawDataSink, SensorReading>("rawSink");
        var anomalyDetectorHandle = builder.AddTransform<AnomalyDetector, SensorReading, string>("anomaly");
        var consoleSinkHandle = builder.AddSink<ConsoleSink<string>, string>("consoleSink");

        // First branch: direct to raw data sink
        builder.Connect(sourceHandle, branchHandle);
        builder.Connect(branchHandle, rawDataSinkHandle);
        
        // Second branch: through anomaly detector
        builder.Connect(branchHandle, anomalyDetectorHandle);
        builder.Connect(anomalyDetectorHandle, consoleSinkHandle);
    }
}
```

In this example, the `BranchNode` duplicates the input stream, sending copies to multiple downstream paths. One copy goes to `RawDataSink` for storage, and another copy goes through `AnomalyDetector` for real-time anomaly alerts.

## Error Handling in Branch Nodes

Branch handlers are user-provided delegates that execute in parallel. When a branch handler throws an exception, the behavior is controlled by the `ErrorHandlingMode` property:

### Error Handling Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `RouteToErrorHandler` (default) | Exceptions are wrapped in `BranchHandlerException` and routed through the pipeline's `IPipelineErrorHandler`. If no handler is configured, the exception propagates. | Production pipelines with error monitoring |
| `CollectAndThrow` | All branch exceptions are collected and thrown as an `AggregateException` after all branches complete. | When you need all branches to attempt execution |
| `LogAndContinue` | Exceptions are logged but swallowed, allowing the pipeline to continue. | Non-critical side effects (use with caution) |

### Example: Configuring Error Handling Mode

```csharp
// Create a branch node with explicit error handling mode
var branchNode = new BranchNode<SensorReading>
{
    ErrorHandlingMode = BranchErrorHandlingMode.RouteToErrorHandler
};

branchNode.AddOutput(async reading =>
{
    // This handler's errors will be routed through the pipeline error handler
    await SendToAlertSystemAsync(reading);
});
```

### Example: Handling Branch Errors with IPipelineErrorHandler

```csharp
public class MyErrorHandler : IPipelineErrorHandler
{
    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        if (error is BranchHandlerException branchEx)
        {
            // Log the branch failure but continue the pipeline
            Console.WriteLine($"Branch {branchEx.BranchIndex} failed: {branchEx.InnerException?.Message}");
            return Task.FromResult(PipelineErrorDecision.ContinueWithoutNode);
        }

        // For other errors, fail the pipeline
        return Task.FromResult(PipelineErrorDecision.FailPipeline);
    }
}
```

### BranchHandlerException Properties

When a branch handler fails, the `BranchHandlerException` provides context about the failure:

- `NodeId`: The ID of the branch node where the failure occurred
- `BranchIndex`: The zero-based index of the failed branch handler
- `FailedItem`: The item that was being processed when the failure occurred
- `InnerException`: The original exception from the branch handler

## Performance Considerations

- **Memory Usage**: For large-scale fan-out scenarios, be mindful of memory consumption. Each branch maintains its own processing queue and buffers, which can multiply memory usage with multiple branches.
- **Backpressure**: If one branch of a `BranchNode` becomes slow, it can create backpressure that affects other branches. Consider using appropriate buffering strategies or async processing in sink implementations.
- **Throughput Impact**: Duplicating streams incurs overhead. For high-throughput scenarios, profile your pipeline to ensure the fan-out doesn't become a bottleneck.
- **Resource Management**: Ensure all branches properly dispose of resources, especially when using sinks that maintain connections or file handles.

## Considerations for Branch Nodes

- **Order of Processing:** While `BranchNode` duplicates items, the order in which items are processed in parallel branches is not guaranteed unless explicitly managed (e.g., by subsequent synchronization points).
- **Performance Impact:** Duplicating streams and processing them in parallel can increase resource consumption (CPU, memory) if not managed carefully.
- **Error Handling:** By default, errors in branch handlers are routed through the pipeline's error handling system. Configure `ErrorHandlingMode` to control this behavior.

## Next Steps

- **[Tap Nodes](tap.md)**: Learn about non-intrusive monitoring and side-channel processing.
- **[Advanced Error Handling Patterns](../resilience/error-handling.md#advanced-patterns)**: Learn more about handling errors in complex pipeline structures.
