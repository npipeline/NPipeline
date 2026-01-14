---
title: Sink Nodes
description: Learn how to implement sink nodes that consume and finalize data at the end of your pipeline.
sidebar_position: 4
---

# Sink Nodes (`ISinkNode<TIn>`)

Sink nodes are the terminal points of a pipeline. They consume the final processed data stream (`TInput`) and typically perform an action such as writing to a database, sending a notification, or displaying results. Sink nodes do not produce any output.

## Interface Definition

```csharp
public interface ISinkNode<TIn> : INode
{
    Task ExecuteAsync(IDataPipe<TIn> input, PipelineContext context, CancellationToken cancellationToken);
}
```

## Implementation Example

A sink that prints each incoming number to the console:

```csharp
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

public sealed class ConsoleSink<T> : ISinkNode<T>
{
    public async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Sink received: {item}");
        }
    }
}
```

## Key Concepts

### Data Pipe Consumption

The `ExecuteAsync` method receives an `IDataPipe<TIn>` which represents the final stream of items produced by the previous node in the pipeline. Sink nodes iterate through this stream and perform appropriate operations.

### Streaming Iteration

Use `await foreach` to asynchronously iterate through the data pipe. This pattern allows efficient memory usage—items are processed as they arrive rather than being buffered in memory.

### Activity Tracking

To access tracing and observability information, use the `PipelineContext` parameter. The current activity can be accessed through `context.Tracer.CurrentActivity`, which returns the active tracing span if one exists, or `null` if tracing is disabled.

```csharp
public async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
{
    var activity = context.Tracer.CurrentActivity;
    if (activity != null)
    {
        // Access tracing metadata, emit logs, track metrics
        await activity.EmitEventAsync("Processing started", cancellationToken);
    }
}
```

### Graceful Shutdown

Always respect the `cancellationToken` parameter to allow graceful shutdown of your pipeline. This is especially important for long-running sinks or those that interact with external systems.

> **⚠️ Important**
>
> Always check the cancellation token in your sink's `ExecuteAsync` method. Failure to do so can cause hangs during pipeline shutdown or when users request cancellation.

## Common Sink Patterns

### Database Write Sink

```csharp
public sealed class DatabaseWriteSink : ISinkNode<CustomerRecord>
{
    private readonly string _connectionString;

    public DatabaseWriteSink(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task ExecuteAsync(IDataPipe<CustomerRecord> input, PipelineContext context, CancellationToken cancellationToken)
    {
        using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync(cancellationToken);

            await foreach (var record in input.WithCancellation(cancellationToken))
            {
                using (var command = new SqlCommand("INSERT INTO Customers (Id, Name, Email) VALUES (@id, @name, @email)", connection))
                {
                    command.Parameters.AddWithValue("@id", record.Id);
                    command.Parameters.AddWithValue("@name", record.Name);
                    command.Parameters.AddWithValue("@email", record.Email);

                    await command.ExecuteNonQueryAsync(cancellationToken);
                }
            }
        }
    }
}
```

### File Write Sink

```csharp
public sealed class FileWriteSink : ISinkNode<string>
{
    private readonly string _filePath;

    public FileWriteSink(string filePath)
    {
        _filePath = filePath;
    }

    public async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        using (var writer = new StreamWriter(_filePath, append: false))
        {
            await foreach (var line in input.WithCancellation(cancellationToken))
            {
                await writer.WriteLineAsync(line);
            }
        }
    }
}
```

### HTTP Request Sink

```csharp
public sealed class HttpPostSink : ISinkNode<DataRecord>
{
    private readonly string _endpoint;
    private readonly HttpClient _httpClient;

    public HttpPostSink(string endpoint)
    {
        _endpoint = endpoint;
        _httpClient = new HttpClient();
    }

    public async Task ExecuteAsync(IDataPipe<DataRecord> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var record in input.WithCancellation(cancellationToken))
        {
            var json = JsonSerializer.Serialize(record);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            var response = await _httpClient.PostAsync(_endpoint, content, cancellationToken);
            response.EnsureSuccessStatusCode();
        }
    }
}
```

### Aggregating Sink (Collect Results)

```csharp
public sealed class CollectingSink : ISinkNode<int>
{
    public List<int> Results { get; } = new();

    public async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Results.Add(item);
        }
    }
}
```

### Metric Collection Sink

```csharp
public sealed class MetricsSink : ISinkNode<ProcessedEvent>
{
    private int _totalProcessed = 0;
    private int _totalErrors = 0;

    public int TotalProcessed => _totalProcessed;
    public int TotalErrors => _totalErrors;

    public async Task ExecuteAsync(IDataPipe<ProcessedEvent> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var @event in input.WithCancellation(cancellationToken))
        {
            if (@event.IsError)
            {
                Interlocked.Increment(ref _totalErrors);
            }
            else
            {
                Interlocked.Increment(ref _totalProcessed);
            }
        }
    }
}
```

## Lineage Support in Sink Nodes

For sink nodes that need to work with lineage data, the `SinkLineageUnwrapDelegate` is available in the `NPipeline.Graph.PipelineDelegates` namespace:

```csharp
using NPipeline.Graph.PipelineDelegates;

// Example sink with lineage support
public class LineageAwareSink : ISinkNode<Output>
{
    private readonly SinkLineageUnwrapDelegate? _lineageUnwrap;
    
    public LineageAwareSink(SinkLineageUnwrapDelegate? lineageUnwrap = null)
    {
        _lineageUnwrap = lineageUnwrap;
    }
    
    public async Task ExecuteAsync(
        IDataPipe<Output> input, 
        PipelineContext context, 
        CancellationToken cancellationToken)
    {
        // Use lineage unwrap delegate if provided
        if (_lineageUnwrap != null)
        {
            var unwrappedInput = _lineageUnwrap(
                input,
                context.LineageSink,
                context.CurrentNodeId,
                context.LineageOptions,
                cancellationToken);
                
            // Process with unwrapped input
            await ProcessItemsAsync(unwrappedInput, cancellationToken);
        }
        else
        {
            // Standard processing without lineage
            await ProcessItemsAsync(input, cancellationToken);
        }
    }
    
    private async Task ProcessItemsAsync(IDataPipe<Output> items, CancellationToken cancellationToken)
    {
        // Your sink processing logic here
        await foreach (var item in items.WithCancellation(cancellationToken))
        {
            // Process each item
            await ProcessItemAsync(item, cancellationToken);
        }
    }
    
    private async Task ProcessItemAsync(Output item, CancellationToken cancellationToken)
    {
        // Your actual item processing logic
        await SaveToDestinationAsync(item, cancellationToken);
    }
    
    private async Task SaveToDestinationAsync(Output item, CancellationToken cancellationToken)
    {
        // Your save logic here
        // For example: save to database, file, API, etc.
    }
}
```

## Implementation Example

```csharp
using NPipeline;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Graph.PipelineDelegates;

/// <summary>
/// Sink node that outputs messages to console.
/// Demonstrates terminal node pattern for pipeline output.
/// </summary>
public sealed class ConsoleSink : ISinkNode<string>
{
    public async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        // Process each message as it arrives from upstream
        await foreach (var message in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine(message);
        }
    }
}

/// <summary>
/// Sink node with lineage support that outputs messages to console.
/// Demonstrates how to work with lineage data in sink nodes.
/// </summary>
public sealed class LineageConsoleSink : ISinkNode<string>
{
    private readonly SinkLineageUnwrapDelegate? _lineageUnwrap;
    
    public LineageConsoleSink(SinkLineageUnwrapDelegate? lineageUnwrap = null)
    {
        _lineageUnwrap = lineageUnwrap;
    }
    
    public async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        // Use lineage unwrap delegate if provided
        if (_lineageUnwrap != null)
        {
            var unwrappedInput = _lineageUnwrap(
                input,
                context.LineageSink,
                context.CurrentNodeId,
                context.LineageOptions,
                cancellationToken);
                
            // Process with unwrapped input
            await foreach (var message in unwrappedInput.WithCancellation(cancellationToken))
            {
                Console.WriteLine(message);
            }
        }
        else
        {
            // Standard processing without lineage
            await foreach (var message in input.WithCancellation(cancellationToken))
            {
                Console.WriteLine(message);
            }
        }
    }
}
```

## Next Steps

* **[Node Types](../nodes/index.md)**: Explore sophisticated patterns like aggregation and batching
* **[Pipeline Execution](../pipeline-execution/index.md)**: Learn how pipelines execute and handle errors
