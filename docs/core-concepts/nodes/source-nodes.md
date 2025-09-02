---
title: Source Nodes
description: Learn how to implement source nodes that generate the initial data stream entering your pipeline.
sidebar_position: 1
---

# Source Nodes (`ISourceNode<TOut>`)

A source node is responsible for generating the initial data stream that enters the pipeline. It doesn't receive any input but produces a stream of `TOutput` items.

## Interface Definition

```csharp
public interface ISourceNode<out TOut> : INode
{
    IDataPipe<TOut> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken);
}
```

The `ExecuteAsync` method returns the data pipe synchronously. The pipeline execution framework wraps this result in a `Task` internally when needed for asynchronous execution orchestration.

## Implementation Example

A simple source that produces a sequence of numbers:

```csharp
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

public sealed class NumberSource : ISourceNode<int>
{
    private readonly int _start;
    private readonly int _count;

    public NumberSource(int start, int count)
    {
        _start = start;
        _count = count;
    }

    public IDataPipe<int> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        static IAsyncEnumerable<int> Stream(int start, int count, CancellationToken ct)
        {
            return GenerateNumbers();

            async IAsyncEnumerable<int> GenerateNumbers()
            {
                for (int i = 0; i < count; i++)
                {
                    ct.ThrowIfCancellationRequested();
                    yield return start + i;
                }
            }
        }

        return new StreamingDataPipe<int>(Stream(_start, _count, cancellationToken));
    }
}
```

## Key Concepts

### Data Pipe Output

The `ExecuteAsync` method returns an `IDataPipe<TOut>` directly (synchronously). The `IDataPipe<T>` abstraction allows NPipeline to support different streaming models (buffered, streaming, etc.) while maintaining a consistent interface.

### Cancellation Support

Always respect the `cancellationToken` parameter to allow graceful shutdown and cancellation of pipeline execution. This is especially important for sources that read from external systems (files, databases, network streams).

### Lazy Initialization

Source nodes are not executed until the pipeline is run. This allows for efficient resource management—connections, file handles, and other resources are only acquired when needed.

## Real-World Examples

### File Reader Source

```csharp
public sealed class FileReaderSource : ISourceNode<string>
{
    private readonly string _filePath;

    public FileReaderSource(string filePath)
    {
        _filePath = filePath;
    }

    public IDataPipe<string> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        static IAsyncEnumerable<string> Stream(string path, CancellationToken ct)
        {
            return ReadLines();

            async IAsyncEnumerable<string> ReadLines()
            {
                var lines = await File.ReadAllLinesAsync(path, ct);
                foreach (var line in lines)
                {
                    ct.ThrowIfCancellationRequested();
                    yield return line;
                }
            }
        }

        return new StreamingDataPipe<string>(Stream(_filePath, cancellationToken));
    }
}
```

### Database Query Source

```csharp
public sealed class DatabaseSource : ISourceNode<CustomerRecord>
{
    private readonly string _connectionString;

    public DatabaseSource(string connectionString)
    {
        _connectionString = connectionString;
    }

    public IDataPipe<CustomerRecord> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        return new StreamingDataPipe<CustomerRecord>(FetchRecordsAsync(cancellationToken));
    }

    private async IAsyncEnumerable<CustomerRecord> FetchRecordsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using (var connection = new SqlConnection(_connectionString))
        {
            await connection.OpenAsync(cancellationToken);
            using (var command = new SqlCommand("SELECT * FROM Customers", connection))
            {
                using (var reader = await command.ExecuteReaderAsync(cancellationToken))
                {
                    while (await reader.ReadAsync(cancellationToken))
                    {
                        yield return new CustomerRecord
                        {
                            Id = reader.GetInt32(0),
                            Name = reader.GetString(1),
                            Email = reader.GetString(2)
                        };
                    }
                }
            }
        }
    }
}
```

## Next Steps

* **[Transform Nodes](transform-nodes.md)**: Learn how to process and transform data within your pipeline
* **[Sink Nodes](sink-nodes.md)**: Understand the final stage of data consumption
