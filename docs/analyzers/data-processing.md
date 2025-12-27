---
title: Data Processing Analyzers
description: Ensure proper input consumption and streaming patterns in pipeline nodes.
sidebar_position: 4
---

## Data Processing Analyzers

Data processing analyzers protect the integrity of data flow through your pipelines. They detect patterns that cause data loss, memory bloat, or improper stream handling.

### NP9205: Non-Streaming Patterns in SourceNode

**ID:** `NP9205`
**Severity:** Warning  
**Category:** Performance  

This analyzer detects non-streaming patterns in SourceNode implementations that can lead to memory issues and poor performance. The analyzer identifies the following problematic patterns:

1. **List and Array allocation and population** in Initialize methods
2. **.ToAsyncEnumerable()** calls on materialized collections
3. **Synchronous I/O operations** like File.ReadAllText, File.WriteAllBytes, etc.
4. **.ToList() and .ToArray()** calls that materialize collections in memory

#### Performance Impact

Non-streaming patterns in SourceNode implementations cause:

1. **High Memory Usage**: Loading entire datasets into memory can cause OutOfMemoryException with large files
2. **Poor Startup Performance**: Applications must wait for all data to be loaded before processing begins
3. **Increased GC Pressure**: Large collections create more garbage collection work
4. **Reduced Scalability**: Memory requirements grow linearly with data size
5. **Blocking I/O**: Synchronous operations block threads and reduce throughput

#### Solution: Use Streaming Patterns

For SourceNode implementations, use IAsyncEnumerable with yield return for proper streaming:

```csharp
// CORRECT: Using IAsyncEnumerable with yield return
public class GoodSourceNode : SourceNode<string>
{
    public override IDataPipe<string> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        return new StreamingDataPipe<string>(ReadLines("large-file.txt", cancellationToken));
    }
    
    // Helper method that yields lines one at a time
    private async IAsyncEnumerable<string> ReadLines(string filePath, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var reader = new StreamReader(
            new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096, useAsync: true));
        
        string? line;
        while ((line = await reader.ReadLineAsync()) != null)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return line;
        }
    }
}
```

### NP9302: Input Parameter Not Consumed

**ID:** `NP9302`
**Severity:** Error  
**Category:** Data Processing  

This analyzer detects when a SinkNode implementation overrides ExecuteAsync but doesn't consume the input parameter. Sink nodes are designed to process all items from the input data pipe, but your implementation ignores the input.

#### Why This Matters

SinkNode is the terminal component in a pipeline that processes all data flowing through it. When a SinkNode doesn't consume its input:

1. **Data Loss**: Items in the input pipe are never processed
2. **Pipeline Inefficiency**: The pipeline moves data but the sink doesn't handle it
3. **Resource Waste**: Memory and processing are used to move data that's never consumed
4. **Unexpected Behavior**: Applications may appear to work but silently ignore data

#### Solution: Always Consume Input

```csharp
// CORRECT: Process all items from input
public class MySinkNode : SinkNode<string>
{
    public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Processing: {item}");
        }
    }
}
```

## Configuration

Adjust analyzer severity in `.editorconfig`:

```ini
# Treat non-streaming patterns as errors
dotnet_diagnostic.NP9205.severity = error

# Treat unconsumed input as errors
dotnet_diagnostic.NP9302.severity = error
```

## See Also

- [Streaming vs Buffering](../../core-concepts/streaming-vs-buffering)
- [Data Pipes](../../core-concepts/data-pipes)
- [Performance Characteristics](../../architecture/performance-characteristics)
