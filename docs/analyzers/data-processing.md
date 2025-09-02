---
title: Data Processing Analyzers
description: Ensure proper input consumption and streaming patterns in pipeline nodes.
sidebar_position: 4
---

## Data Processing Analyzers

Data processing analyzers protect the integrity of data flow through your pipelines. They detect patterns that cause data loss, memory bloat, or improper stream handling.

### NP9211: Non-Streaming Patterns in SourceNode

**ID:** `NP9211`  
**Severity:** Warning  
**Category:** Performance  

This analyzer detects non-streaming patterns in SourceNode implementations that can lead to memory issues and poor performance. The analyzer identifies the following problematic patterns:

1. **List and Array allocation and population** in ExecuteAsync methods
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

#### Problematic Patterns

```csharp
// ❌ PROBLEM: Materializing all data in memory
public class BadSourceNode : SourceNode<string>
{
    protected override async Task ExecuteAsync(IDataPipe<string> output, PipelineContext context, CancellationToken cancellationToken)
    {
        // NP9211: Allocating List<T> and populating it
        var items = new List<string>();
        
        // Read all lines from file into memory
        var lines = File.ReadAllLines("large-file.txt"); // NP9211: Synchronous I/O
        
        foreach (var line in lines)
        {
            items.Add(line);
        }
        
        // NP9211: Materializing collection with ToList()
        foreach (var item in items.ToList())
        {
            await output.ProduceAsync(item, cancellationToken);
        }
    }
}

// ❌ PROBLEM: Using ToAsyncEnumerable on materialized collection
public class AnotherBadSourceNode : SourceNode<int>
{
    protected override async Task ExecuteAsync(IDataPipe<int> output, PipelineContext context, CancellationToken cancellationToken)
    {
        // NP9211: Creating array and then converting to async enumerable
        var numbers = Enumerable.Range(0, 1000000).ToArray(); // NP9211: Array allocation
        
        // NP9211: Using ToAsyncEnumerable on materialized collection
        await foreach (var number in numbers.ToAsyncEnumerable())
        {
            await output.ProduceAsync(number, cancellationToken);
        }
    }
}
```

#### Solution: Use Streaming Patterns

For SourceNode implementations, use async IAsyncEnumerable with yield return for proper streaming:

```csharp
// ✅ CORRECT: Using IAsyncEnumerable with yield return
public class GoodSourceNode : SourceNode<string>
{
    protected override async Task ExecuteAsync(IDataPipe<string> output, PipelineContext context, CancellationToken cancellationToken)
    {
        // Stream data line by line without materializing in memory
        await foreach (var line in ReadLinesAsync("large-file.txt", cancellationToken))
        {
            await output.ProduceAsync(line, cancellationToken);
        }
    }
    
    // Helper method that yields lines one at a time
    private async IAsyncEnumerable<string> ReadLinesAsync(string filePath, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var reader = new StreamReader(
            new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 4096, useAsync: true));
        
        string? line;
        while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
        {
            yield return line; // Stream one line at a time
        }
    }
}

// ✅ CORRECT: Streaming from database
public class DatabaseSourceNode : SourceNode<DataRecord>
{
    private readonly IDbConnection _connection;
    
    public DatabaseSourceNode(IDbConnection connection)
    {
        _connection = connection;
    }
    
    protected override async Task ExecuteAsync(IDataPipe<DataRecord> output, PipelineContext context, CancellationToken cancellationToken)
    {
        await using var command = _connection.CreateCommand();
        command.CommandText = "SELECT Id, Name FROM DataRecords";
        
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        
        while (await reader.ReadAsync(cancellationToken))
        {
            var record = new DataRecord
            {
                Id = reader.GetInt32(0),
                Name = reader.GetString(1)
            };
            
            await output.ProduceAsync(record, cancellationToken);
        }
    }
}

// ✅ CORRECT: Generating data stream without materialization
public class NumberGeneratorSourceNode : SourceNode<int>
{
    private readonly int _start;
    private readonly int _count;
    
    public NumberGeneratorSourceNode(int start, int count)
    {
        _start = start;
        _count = count;
    }
    
    protected override async Task ExecuteAsync(IDataPipe<int> output, PipelineContext context, CancellationToken cancellationToken)
    {
        for (int i = 0; i < _count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await output.ProduceAsync(_start + i, cancellationToken);
            
            // Optional: Add small delay to prevent overwhelming downstream nodes
            if (i % 1000 == 0)
            {
                await Task.Delay(1, cancellationToken);
            }
        }
    }
}
```

#### Advanced Streaming Patterns

##### Streaming with Transformation

```csharp
// ✅ GOOD: Streaming with transformation
public class TransformingSourceNode : SourceNode<ProcessedData>
{
    protected override async Task ExecuteAsync(IDataPipe<ProcessedData> output, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var rawItem in GetRawItemsAsync(cancellationToken))
        {
            // Transform item without materializing the entire collection
            var processedItem = ProcessItem(rawItem);
            await output.ProduceAsync(processedItem, cancellationToken);
        }
    }
    
    private async IAsyncEnumerable<RawData> GetRawItemsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Stream raw items from source
        await foreach (var item in ReadFromSourceAsync(cancellationToken))
        {
            yield return item;
        }
    }
    
    private ProcessedData ProcessItem(RawData raw)
    {
        // Synchronous transformation is fine for individual items
        return new ProcessedData
        {
            Id = raw.Id,
            Value = raw.Value * 2,
            Timestamp = DateTime.UtcNow
        };
    }
}
```

##### Streaming with Filtering

```csharp
// ✅ GOOD: Streaming with filtering
public class FilteringSourceNode : SourceNode<FilteredData>
{
    private readonly Func<DataItem, bool> _filter;
    
    public FilteringSourceNode(Func<DataItem, bool> filter)
    {
        _filter = filter;
    }
    
    protected override async Task ExecuteAsync(IDataPipe<FilteredData> output, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in GetAllItemsAsync(cancellationToken))
        {
            // Filter items without materializing the entire collection
            if (_filter(item))
            {
                var filteredItem = new FilteredData(item);
                await output.ProduceAsync(filteredItem, cancellationToken);
            }
        }
    }
    
    private async IAsyncEnumerable<DataItem> GetAllItemsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Stream items from source
        await foreach (var item in ReadFromSourceAsync(cancellationToken))
        {
            yield return item;
        }
    }
}
```

#### When to Use Each Pattern

| Scenario | Recommended Approach | Reason |
|----------|----------------------|---------|
| Large files/streams | `IAsyncEnumerable` with `yield return` | Minimal memory usage |
| Database queries | Stream from database cursor | Avoid loading entire result set |
| API calls with pagination | Page through results | Process data as it arrives |
| Data generation | Generate and yield items | No need to store all items |
| Small datasets (< 1000 items) | Either approach is fine | Memory impact is negligible |

#### Migration Guide

To migrate existing non-streaming SourceNode implementations:

1. **Identify non-streaming patterns** using the NP9211 analyzer
2. **Replace List and Array allocations** with `IAsyncEnumerable` methods
3. **Convert synchronous I/O** to async equivalents (`File.ReadAllText` → `File.ReadAllTextAsync`)
4. **Remove .ToAsyncEnumerable()** calls on materialized collections
5. **Use yield return** to stream items one at a time
6. **Add cancellation support** with `[EnumeratorCancellation]` attribute

### NP9312: Input Parameter Not Consumed

**ID:** `NP9312`  
**Severity:** Error  
**Category:** Data Processing  

This analyzer detects when a SinkNode implementation overrides ExecuteAsync but doesn't consume the input parameter. Sink nodes are designed to process all items from the input data pipe, but your implementation ignores the input.

This analyzer identifies these problematic patterns:

1. **SinkNode.ExecuteAsync override without input consumption** - The method doesn't use the input parameter
2. **Empty ExecuteAsync implementation** - The method returns without processing input
3. **ExecuteAsync with only side effects** - The method performs operations but ignores input data

#### Why This Matters

SinkNode is the terminal component in a pipeline that processes all data flowing through it. When a SinkNode doesn't consume its input:

1. **Data Loss**: Items in the input pipe are never processed
2. **Pipeline Inefficiency**: The pipeline moves data but the sink doesn't handle it
3. **Resource Waste**: Memory and processing are used to move data that's never consumed
4. **Unexpected Behavior**: Applications may appear to work but silently ignore data

#### Solution: Always Consume Input

```csharp
// ✅ CORRECT: Process all items from input
public class MySinkNode : SinkNode<string>
{
    public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        // Process all items from input
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            Console.WriteLine($"Processing: {item}");
            // Save to database, write to file, etc.
        }
    }
}

// ✅ CORRECT: Use DataPipe operations
public class CountingSinkNode : SinkNode<string>
{
    public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        // Count all items
        var count = await input.CountAsync(cancellationToken);
        Console.WriteLine($"Total items processed: {count}");
    }
}

// ✅ CORRECT: Handle empty input gracefully
public class RobustSinkNode : SinkNode<string>
{
    public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        var hasItems = false;
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            hasItems = true;
            Console.WriteLine($"Processing: {item}");
        }
        
        if (!hasItems)
        {
            Console.WriteLine("No items to process");
        }
    }
}

// ✅ CORRECT: Conditional processing with default input consumption
public class ConditionalSinkNode : SinkNode<string>
{
    private readonly bool _shouldProcess;
    
    public ConditionalSinkNode(bool shouldProcess)
    {
        _shouldProcess = shouldProcess;
    }
    
    public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        if (_shouldProcess)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                Console.WriteLine($"Processing: {item}");
            }
        }
        else
        {
            // Always consume input even when not processing
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // Just consume the items
            }
        }
    }
}
```

#### Common Input Consumption Patterns

| Pattern | Example | Use Case |
|---------|---------|----------|
| Process all items | `await foreach (var item in input.WithCancellation(cancellationToken)) { ... }` | Standard processing of each item |
| Count items | `var count = await input.CountAsync(cancellationToken);` | When you only need the count |
| Collect to list | `var items = await input.ToListAsync(cancellationToken);` | When you need all items in memory |
| First item only | `var first = await input.FirstAsync(cancellationToken);` | When you only need the first item |
| Any items check | `var hasItems = await input.AnyAsync(cancellationToken);` | When you just need to check if input is non-empty |

#### Best Practices for SinkNode

1. **Always consume the input** - Use `await foreach` or other data pipe operations
2. **Pass cancellation token** - Use `WithCancellation(cancellationToken)` for proper cancellation support
3. **Handle empty input** - Your code should work correctly even if the input pipe is empty
4. **Consider performance** - For large datasets, process items in a streaming fashion rather than collecting all items
5. **Don't silently ignore input** - Even if you don't need to process items, consume them to acknowledge receipt

## Why Data Processing Analyzers Matter

1. **Memory Efficiency**: Streaming patterns use constant memory regardless of data size
2. **Better Performance**: Processing begins immediately without waiting for all data to load
3. **Scalability**: Can handle arbitrarily large datasets without running out of memory
4. **Data Integrity**: All data flowing through pipelines is properly consumed
5. **Resource Utilization**: Lower GC pressure and better cache locality

## Configuration

Adjust analyzer severity in `.editorconfig`:

```ini
# Treat non-streaming patterns as errors
dotnet_diagnostic.NP9211.severity = error

# Treat unconsummed input as errors
dotnet_diagnostic.NP9312.severity = error
```

## See Also

- [Streaming vs Buffering](../core-concepts/streaming-vs-buffering.md)
- [Data Pipes](../core-concepts/data-pipes.md)
- [Performance Characteristics](../architecture/performance-characteristics.md)
