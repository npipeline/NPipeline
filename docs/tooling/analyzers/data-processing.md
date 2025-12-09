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

#### Problematic Patterns

```csharp
// :x: PROBLEM: Materializing all data in memory
public class BadSourceNode : SourceNode<string>
{
    public override IDataPipe<string> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        var output = new DataPipe<string>();
        
        // NP9205: Allocating List<T> and populating it
        var items = new List<string>();
        
        // Read all lines from file into memory
        var lines = File.ReadAllLines("large-file.txt"); // NP9205: Synchronous I/O
        
        foreach (var line in lines)
        {
            items.Add(line);
        }
        
        // NP9205: Materializing collection with ToList()
        foreach (var item in items.ToList())
        {
            output.Produce(item);
        }
        
        return output;
    }
}

// :x: PROBLEM: Using ToAsyncEnumerable on materialized collection
public class AnotherBadSourceNode : SourceNode<int>
{
    public override IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        var output = new DataPipe<int>();
        
        // NP9205: Creating array and then converting to async enumerable
        var numbers = Enumerable.Range(0, 1000000).ToArray(); // NP9205: Array allocation
        
        // NP9205: Using ToAsyncEnumerable on materialized collection
        foreach (var number in numbers.ToAsyncEnumerable())
        {
            output.Produce(number);
        }
        
        return output;
    }
}
```

#### Solution: Use Streaming Patterns

For SourceNode implementations, use IAsyncEnumerable with yield return for proper streaming:

```csharp
// :heavy_check_mark: CORRECT: Using IAsyncEnumerable with yield return
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
            yield return line; // Stream one line at a time
        }
    }
}

// :heavy_check_mark: CORRECT: Streaming from database
public class DatabaseSourceNode : SourceNode<DataRecord>
{
    private readonly IDbConnection _connection;
    
    public DatabaseSourceNode(IDbConnection connection)
    {
        _connection = connection;
    }
    
    public override IDataPipe<DataRecord> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        return new StreamingDataPipe<DataRecord>(ReadRecords(cancellationToken));
    }
    
    private async IAsyncEnumerable<DataRecord> ReadRecords([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var command = _connection.CreateCommand();
        command.CommandText = "SELECT Id, Name FROM DataRecords";
        
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        
        while (await reader.ReadAsync(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new DataRecord
            {
                Id = reader.GetInt32(0),
                Name = reader.GetString(1)
            };
        }
    }
}

// :heavy_check_mark: CORRECT: Generating data stream without materialization
public class NumberGeneratorSourceNode : SourceNode<int>
{
    private readonly int _start;
    private readonly int _count;
    
    public NumberGeneratorSourceNode(int start, int count)
    {
        _start = start;
        _count = count;
    }
    
    public override IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        return new StreamingDataPipe<int>(GenerateNumbers(cancellationToken));
    }
    
    private async IAsyncEnumerable<int> GenerateNumbers([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        for (int i = 0; i < _count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return _start + i;
            
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
// :heavy_check_mark: GOOD: Streaming with transformation
public class TransformingSourceNode : SourceNode<ProcessedData>
{
    public override IDataPipe<ProcessedData> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        return new StreamingDataPipe<ProcessedData>(TransformItems(cancellationToken));
    }
    
    private async IAsyncEnumerable<ProcessedData> TransformItems([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var rawItem in GetRawItemsAsync(cancellationToken))
        {
            // Transform item without materializing the entire collection
            var processedItem = ProcessItem(rawItem);
            yield return processedItem;
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
// :heavy_check_mark: GOOD: Streaming with filtering
public class FilteringSourceNode : SourceNode<FilteredData>
{
    private readonly Func<DataItem, bool> _filter;
    
    public FilteringSourceNode(Func<DataItem, bool> filter)
    {
        _filter = filter;
    }
    
    public override IDataPipe<FilteredData> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        return new StreamingDataPipe<FilteredData>(FilterItems(cancellationToken));
    }
    
    private async IAsyncEnumerable<FilteredData> FilterItems([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in GetAllItemsAsync(cancellationToken))
        {
            // Filter items without materializing the entire collection
            if (_filter(item))
            {
                yield return new FilteredData(item);
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

#### Implementation Guide

To implement streaming SourceNode implementations:

1. **Identify non-streaming patterns** using the NP9205 analyzer
2. **Replace List and Array allocations** with `IAsyncEnumerable` methods
3. **Convert synchronous I/O** to async equivalents (`File.ReadAllText` → `File.ReadAllTextAsync`)
4. **Remove .ToAsyncEnumerable()** calls on materialized collections
5. **Use yield return** to stream items one at a time
6. **Add cancellation support** with `[EnumeratorCancellation]` attribute

### NP9302: Input Parameter Not Consumed

**ID:** `NP9302`
**Severity:** Error  
**Category:** Data Processing  

This analyzer detects when a SinkNode implementation overrides ExecuteAsync but doesn't consume the input parameter. Sink nodes are designed to process all items from the input data pipe, but your implementation ignores the input.

This analyzer identifies these problematic patterns:

1. **SinkNode.ExecuteAsync override without input consumption** - The method doesn't use the input parameter
2. **Empty ExecuteAsync implementation** - The method returns without processing input
3. **ExecuteAsync with only side effects** - The method performs operations but ignores input data

#### Why This Matters (NP9210)

SinkNode is the terminal component in a pipeline that processes all data flowing through it. When a SinkNode doesn't consume its input:

1. **Data Loss**: Items in the input pipe are never processed
2. **Pipeline Inefficiency**: The pipeline moves data but the sink doesn't handle it
3. **Resource Waste**: Memory and processing are used to move data that's never consumed
4. **Unexpected Behavior**: Applications may appear to work but silently ignore data

#### Solution: Always Consume Input

```csharp
// :heavy_check_mark: CORRECT: Process all items from input
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

// :heavy_check_mark: CORRECT: Use DataPipe operations
public class CountingSinkNode : SinkNode<string>
{
    public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        // Count all items
        var count = await input.CountAsync(cancellationToken);
        Console.WriteLine($"Total items processed: {count}");
    }
}

// :heavy_check_mark: CORRECT: Handle empty input gracefully
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

// :heavy_check_mark: CORRECT: Conditional processing with default input consumption
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

### NP9210: StreamTransformNode Suggestion

**ID:** `NP9210`
**Severity:** Info  
**Category:** Design  

This analyzer detects when a class implements `ITransformNode<TIn, TOut>` but the `TOut` generic argument is `IAsyncEnumerable<T>`, meaning `ExecuteAsync` returns `Task<IAsyncEnumerable<T>>`. It suggests using `IStreamTransformNode` instead for better interface segregation and optimized execution.

#### Why This Matters

When a transform node's `ExecuteAsync` method returns `Task<IAsyncEnumerable<T>>`, it indicates that the node is performing stream-based transformations. Using `IStreamTransformNode` instead of `ITransformNode` provides several benefits:

1. **Better Interface Segregation**: `IStreamTransformNode` is specifically designed for stream-based transformations
2. **Clearer Intent**: Makes it obvious that the node processes streams rather than individual items
3. **Optimized Execution**: Allows the pipeline to use stream-specific execution strategies
4. **Type Safety**: Ensures proper handling of streaming data patterns

#### Problematic Pattern (NP9210)

```csharp
using System.Globalization;

// :x: PROBLEM: Using ITransformNode with IAsyncEnumerable return type
public class DataProcessor : ITransformNode<string, IAsyncEnumerable<int>>
{
    public Task<IAsyncEnumerable<int>> ExecuteAsync(
        string input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Each call returns a stream, which violates the single-output contract
        return Task.FromResult(ParseNumbersAsync(input));
    }

    private static async IAsyncEnumerable<int> ParseNumbersAsync(string csv)
    {
        foreach (var token in csv.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            yield return int.Parse(token, CultureInfo.InvariantCulture);
            await Task.Yield();
        }
    }
}
```

#### Solution: Use IStreamTransformNode

```csharp
// :heavy_check_mark: CORRECT: Using IStreamTransformNode for stream-based transformations
public class DataProcessor : IStreamTransformNode<InputData, OutputData>
{
    public async IAsyncEnumerable<OutputData> ExecuteAsync(
        IAsyncEnumerable<InputData> input, 
        PipelineContext context, 
        CancellationToken cancellationToken)
    {
        // Process input stream and return output stream
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            foreach (var result in ProcessInput(item))
            {
                yield return result;
            }
        }
    }
}
```

#### Code Fix (NP9210)

The analyzer provides a code fix that automatically:

1. Changes the base interface from `ITransformNode<TIn, TOut>` to `IStreamTransformNode<TIn, TOut>`
2. Updates the `ExecuteAsync` method signature:
    - Changes the first parameter from `TInput item` to `IAsyncEnumerable<TInput> input`
    - Changes the return type to `IAsyncEnumerable<TOutput>` (no `Task` wrapper)
3. Adds the necessary using statement for `System.Collections.Generic` if not present

#### When to Use Each Interface

| Scenario | Recommended Interface | Reason |
|----------|----------------------|---------|
| Single input → Single output | `ITransformNode<TIn, TOut>` | Simple one-to-one transformation |
| Single input → Multiple outputs | `IStreamTransformNode<TIn, TOut>` | Stream-based transformation |
| Stream input → Stream output | `IStreamTransformNode<TIn, TOut>` | Optimized for streaming |
| Batch processing | `IStreamTransformNode<TIn, TOut>` | Better memory efficiency |

*Reasoning:* NP9210 highlights transform nodes that secretly produce streams so they can adopt `IStreamTransformNode` and unlock stream-aware execution strategies.

### NP9211: StreamTransformNode Execution Strategy Mismatch

**ID:** `NP9211`
**Severity:** Warning  
**Category:** Design  

This analyzer detects when a class implements `IStreamTransformNode` but uses an execution strategy that doesn't implement `IStreamExecutionStrategy`. It warns about potential performance issues from using non-stream-optimized execution strategies.

#### Why This Matters (NP9211)

`IStreamTransformNode` is designed to work with execution strategies that implement `IStreamExecutionStrategy`. Using a regular `IExecutionStrategy` may result in:

1. **Suboptimal Performance**: Non-stream strategies cannot take advantage of stream-specific optimizations
2. **Inefficient Memory Usage**: May buffer entire streams instead of processing them incrementally
3. **Reduced Throughput**: Cannot leverage streaming parallelism and backpressure handling
4. **Poor Cancellation Support**: Stream strategies provide better cancellation propagation

#### Problematic Patterns (NP9211)

```csharp
using System.Globalization;

// :x: PROBLEM: Using non-stream execution strategy with IStreamTransformNode
public class StreamProcessor : IStreamTransformNode<InputData, OutputData>
{
    // NP9211: RegularExecutionStrategy doesn't implement IStreamExecutionStrategy
    public IExecutionStrategy ExecutionStrategy { get; } = new RegularExecutionStrategy();
    
    public async IAsyncEnumerable<OutputData> ExecuteAsync(
        IAsyncEnumerable<InputData> input, 
        PipelineContext context, 
        CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            yield return ProcessItem(item);
        }
    }
}

// :x: PROBLEM: Setting execution strategy in constructor
public class AnotherStreamProcessor : IStreamTransformNode<string, int>
{
    public IExecutionStrategy ExecutionStrategy { get; }
    
    public AnotherStreamProcessor()
    {
        // NP9211: SimpleExecutionStrategy doesn't implement IStreamExecutionStrategy
        ExecutionStrategy = new SimpleExecutionStrategy();
    }
    
    public async IAsyncEnumerable<int> ExecuteAsync(
        IAsyncEnumerable<string> input, 
        PipelineContext context, 
        CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            yield return int.Parse(item, CultureInfo.InvariantCulture);
        }
    }
}
```

#### Solution: Use Stream-Optimized Execution Strategies

```csharp
using System.Globalization;

// :heavy_check_mark: CORRECT: Using BatchingExecutionStrategy with IStreamTransformNode
public class StreamProcessor : IStreamTransformNode<InputData, OutputData>
{
    // BatchingExecutionStrategy implements both IExecutionStrategy and IStreamExecutionStrategy
    public IExecutionStrategy ExecutionStrategy { get; } = new BatchingExecutionStrategy(100, TimeSpan.FromSeconds(1));
    
    public async IAsyncEnumerable<OutputData> ExecuteAsync(
        IAsyncEnumerable<InputData> input, 
        PipelineContext context, 
        CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            yield return ProcessItem(item);
        }
    }
}

// :heavy_check_mark: CORRECT: Using UnbatchingExecutionStrategy for individual item processing
public class ItemProcessor : IStreamTransformNode<string, int>
{
    // UnbatchingExecutionStrategy implements IStreamExecutionStrategy
    public IExecutionStrategy ExecutionStrategy { get; } = new UnbatchingExecutionStrategy();
    
    public async IAsyncEnumerable<int> ExecuteAsync(
        IAsyncEnumerable<string> input, 
        PipelineContext context, 
        CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            yield return int.Parse(item, CultureInfo.InvariantCulture);
        }
    }
}
```

#### Available Stream Execution Strategies

| Strategy | When to Use | Benefits |
|----------|--------------|----------|
| `BatchingExecutionStrategy` | When you can process items in batches for better throughput | Reduces overhead, improves throughput |
| `UnbatchingExecutionStrategy` | When items must be processed individually | Preserves item ordering, simpler processing |
| Custom strategy implementing `IStreamExecutionStrategy` | When you need specialized behavior | Tailored to specific use case |

#### Code Fix (NP9211)

The analyzer provides two code fix options:

1. **Replace with BatchingExecutionStrategy**: Creates a new `BatchingExecutionStrategy` with default parameters (batch size: 100, timeout: 1 second)
2. **Replace with UnbatchingExecutionStrategy**: Creates a new `UnbatchingExecutionStrategy` with no parameters

#### Creating Custom Stream Execution Strategies

```csharp
// :heavy_check_mark: CORRECT: Custom strategy implementing IExecutionStrategy, IStreamExecutionStrategy
public class CustomStreamExecutionStrategy : IExecutionStrategy, IStreamExecutionStrategy
{
    public async Task ExecuteAsync<TInput, TOutput>(
        IAsyncEnumerable<TInput> input,
        IAsyncEnumerable<TOutput> output,
        Func<TInput, CancellationToken, Task<TOutput>> executeFunc,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Custom stream processing logic
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            var result = await executeFunc(item, cancellationToken);
            // Process result in stream-optimized way
        }
    }
    
    // Implement IExecutionStrategy members for compatibility
    public async Task ExecuteAsync<TInput, TOutput>(
        TInput input,
        IAsyncEnumerable<TOutput> output,
        Func<TInput, CancellationToken, Task<TOutput>> executeFunc,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Fallback implementation for non-stream scenarios
        var result = await executeFunc(input, cancellationToken);
        // Process single result
    }
}
```

*Reasoning:* NP9211 ensures every `IStreamTransformNode` pairs with a stream-aware execution strategy, preventing buffering and preserving streaming optimizations.

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
dotnet_diagnostic.NP9205.severity = error

# Treat unconsumed input as errors
dotnet_diagnostic.NP9302.severity = error

# Treat StreamTransformNode suggestion as info
dotnet_diagnostic.NP9210.severity = info

# Treat StreamTransformNode execution strategy mismatch as warnings
dotnet_diagnostic.NP9211.severity = warning
```

## See Also

- [Streaming vs Buffering](../../core-concepts/streaming-vs-buffering.md)
- [Data Pipes](../../core-concepts/data-pipes.md)
- [Performance Characteristics](../../architecture/performance-characteristics.md)
