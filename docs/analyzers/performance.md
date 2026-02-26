---
title: Performance Analyzers
description: Identify blocking operations, non-streaming patterns, and async/await anti-patterns that harm performance.
sidebar_position: 3
---

## Performance Analyzers

Performance analyzers detect patterns that harm throughput, increase latency, cause thread starvation, or prevent proper streaming of data. These violations directly contradict NPipeline's core mission of high-performance, non-blocking I/O.

### NP9101: Blocking Operations in Async Methods

**ID:** `NP9101`
**Severity:** Warning  
**Category:** Performance  

This analyzer detects blocking operations in async methods that can lead to deadlocks, thread pool starvation, and reduced performance. The analyzer identifies the following blocking patterns:

1. **Task.Result and Task.Wait()** calls that block the current thread
2. **GetAwaiter().GetResult()** patterns that synchronously wait for task completion
3. **Thread.Sleep()** in async methods (should use Task.Delay instead)
4. **Synchronous file I/O operations** (File.ReadAllText, File.WriteAllBytes, etc.)
5. **Synchronous network I/O operations** (WebClient.DownloadString, unawaited HttpClient calls)
6. **Unawaited StreamReader/Writer operations** (ReadToEnd, WriteLine without await)

#### Why This Matters

Blocking operations in async code:

- **Cause deadlocks** in certain synchronization contexts (UI threads, ASP.NET Classic)
- **Starve the thread pool** by blocking threads that should be available for other work
- **Reduce scalability** because you can only handle as many concurrent operations as you have threads
- **Increase latency** because blocked threads can't process other work
- **Contradict async design** and defeat the purpose of asynchronous I/O

#### Problematic Patterns

```csharp
// PROBLEM: Blocking on Task.Result
public async Task<string> ProcessDataAsync()
{
    var task = SomeOperationAsync();
    return task.Result; // NP9101: Blocks until task completes
}

// PROBLEM: Blocking on Task.Wait()
public async Task ProcessDataAsync()
{
    var task = SomeOperationAsync();
    task.Wait(); // NP9101: Blocks until task completes
}

// PROBLEM: Using GetAwaiter().GetResult()
public async Task<string> ProcessDataAsync()
{
    var task = SomeOperationAsync();
    return task.GetAwaiter().GetResult(); // NP9101: Synchronous blocking
}

// PROBLEM: Synchronous I/O in async method
public async Task ProcessFileAsync()
{
    var content = File.ReadAllText("file.txt"); // NP9101: Synchronous I/O
    await ProcessAsync(content);
}

// PROBLEM: Thread.Sleep instead of Task.Delay
public async Task WaitAsync()
{
    Thread.Sleep(1000); // NP9101: Blocks the thread
    await ContinueAsync();
}
```

#### Solution: Use await

```csharp
// CORRECT: Use await
public async Task<string> ProcessDataAsync()
{
    var task = SomeOperationAsync();
    return await task; // Properly awaits without blocking
}

// CORRECT: Use async I/O
public async Task ProcessFileAsync()
{
    var content = await File.ReadAllTextAsync("file.txt"); // Async I/O
    await ProcessAsync(content);
}

// CORRECT: Use Task.Delay
public async Task WaitAsync()
{
    await Task.Delay(1000); // Non-blocking delay
    await ContinueAsync();
}
```

### NP9102: Synchronous over Async Patterns

**ID:** `NP9102`
**Severity:** Warning  
**Category:** Performance  

This analyzer detects "sync-over-async" patterns like unawaited async method calls or fire-and-forget operations. These patterns create unpredictable behavior and make it impossible to wait for completion or handle errors.

#### Problematic Sync-Over-Async Patterns

```csharp
// PROBLEM: Fire-and-forget async call (unawaited)
public async Task ProcessDataAsync()
{
    SomeOperationAsync(); // NP9102: Async method not awaited
    DoSomethingElse();
}

// PROBLEM: Async method called from sync method
public void ProcessData()
{
    var result = SomeOperationAsync(); // NP9102: Async method not awaited
}

// PROBLEM: Task.Run wrapping sync work
public async Task ProcessDataAsync()
{
    var result = await Task.Run(() => 
    {
        return SomeSynchronousOperation(); // NP9102: Unnecessary Task.Run
    });
}
```

#### Solution: Always Await

```csharp
// CORRECT: Await the async call
public async Task ProcessDataAsync()
{
    await SomeOperationAsync(); // Properly awaited
    DoSomethingElse();
}

// CORRECT: Make calling method async
public async Task ProcessDataAsync()
{
    var result = await SomeOperationAsync(); // Properly awaited
}

// CORRECT: Call sync methods directly
public async Task ProcessDataAsync()
{
    var result = SomeSynchronousOperation(); // Direct call, no Task.Run
    await ProcessResultAsync(result);
}
```

### NP9103: LINQ Operations in Hot Paths

**ID:** `NP9103`
**Severity:** Warning
**Category:** Performance

This analyzer detects LINQ operations in high-frequency execution paths that cause unnecessary allocations and GC pressure, significantly impacting performance in high-throughput NPipeline scenarios.

#### Why This Matters

LINQ in hot paths causes:

1. **Excessive Allocations**: Each LINQ operation creates intermediate collections
2. **GC Pressure**: Frequent garbage collection reduces throughput
3. **Poor Performance**: Overhead of delegates and iterators
4. **Memory Fragmentation**: Many small objects fragment the heap

#### Problematic Patterns

```csharp
// PROBLEM: LINQ in ExecuteAsync method
public class BadTransform : ITransformNode<Input, Output>
{
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        // NP9103: LINQ in hot path creates allocations
        var filtered = input.Items.Where(x => x.IsActive).ToList();
        var sorted = filtered.OrderBy(x => x.Priority).ToList();
        var grouped = sorted.GroupBy(x => x.Category).ToList();
        
        return new Output(grouped);
    }
}

// PROBLEM: LINQ in loop
foreach (var batch in batches)
{
    // NP9103: LINQ inside loop creates pressure
    var processed = batch.Select(x => ProcessItem(x)).Where(x => x != null).ToList();
    await SendBatchAsync(processed);
}

// PROBLEM: Materializing LINQ results
var items = sourceData.Where(x => x.IsValid).Select(x => x.Transform()).ToArray(); // NP9103: Immediate materialization
```

#### Solution: Use Imperative Alternatives

```csharp
// CORRECT: Use imperative processing
public class GoodTransform : ITransformNode<Input, Output>
{
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        var filtered = new List<Item>();
        foreach (var item in input.Items)
        {
            if (item.IsActive)
                filtered.Add(item);
        }
        
        filtered.Sort((x, y) => x.Priority.CompareTo(y.Priority));
        
        var grouped = new Dictionary<string, List<Item>>();
        foreach (var item in filtered)
        {
            if (!grouped.ContainsKey(item.Category))
                grouped[item.Category] = new List<Item>();
            grouped[item.Category].Add(item);
        }
        
        return new Output(grouped.Values.ToList());
    }
}

// CORRECT: Process items directly in loop
foreach (var batch in batches)
{
    var processed = new List<Item>();
    foreach (var item in batch)
    {
        var result = ProcessItem(item);
        if (result != null)
            processed.Add(result);
    }
    await SendBatchAsync(processed);
}
```

#### LINQ Alternatives Guidelines

| LINQ Operation | Imperative Alternative | Performance Benefit |
|----------------|----------------------|-------------------|
| Where() | foreach with if | No intermediate collection |
| Select() | foreach with transformation | No delegate overhead |
| OrderBy() | Sort() with comparer | In-place sorting |
| GroupBy() | Dictionary grouping | Direct grouping |
| ToList()/ToArray() | Pre-sized collection | No resizing |

### NP9104: Inefficient String Operations

**ID:** `NP9104`
**Severity:** Warning
**Category:** Performance

This analyzer detects inefficient string operations that cause excessive allocations and GC pressure in performance-critical NPipeline code, particularly in high-throughput scenarios.

#### Why This Matters

Inefficient string operations cause:

1. **Memory Pressure**: Excessive allocations increase GC frequency
2. **Poor Performance**: String operations are expensive in hot paths
3. **Reduced Throughput**: Time spent on string operations reduces processing capacity
4. **Scalability Issues**: Performance degrades with increased load

#### Problematic Patterns

```csharp
// PROBLEM: String concatenation in loop
public class BadTransform : ITransformNode<Input, Output>
{
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        string result = "";
        foreach (var item in input.Items) // NP9104: Concatenation in loop
        {
            result += item.ToString(); // Creates new string each iteration
        }
        return new Output(result);
    }
}

// PROBLEM: Inefficient string formatting
protected override async Task<string> ProcessAsync(Data data, CancellationToken cancellationToken)
{
    return string.Format("{0}-{1}-{2}", data.Id, data.Name, data.Value); // NP9104: Inefficient formatting
}

// PROBLEM: String operations in LINQ
var results = items.Select(x => x.Name.ToUpper().Substring(0, 5).Trim()); // NP9104: Multiple allocations per item
```

#### Solution: Use Efficient String Operations

```csharp
// CORRECT: Use StringBuilder for concatenation
public class GoodTransform : ITransformNode<Input, Output>
{
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        var sb = new StringBuilder();
        foreach (var item in input.Items)
        {
            sb.Append(item.ToString());
        }
        return new Output(sb.ToString());
    }
}

// CORRECT: Use string interpolation
protected override async Task<string> ProcessAsync(Data data, CancellationToken cancellationToken)
{
    return $"{data.Id}-{data.Name}-{data.Value}"; // Efficient formatting
}

// CORRECT: Use span-based operations
protected override async Task<string> ProcessAsync(string input, CancellationToken cancellationToken)
{
    return input.AsSpan().Slice(0, Math.Min(5, input.Length)).Trim().ToString(); // Zero-allocation where possible
}
```

#### String Operation Guidelines

| Operation | Efficient Alternative | When to Use |
|------------|----------------------|--------------|
| Concatenation in loop | StringBuilder | Multiple concatenations |
| String.Format | Interpolation | Simple formatting |
| Substring/Trim | AsSpan().Slice() | Hot paths |
| ToUpper/ToLower | string.Create with Span | Case conversion in hot paths |
| Join | string.Join with Span | Array/list joining |

### NP9106: Missing ValueTask Optimization

**ID:** `NP9106`
**Severity:** Warning
**Category:** Performance

This analyzer detects async methods that return Task instead of ValueTask, which can lead to unnecessary allocations and reduced performance in hot paths.

#### Why This Matters

Using Task instead of ValueTask in hot paths:

1. **Increases GC Pressure**: Each Task object creates heap allocation
2. **Reduces Throughput**: Additional boxing and unboxing operations
3. **Decreases Cache Locality**: Task objects have additional metadata

#### Problematic Patterns

```csharp
// PROBLEM: Async method returning Task instead of ValueTask
public async Task ProcessDataAsync()
{
    // NP9106: Async method returning Task instead of ValueTask
    var result = await SomeAsyncOperation();
    return result;
}
```

#### Solution: Use ValueTask

```csharp
// CORRECT: Use ValueTask
public async ValueTask ProcessDataAsync()
{
    // Properly awaited
    var result = await SomeAsyncOperation();
    return result;
}
```

### NP9105: Anonymous Object Allocation

**ID:** `NP9105`
**Severity:** Warning
**Category:** Performance

This analyzer detects anonymous object creation in performance-critical NPipeline code that causes unnecessary GC pressure and allocation overhead, particularly in high-throughput scenarios.

#### Why This Matters

Anonymous object allocations cause:

1. **GC Pressure**: Each anonymous object creates heap allocation
2. **Memory Overhead**: Anonymous objects have additional metadata
3. **Poor Cache Locality**: Scattered object references
4. **Reduced Throughput**: Time spent in garbage collection

#### Problematic Patterns

```csharp
// PROBLEM: Anonymous objects in ExecuteAsync
protected override async Task ExecuteAsync(IDataPipe<Output> output, PipelineContext context, CancellationToken cancellationToken)
{
    foreach (var item in inputItems)
    {
        // NP9105: Anonymous object allocation in hot path
        var result = new { Id = item.Id, Name = item.Name, Value = item.Value * 2 };
        await output.ProduceAsync(new Output(result), cancellationToken);
    }
}

// PROBLEM: Anonymous objects in LINQ
var processed = items.Select(x => new // NP9105: Anonymous object in LINQ
{
    Id = x.Id,
    Name = x.Name
}).ToList();

// PROBLEM: Anonymous objects in loops
foreach (var item in items)
{
    // NP9105: Anonymous object allocation per iteration
    var temp = new { Original = item, Processed = Process(item) };
}
```

#### Solution: Use Named Types or Value Types

```csharp
// CORRECT: Define named type for results
public record ProcessedItem(int Id, string Name, double Value);

protected override async Task ExecuteAsync(IDataPipe<Output> output, PipelineContext context, CancellationToken cancellationToken)
{
    foreach (var item in inputItems)
    {
        var result = new ProcessedItem(item.Id, item.Name, item.Value * 2);
        await output.ProduceAsync(new Output(result), cancellationToken);
    }
}

// CORRECT: Use named type in LINQ
public record ProcessedData(int Id, double ProcessedValue, DateTime Timestamp);

var processed = items.Select(x => new ProcessedData(
    x.Id,
    x.Value * 2,
    DateTime.UtcNow)).ToList();

// CORRECT: Use struct for value-type data
public readonly struct ProcessedItem
{
    public readonly int Id;
    public readonly double ProcessedValue;
    
    public ProcessedItem(int id, double processedValue)
    {
        Id = id;
        ProcessedValue = processedValue;
    }
}
```

#### Anonymous Object Alternatives

| Scenario | Recommended Alternative | Benefit |
|----------|----------------------|----------|
| Temporary data transfer | Named record/class | Type safety, reuse |
| Key-value pairs | Tuple or struct | Stack allocation for structs |
| Multiple return values | Out parameters or struct | No heap allocation |
| LINQ projections | Named type constructor | Clearer intent |

### NP9107: Non-Streaming Patterns in SourceNode

**ID:** `NP9107`
**Severity:** Warning
**Category:** Performance

This analyzer detects non-streaming patterns in SourceNode implementations that can lead to memory issues and poor performance. See the [Data Processing Analyzers](./data-processing.md) section for detailed information about this analyzer.

#### Problematic Patterns

```csharp
// PROBLEM: Non-streaming implementation
public class BadSourceNode : ISourceNode<Output>
{
    public override IDataPipe<Output> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        var items = LoadAllItems(); // Loads everything into memory
        return new StreamingDataPipe<Output>(items.ToAsyncEnumerable());
    }
}
```

#### Solution: Use Streaming Patterns

```csharp
// CORRECT: Streaming implementation
public class GoodSourceNode : ISourceNode<Output>
{
    public override IDataPipe<Output> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        return new StreamingDataPipe<Output>(GetItemsAsync(cancellationToken));
    }
    
    private async IAsyncEnumerable<Output> GetItemsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Stream items one at a time
        await foreach (var item in _dataSource.GetItemsAsync(cancellationToken))
        {
            yield return ProcessItem(item);
        }
    }
}
```

## Best Practices for Performance

1. **Always use await** - Never block on async code with .Result, .Wait(), or .GetResult()
2. **Respect cancellation tokens** - Check them frequently and pass them to all async operations
3. **Never swallow OperationCanceledException** - Always re-throw or handle it appropriately
4. **Use ValueTask for sync-heavy paths** - Avoid unnecessary allocations
5. **Use async all the way down** - Don't mix sync and async code
6. **Use ConfigureAwait(false) in library code** - Improves performance and prevents deadlocks

## Configuration

Adjust analyzer severity in `.editorconfig`:

```ini
# Treat blocking operations as errors
dotnet_diagnostic.NP9101.severity = error

# Treat swallowed cancellation as errors
dotnet_diagnostic.NP9201.severity = error

# Treat fire-and-forget async as errors
dotnet_diagnostic.NP9102.severity = error

# Treat ignored cancellation tokens as errors
dotnet_diagnostic.NP9203.severity = error

# Treat LINQ in hot paths as warnings
dotnet_diagnostic.NP9103.severity = warning

# Treat inefficient string operations as warnings
dotnet_diagnostic.NP9104.severity = warning

# Treat anonymous object allocation as warnings
dotnet_diagnostic.NP9105.severity = warning

# Treat missing ValueTask optimization as warnings
dotnet_diagnostic.NP9106.severity = warning

# Treat non-streaming patterns as errors
dotnet_diagnostic.NP9107.severity = error
```

## See Also

- [Performance Hygiene](../../advanced-topics/performance-hygiene)
- [Performance Characteristics](../../architecture/performance-characteristics)
