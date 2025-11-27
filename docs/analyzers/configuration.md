---
title: Configuration Analyzers
description: Detect configuration issues that can cause performance problems, resource leaks, or silent failures in NPipeline pipelines.
sidebar_position: 5
---

## Configuration Analyzers

Configuration analyzers detect issues with pipeline configuration that can lead to performance problems, resource leaks, or silent failures. These analyzers focus on ensuring that your pipeline configuration is optimal for your workload and doesn't introduce hidden problems.

### NP9501: Unbounded Materialization Configuration

**ID:** `NP9501`  
**Severity:** Error  
**Category:** Configuration  

This analyzer detects when `PipelineRetryOptions.MaxMaterializedItems` is null or missing, which causes unbounded memory growth in `ResilientExecutionStrategy` and silently disables restart functionality. This is a critical configuration error that can lead to `OutOfMemoryException` in production.

#### Why This Matters

Unbounded materialization configuration causes:

1. **Memory Leaks**: Unlimited memory growth as items are materialized for retry scenarios
2. **Silent Failures**: Restart functionality is disabled without any indication
3. **Production Crashes**: OutOfMemoryException in high-throughput scenarios
4. **Resource Exhaustion**: System becomes unstable under load

#### Problematic Configuration

```csharp
// ❌ PROBLEM: MaxMaterializedItems not specified (defaults to null)
var retryOptions = new PipelineRetryOptions(
    maxRetryCount: 3,
    baseDelay: TimeSpan.FromSeconds(1),
    maxDelay: TimeSpan.FromMinutes(1));

// ❌ PROBLEM: MaxMaterializedItems explicitly set to null
var retryOptions = new PipelineRetryOptions(
    maxRetryCount: 3,
    baseDelay: TimeSpan.FromSeconds(1),
    maxDelay: TimeSpan.FromMinutes(1),
    maxMaterializedItems: null); // NP9501: Unbounded materialization
```

#### Solution: Set MaxMaterializedItems

```csharp
// ✅ CORRECT: Set reasonable MaxMaterializedItems
var retryOptions = new PipelineRetryOptions(
    maxRetryCount: 3,
    baseDelay: TimeSpan.FromSeconds(1),
    maxDelay: TimeSpan.FromMinutes(1),
    maxMaterializedItems: 1000); // Bounded memory usage

// ✅ CORRECT: Use named parameters for clarity
var retryOptions = new PipelineRetryOptions
{
    MaxRetryCount = 3,
    BaseDelay = TimeSpan.FromSeconds(1),
    MaxDelay = TimeSpan.FromMinutes(1),
    MaxMaterializedItems = 1000 // Prevents unbounded growth
};
```

#### Choosing the Right MaxMaterializedItems Value

| Scenario | Recommended Value | Reason |
|----------|-------------------|---------|
| Small items (< 1KB) | 1000-10000 | Low memory per item |
| Medium items (1KB-10KB) | 100-1000 | Balance memory and performance |
| Large items (> 10KB) | 10-100 | Prevent memory pressure |
| Memory-constrained environments | 10-50 | Conservative approach |
| High-throughput scenarios | 100-1000 | Depends on item size |

### NP9502: Inappropriate Parallelism Configuration

**ID:** `NP9502`  
**Severity:** Warning  
**Category:** Performance  

This analyzer detects inappropriate parallelism configurations that can cause resource contention, thread pool starvation, or suboptimal resource utilization in NPipeline pipelines.

#### Why This Matters

Inappropriate parallelism configuration causes:

1. **Resource Contention**: Too much parallelism competes for limited resources
2. **Thread Pool Starvation**: Excessive parallelism exhausts available threads
3. **Poor Performance**: Suboptimal resource utilization reduces throughput
4. **System Instability**: Overloaded system becomes unpredictable

#### Problematic Configuration

```csharp
// ❌ PROBLEM: High parallelism for CPU-bound workloads
builder.AddTransform<CpuIntensiveTransform, Input, Output>("transform")
    .WithParallelism(Environment.ProcessorCount * 4); // NP9502: Excessive parallelism

// ❌ PROBLEM: PreserveOrdering with high parallelism
var parallelOptions = new ParallelOptions(
    maxDegreeOfParallelism: 16,
    preserveOrdering: true); // NP9502: Ordering overhead with high parallelism

// ❌ PROBLEM: Single-threaded for CPU-bound work
builder.AddTransform<CpuTransform, Input, Output>("transform")
    .WithParallelism(1); // NP9502: Underutilizing CPU resources
```

#### Solution: Match Parallelism to Workload

```csharp
// ✅ CORRECT: Appropriate parallelism for CPU-bound workloads
builder.AddTransform<CpuIntensiveTransform, Input, Output>("transform")
    .WithParallelism(Environment.ProcessorCount); // Match CPU cores

// ✅ CORRECT: Moderate parallelism for I/O-bound workloads
builder.AddTransform<DatabaseTransform, Input, Output>("transform")
    .WithParallelism(Environment.ProcessorCount * 2); // I/O can handle more

// ✅ CORRECT: Disable PreserveOrdering with high parallelism
var parallelOptions = new ParallelOptions(
    maxDegreeOfParallelism: 16,
    preserveOrdering: false); // Better performance
```

#### Parallelism Guidelines

| Workload Type | Recommended Parallelism | PreserveOrdering |
|---------------|------------------------|------------------|
| CPU-bound | Processor count | Only if required |
| I/O-bound | Processor count × 2 | Only if required |
| Mixed | Processor count × 1.5 | Only if required |
| Memory-intensive | Processor count ÷ 2 | Only if required |

### NP9503: Batching Configuration Mismatch

**ID:** `NP9503`  
**Severity:** Warning  
**Category:** Performance  

This analyzer detects batching configurations where batch sizes and timeouts are misaligned, causing either excessive latency from large batches or inefficient processing from small batches.

#### Why This Matters

Batching configuration mismatches cause:

1. **Poor Throughput**: Small batches processed frequently reduce efficiency
2. **Excessive Latency**: Large batches wait unnecessarily long
3. **Resource Waste**: Inefficient use of system resources
4. **Unpredictable Performance**: Inconsistent processing times

#### Problematic Configuration

```csharp
// ❌ PROBLEM: Large batch size with short timeout
var batchingOptions = new BatchingOptions(
    batchSize: 1000,
    timeout: TimeSpan.FromMilliseconds(100)); // NP9503: Batch won't fill

// ❌ PROBLEM: Small batch size with long timeout
var batchingOptions = new BatchingOptions(
    batchSize: 5,
    timeout: TimeSpan.FromMinutes(1)); // NP9503: Unnecessary latency

// ❌ PROBLEM: Medium batch with disproportionate timeout
var batchingStrategy = new BatchingStrategy(
    batchSize: 50,
    maxWaitTime: TimeSpan.FromMilliseconds(10)); // NP9503: Too short for batch size
```

#### Solution: Align Batch Size and Timeout

```csharp
// ✅ CORRECT: Balanced batching configuration
var batchingOptions = new BatchingOptions(
    batchSize: 100,
    timeout: TimeSpan.FromSeconds(1)); // Reasonable fill time

// ✅ CORRECT: Fast batching for small items
var batchingOptions = new BatchingOptions(
    batchSize: 10,
    timeout: TimeSpan.FromMilliseconds(100)); // Quick turnover

// ✅ CORRECT: Large batch with proportional timeout
var batchingStrategy = new BatchingStrategy(
    batchSize: 1000,
    maxWaitTime: TimeSpan.FromSeconds(5)); // Sufficient time to fill
```

#### Batching Configuration Guidelines

| Batch Size | Recommended Timeout | Use Case |
|------------|---------------------|-----------|
| 1-10 | 50-500ms | Low-latency scenarios |
| 10-100 | 500ms-2s | General purpose |
| 100-1000 | 1-10s | High-throughput scenarios |
| 1000+ | 5-30s | Batch processing systems |

### NP9504: Timeout Configuration Issues

**ID:** `NP9504`  
**Severity:** Warning  
**Category:** Configuration  

This analyzer detects inappropriate timeout configurations that can cause resource leaks, hanging operations, or inefficient resource utilization in NPipeline pipelines.

#### Why This Matters

Inappropriate timeout configurations cause:

1. **Resource Leaks**: Operations that never timeout hold resources indefinitely
2. **Hanging Operations**: Too long timeouts cause unresponsive systems
3. **Premature Failures**: Too short timeouts cause unnecessary failures
4. **Poor User Experience**: Inconsistent behavior and unpredictable responses

#### Problematic Configuration

```csharp
// ❌ PROBLEM: Zero timeout for I/O operations
var resilientStrategy = new ResilientExecutionStrategy(
    timeout: TimeSpan.Zero); // NP9504: Immediate failures

// ❌ PROBLEM: Very short timeout for I/O operations
builder.AddTransform<DatabaseTransform, Input, Output>("transform")
    .WithTimeout(TimeSpan.FromMilliseconds(10)); // NP9504: Too short for database

// ❌ PROBLEM: Excessive timeout for CPU operations
builder.AddTransform<CpuTransform, Input, Output>("transform")
    .WithTimeout(TimeSpan.FromHours(1)); // NP9504: Resource leak risk

// ❌ PROBLEM: Very long retry timeout
var retryOptions = new PipelineRetryOptions(
    maxRetryCount: 3,
    baseDelay: TimeSpan.FromSeconds(1),
    maxDelay: TimeSpan.FromHours(2)); // NP9504: Excessive retry duration
```

#### Solution: Set Appropriate Timeouts

```csharp
// ✅ CORRECT: Reasonable timeout for I/O operations
builder.AddTransform<DatabaseTransform, Input, Output>("transform")
    .WithTimeout(TimeSpan.FromSeconds(30)); // Sufficient for database operations

// ✅ CORRECT: Appropriate timeout for CPU operations
builder.AddTransform<CpuTransform, Input, Output>("transform")
    .WithTimeout(TimeSpan.FromMinutes(2)); // Reasonable for CPU work

// ✅ CORRECT: Balanced retry timeout
var retryOptions = new PipelineRetryOptions(
    maxRetryCount: 3,
    baseDelay: TimeSpan.FromSeconds(1),
    maxDelay: TimeSpan.FromMinutes(5)); // Reasonable retry ceiling
```

#### Timeout Guidelines

| Operation Type | Recommended Timeout | Maximum Timeout |
|----------------|---------------------|-----------------|
| Database I/O | 5-60 seconds | 5 minutes |
| Network I/O | 1-30 seconds | 2 minutes |
| File I/O | 10-120 seconds | 10 minutes |
| CPU-bound | 30 seconds-5 minutes | 30 minutes |
| Retry operations | 1-10 minutes | 30 minutes |

### NP9505: Inefficient String Operations

**ID:** `NP9505`  
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
// ❌ PROBLEM: String concatenation in loop
public class BadTransform : ITransformNode<Input, Output>
{
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        string result = "";
        foreach (var item in input.Items) // NP9505: Concatenation in loop
        {
            result += item.ToString(); // Creates new string each iteration
        }
        return new Output(result);
    }
}

// ❌ PROBLEM: Inefficient string formatting
protected override async Task<string> ProcessAsync(Data data, CancellationToken cancellationToken)
{
    return string.Format("{0}-{1}-{2}", data.Id, data.Name, data.Value); // NP9505: Inefficient formatting
}

// ❌ PROBLEM: String operations in LINQ
var results = items.Select(x => x.Name.ToUpper().Substring(0, 5).Trim()); // NP9505: Multiple allocations per item
```

#### Solution: Use Efficient String Operations

```csharp
// ✅ CORRECT: Use StringBuilder for concatenation
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

// ✅ CORRECT: Use string interpolation
protected override async Task<string> ProcessAsync(Data data, CancellationToken cancellationToken)
{
    return $"{data.Id}-{data.Name}-{data.Value}"; // Efficient formatting
}

// ✅ CORRECT: Use span-based operations
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

### NP9506: LINQ in Hot Paths

**ID:** `NP9506`  
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
// ❌ PROBLEM: LINQ in ExecuteAsync method
public class BadTransform : ITransformNode<Input, Output>
{
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        // NP9506: LINQ in hot path creates allocations
        var filtered = input.Items.Where(x => x.IsActive).ToList();
        var sorted = filtered.OrderBy(x => x.Priority).ToList();
        var grouped = sorted.GroupBy(x => x.Category).ToList();
        
        return new Output(grouped);
    }
}

// ❌ PROBLEM: LINQ in loop
foreach (var batch in batches)
{
    // NP9506: LINQ inside loop creates pressure
    var processed = batch.Select(x => ProcessItem(x)).Where(x => x != null).ToList();
    await SendBatchAsync(processed);
}

// ❌ PROBLEM: Materializing LINQ results
var items = sourceData.Where(x => x.IsValid).Select(x => x.Transform()).ToArray(); // NP9506: Immediate materialization
```

#### Solution: Use Imperative Alternatives

```csharp
// ✅ CORRECT: Use imperative processing
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

// ✅ CORRECT: Process items directly in loop
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

### NP9507: Anonymous Object Allocation

**ID:** `NP9507`  
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
// ❌ PROBLEM: Anonymous objects in ExecuteAsync
protected override async Task ExecuteAsync(IDataPipe<Output> output, PipelineContext context, CancellationToken cancellationToken)
{
    foreach (var item in inputItems)
    {
        // NP9507: Anonymous object allocation in hot path
        var result = new { Id = item.Id, Name = item.Name, Value = item.Value * 2 };
        await output.ProduceAsync(new Output(result), cancellationToken);
    }
}

// ❌ PROBLEM: Anonymous objects in LINQ
var processed = items.Select(x => new // NP9507: Anonymous object in LINQ
{
    Id = x.Id,
    ProcessedValue = x.Value * 2,
    Timestamp = DateTime.UtcNow
}).ToList();

// ❌ PROBLEM: Anonymous objects in loops
foreach (var item in largeCollection)
{
    // NP9507: Anonymous object allocation per iteration
    var temp = new { Original = item, Processed = Process(item) };
    results.Add(temp);
}
```

#### Solution: Use Named Types or Value Types

```csharp
// ✅ CORRECT: Define named type for results
public record ProcessedItem(int Id, string Name, double Value);

protected override async Task ExecuteAsync(IDataPipe<Output> output, PipelineContext context, CancellationToken cancellationToken)
{
    foreach (var item in inputItems)
    {
        var result = new ProcessedItem(item.Id, item.Name, item.Value * 2);
        await output.ProduceAsync(new Output(result), cancellationToken);
    }
}

// ✅ CORRECT: Use named type in LINQ
public record ProcessedData(int Id, double ProcessedValue, DateTime Timestamp);

var processed = items.Select(x => new ProcessedData(
    x.Id,
    x.Value * 2,
    DateTime.UtcNow)).ToList();

// ✅ CORRECT: Use struct for value-type data
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

## Configuration Best Practices

### General Guidelines

1. **Profile Before Optimizing**: Measure actual performance when changing configuration
2. **Start Conservative**: Begin with conservative values and adjust based on metrics
3. **Monitor Resource Usage**: Watch memory, CPU, and thread pool metrics
4. **Test Under Load**: Validate configuration with realistic workloads

### Configuration Checklist

- [ ] Set `MaxMaterializedItems` to bound memory usage
- [ ] Match parallelism to workload characteristics
- [ ] Align batch sizes with processing times
- [ ] Set appropriate timeouts for operation types
- [ ] Use efficient string operations in hot paths
- [ ] Avoid LINQ in high-frequency methods
- [ ] Replace anonymous objects with named types

### Monitoring Configuration

```csharp
// Add monitoring to validate configuration
public class MonitoredTransform : ITransformNode<Input, Output>
{
    private readonly IMetrics _metrics;
    
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            var result = await ProcessAsync(input, cancellationToken);
            
            // Monitor processing time
            _metrics.Histogram("transform.duration", stopwatch.ElapsedMilliseconds);
            
            return result;
        }
        catch (Exception ex)
        {
            // Monitor errors
            _metrics.Counter("transform.errors").Increment();
            throw;
        }
    }
}
```

## Configuration

Adjust analyzer severity in `.editorconfig`:

```ini
# Treat configuration issues as errors
dotnet_diagnostic.NP9501.severity = error
dotnet_diagnostic.NP9502.severity = warning
dotnet_diagnostic.NP9503.severity = warning
dotnet_diagnostic.NP9504.severity = warning
dotnet_diagnostic.NP9505.severity = warning
dotnet_diagnostic.NP9506.severity = warning
dotnet_diagnostic.NP9507.severity = warning
```

## See Also

- [Performance Analyzers](./performance.md) - Write fast, non-blocking code
- [Best Practice Analyzers](./best-practices.md) - Follow framework design principles
- [Data Processing Analyzers](./data-processing.md) - Ensure data flows correctly