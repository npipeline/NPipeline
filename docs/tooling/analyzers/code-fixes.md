---
title: Code Fix Providers
description: Automated code fixes for common analyzer issues in NPipeline.
sidebar_position: 6
---

## Code Fix Providers

NPipeline analyzers include automated code fix providers that can automatically resolve many common issues detected by the analyzers. These code fixes appear as light bulb suggestions in Visual Studio and can be applied with a single click.

### Overview

Code fix providers help developers quickly resolve analyzer warnings by:

1. **Automatically generating correct code patterns** - No need to remember exact syntax
2. **Applying framework best practices** - Fixes follow NPipeline conventions
3. **Reducing cognitive load** - Focus on business logic instead of boilerplate
4. **Preventing copy-paste errors** - Consistent, correct implementations

### Available Code Fixes

#### NP9501: Unbounded Materialization Configuration

**Fix Provider:** `UnboundedMaterializationConfigurationCodeFixProvider`

This code fix automatically adds appropriate `MaxMaterializedItems` values to `PipelineRetryOptions` constructors.

##### Before Fix

```csharp
// :x: Missing MaxMaterializedItems parameter
var retryOptions = new PipelineRetryOptions(
    maxRetryCount: 3,
    baseDelay: TimeSpan.FromSeconds(1),
    maxDelay: TimeSpan.FromMinutes(1));
```

##### After Fix

```csharp
// :heavy_check_mark: Fixed with reasonable default
var retryOptions = new PipelineRetryOptions(
    maxRetryCount: 3,
    baseDelay: TimeSpan.FromSeconds(1),
    maxDelay: TimeSpan.FromMinutes(1),
    maxMaterializedItems: 1000);
```

##### Fix Options

The code fix provider offers several options based on context:

| Option | Value | Use Case |
|---------|--------|-----------|
| Conservative | 100 | Memory-constrained environments |
| Balanced | 1000 | General purpose scenarios |
| High Throughput | 10000 | High-performance scenarios |
| Custom | Prompts for value | Specific requirements |

#### NP9502: Inappropriate Parallelism Configuration

**Fix Provider:** `InappropriateParallelismConfigurationCodeFixProvider`

This code fix suggests appropriate parallelism values based on workload type and system characteristics.

##### Before Fix

```csharp
// :x: Excessive parallelism for CPU-bound work
builder.AddTransform<CpuTransform, Input, Output>("transform")
    .WithParallelism(Environment.ProcessorCount * 4);
```

##### After Fix

```csharp
// :heavy_check_mark: Fixed with appropriate parallelism
builder.AddTransform<CpuTransform, Input, Output>("transform")
    .WithParallelism(Environment.ProcessorCount);
```

##### Fix Options

| Workload Type | Suggested Parallelism | Rationale |
|---------------|----------------------|------------|
| CPU-bound | `Environment.ProcessorCount` | Matches available cores |
| I/O-bound | `Environment.ProcessorCount * 2` | I/O can handle more concurrency |
| Mixed | `Environment.ProcessorCount * 1.5` | Balanced approach |
| Memory-intensive | `Environment.ProcessorCount / 2` | Conservative resource usage |

#### NP9503: Batching Configuration Mismatch

**Fix Provider:** `BatchingConfigurationMismatchCodeFixProvider`

This code fix aligns batch sizes with appropriate timeout values based on processing characteristics.

##### Before Fix

```csharp
// :x: Large batch with short timeout
var batchingOptions = new BatchingOptions(
    batchSize: 1000,
    timeout: TimeSpan.FromMilliseconds(100));
```

##### After Fix

```csharp
// :heavy_check_mark: Fixed with proportional timeout
var batchingOptions = new BatchingOptions(
    batchSize: 1000,
    timeout: TimeSpan.FromSeconds(5));
```

##### Fix Strategies

| Batch Size | Recommended Timeout | Calculation Method |
|------------|---------------------|-------------------|
| 1-10 | 50-500ms | 50ms per item |
| 10-100 | 500ms-2s | 20ms per item |
| 100-1000 | 1-10s | 10ms per item |
| 1000+ | 5-30s | 5ms per item |

#### NP9504: Timeout Configuration Issues

**Fix Provider:** `TimeoutConfigurationCodeFixProvider`

This code fix suggests appropriate timeout values based on operation type and characteristics.

##### Before Fix

```csharp
// :x: Zero timeout for I/O operations
builder.AddTransform<DatabaseTransform, Input, Output>("transform")
    .WithTimeout(TimeSpan.Zero);
```

##### After Fix

```csharp
// :heavy_check_mark: Fixed with appropriate timeout
builder.AddTransform<DatabaseTransform, Input, Output>("transform")
    .WithTimeout(TimeSpan.FromSeconds(30));
```

##### Timeout Recommendations

| Operation Type | Suggested Timeout | Maximum |
|----------------|---------------------|-----------|
| Database I/O | 30 seconds | 5 minutes |
| Network I/O | 10 seconds | 2 minutes |
| File I/O | 60 seconds | 10 minutes |
| CPU-bound | 2 minutes | 30 minutes |

#### NP9101: Blocking Operations in Async Methods

**Fix Provider:** `BlockingAsyncOperationCodeFixProvider`

This code fix automatically converts blocking operations in async methods to proper async patterns.

##### Before Fix

```csharp
// :x: PROBLEM: Blocking on Task.Result
public async Task<string> ProcessDataAsync()
{
    var task = SomeOperationAsync();
    return task.Result; // NP9101: Blocks until task completes
}

// :x: PROBLEM: Synchronous I/O in async method
public async Task ProcessFileAsync()
{
    var content = File.ReadAllText("file.txt"); // NP9101: Synchronous I/O
    await ProcessAsync(content);
}
```

##### After Fix

```csharp
// :heavy_check_mark: CORRECT: Use await
public async Task<string> ProcessDataAsync()
{
    var task = SomeOperationAsync();
    return await task; // Properly awaits without blocking
}

// :heavy_check_mark: CORRECT: Use async I/O
public async Task ProcessFileAsync()
{
    var content = await File.ReadAllTextAsync("file.txt"); // Async I/O
    await ProcessAsync(content);
}
```

#### NP9102: Swallowed OperationCanceledException

**Fix Provider:** `OperationCanceledExceptionCodeFixProvider`

This code fix ensures that OperationCanceledException is properly re-thrown or handled to maintain cancellation contracts.

##### Before Fix

```csharp
// :x: PROBLEM: Swallowing OperationCanceledException
public async Task ProcessAsync(CancellationToken cancellationToken)
{
    try
    {
        await SomeOperationAsync(cancellationToken);
    }
    catch (OperationCanceledException)
    {
        // NP9102: Silently swallowing cancellation
        Console.WriteLine("Operation cancelled");
    }
}
```

##### After Fix

```csharp
// :heavy_check_mark: CORRECT: Re-throw cancellation exception
public async Task ProcessAsync(CancellationToken cancellationToken)
{
    try
    {
        await SomeOperationAsync(cancellationToken);
    }
    catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
    {
        throw; // Re-throw to propagate cancellation
    }
    catch (OperationCanceledException)
    {
        // Handle other cancellation scenarios
        throw;
    }
}
```

#### NP9103: Synchronous over Async Patterns

**Fix Provider:** `SynchronousOverAsyncCodeFixProvider`

This code fix converts fire-and-forget async calls to properly awaited async operations.

##### Before Fix

```csharp
// :x: PROBLEM: Fire-and-forget async call (unawaited)
public async Task ProcessDataAsync()
{
    SomeOperationAsync(); // NP9103: Async method not awaited
    DoSomethingElse();
}
```

##### After Fix

```csharp
// :heavy_check_mark: CORRECT: Await the async call
public async Task ProcessDataAsync()
{
    await SomeOperationAsync(); // Properly awaited
    DoSomethingElse();
}
```

#### NP9104: Cancellation Token Not Respected

**Fix Provider:** `CancellationTokenRespectCodeFixProvider`

This code fix adds proper cancellation token checking and propagation in long-running operations.

##### Before Fix

```csharp
// :x: PROBLEM: Not checking cancellation token in loop
public async Task ProcessItemsAsync(IEnumerable<Item> items, CancellationToken cancellationToken)
{
    foreach (var item in items)
    {
        // NP9104: Not checking cancellation token
        await ProcessItemAsync(item);
    }
}
```

##### After Fix

```csharp
// :heavy_check_mark: CORRECT: Check cancellation token before processing
public async Task ProcessItemsAsync(IEnumerable<Item> items, CancellationToken cancellationToken)
{
    foreach (var item in items)
    {
        cancellationToken.ThrowIfCancellationRequested();
        await ProcessItemAsync(item, cancellationToken);
    }
}
```

#### NP9201: LINQ Operations in Hot Paths

**Fix Provider:** `LinqInHotPathsCodeFixProvider`

This code fix converts LINQ operations in performance-critical code to imperative alternatives.

##### Before Fix

```csharp
// :x: PROBLEM: LINQ in ExecuteAsync method
public class BadTransform : ITransformNode<Input, Output>
{
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        // NP9201: LINQ in hot path creates allocations
        var filtered = input.Items.Where(x => x.IsActive).ToList();
        var sorted = filtered.OrderBy(x => x.Priority).ToList();
        return new Output(sorted);
    }
}
```

##### After Fix

```csharp
// :heavy_check_mark: CORRECT: Use imperative processing
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
        
        return new Output(filtered);
    }
}
```

#### NP9202: Inefficient String Operations

**Fix Provider:** `InefficientStringOperationsCodeFixProvider`

This code fix replaces inefficient string operations with high-performance alternatives.

##### Before Fix

```csharp
// :x: PROBLEM: String concatenation in loop
public class BadTransform : ITransformNode<Input, Output>
{
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        string result = "";
        foreach (var item in input.Items) // NP9202: Concatenation in loop
        {
            result += item.ToString(); // Creates new string each iteration
        }
        return new Output(result);
    }
}
```

##### After Fix

```csharp
// :heavy_check_mark: CORRECT: Use StringBuilder for concatenation
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
```

#### NP9203: Anonymous Object Allocation

**Fix Provider:** `AnonymousObjectAllocationCodeFixProvider`

This code fix creates named types to replace anonymous object allocations in performance-critical code.

##### Before Fix

```csharp
// :x: PROBLEM: Anonymous objects in ExecuteAsync
protected override async Task ExecuteAsync(IDataPipe<Output> output, PipelineContext context, CancellationToken cancellationToken)
{
    foreach (var item in inputItems)
    {
        // NP9203: Anonymous object allocation in hot path
        var result = new { Id = item.Id, Name = item.Name, Value = item.Value * 2 };
        await output.ProduceAsync(new Output(result), cancellationToken);
    }
}
```

##### After Fix

```csharp
// :heavy_check_mark: CORRECT: Define named type for results
public record ProcessedItem(int Id, string Name, double Value);

protected override async Task ExecuteAsync(IDataPipe<Output> output, PipelineContext context, CancellationToken cancellationToken)
{
    foreach (var item in inputItems)
    {
        var result = new ProcessedItem(item.Id, item.Name, item.Value * 2);
        await output.ProduceAsync(new Output(result), cancellationToken);
    }
}
```

#### NP9204: Missing ValueTask Optimization

**Fix Provider:** `ValueTaskOptimizationCodeFixProvider`

This code fix converts Task return types to ValueTask for methods that frequently complete synchronously.

##### Before Fix

```csharp
// :x: PROBLEM: Allocates heap object even for synchronous completions
public async Task<string> GetDataAsync(string id)
{
    var cached = _cache.Get(id);
    if (cached != null)
    {
        return cached; // Allocates Task on heap
    }
    
    return await FetchFromDatabaseAsync(id);
}
```

##### After Fix

```csharp
// :heavy_check_mark: CORRECT: No allocation for synchronous returns
public async ValueTask<string> GetDataAsync(string id)
{
    var cached = _cache.Get(id);
    if (cached != null)
    {
        return cached; // No allocation - synchronous completion
    }
    
    return await FetchFromDatabaseAsync(id);
}
```

#### NP9205: Non-Streaming Patterns in SourceNode

**Fix Provider:** `SourceNodeStreamingCodeFixProvider`

This code fix converts non-streaming SourceNode implementations to proper streaming patterns using IAsyncEnumerable.

##### Before Fix

```csharp
// :x: PROBLEM: Non-streaming implementation
public class BadSourceNode : ISourceNode<Output>
{
    public override IDataPipe<Output> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        var items = LoadAllItems(); // Loads everything into memory
        return new StreamingDataPipe<Output>(items.ToAsyncEnumerable());
    }
}
```

##### After Fix

```csharp
// :heavy_check_mark: CORRECT: Streaming implementation
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

#### NP9301: Inefficient Exception Handling

**Fix Provider:** `InefficientExceptionHandlingCodeFixProvider`

This code fix converts inefficient exception handling patterns to specific exception handling and alternative patterns.

##### Before Fix

```csharp
// :x: PROBLEM: Catching Exception instead of specific exceptions
public async Task ProcessAsync(Input input, CancellationToken cancellationToken)
{
    try
    {
        await ProcessItemAsync(input, cancellationToken);
    }
    catch (Exception ex) // NP9301: Too broad exception handling
    {
        _logger.LogError(ex, "Processing failed");
        throw;
    }
}
```

##### After Fix

```csharp
// :heavy_check_mark: CORRECT: Catch specific exceptions
public async Task ProcessAsync(Input input, CancellationToken cancellationToken)
{
    try
    {
        await ProcessItemAsync(input, cancellationToken);
    }
    catch (InvalidOperationException ex)
    {
        _logger.LogError(ex, "Invalid operation during processing");
        throw;
    }
    catch (TimeoutException ex)
    {
        _logger.LogError(ex, "Processing timeout");
        throw new ProcessingTimeoutException("Item processing timed out", ex);
    }
}
```

#### NP9302: SinkNode Input Not Consumed

**Fix Provider:** `SinkNodeInputConsumptionCodeFixProvider`

This code fix adds proper input consumption patterns to SinkNode implementations.

##### Before Fix

```csharp
// :x: PROBLEM: Not consuming input
public class BadSinkNode : ISinkNode<Input>
{
    protected override async Task ExecuteAsync(IDataPipe<Input> input, PipelineContext context, CancellationToken cancellationToken)
    {
        // NP9302: Input pipe not consumed - items are dropped
        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
    }
}
```

##### After Fix

```csharp
// :heavy_check_mark: CORRECT: Consume all input
public class GoodSinkNode : ISinkNode<Input>
{
    protected override async Task ExecuteAsync(IDataPipe<Input> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.ConsumeAsync(cancellationToken))
        {
            await ProcessItemAsync(item, cancellationToken);
        }
    }
}
```

#### NP9303: Unsafe PipelineContext Access

**Fix Provider:** `PipelineContextAccessCodeFixProvider`

This code fix adds null-safe patterns and proper error handling for PipelineContext access.

##### Before Fix

```csharp
// :x: PROBLEM: Unsafe context access
public class BadTransform : ITransformNode<Input, Output>
{
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        // NP9303: Potential null reference exception
        var nodeId = context.NodeId;
        var metadata = context.Metadata["key"];
        return new Output(input, nodeId, metadata);
    }
}
```

##### After Fix

```csharp
// :heavy_check_mark: CORRECT: Safe context access
public class GoodTransform : ITransformNode<Input, Output>
{
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        var nodeId = context?.NodeId ?? "unknown";
        var metadata = context?.Metadata?.ContainsKey("key") == true
            ? context.Metadata["key"]
            : null;
        return new Output(input, nodeId, metadata);
    }
}
```

#### NP9401: Direct Dependency Instantiation

**Fix Provider:** `DependencyInjectionCodeFixProvider`

This code fix converts direct dependency instantiation to proper constructor injection patterns.

##### Before Fix

```csharp
// :x: PROBLEM: Direct dependency instantiation
public class BadTransform : ITransformNode<Input, Output>
{
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        // NP9401: Direct instantiation creates tight coupling
        var processor = new DataProcessor();
        var result = await processor.ProcessAsync(input);
        return new Output(result);
    }
}
```

##### After Fix

```csharp
// :heavy_check_mark: CORRECT: Constructor injection
public class GoodTransform : ITransformNode<Input, Output>
{
    private readonly IDataProcessor _processor;
    
    public GoodTransform(IDataProcessor processor)
    {
        _processor = processor ?? throw new ArgumentNullException(nameof(processor));
    }
    
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        var result = await _processor.ProcessAsync(input);
        return new Output(result);
    }
}
```

#### NP9001: Incomplete Resilience Configuration

**Fix Provider:** `ResilientExecutionConfigurationCodeFixProvider`

This code fix adds missing resilience configuration prerequisites to error handlers.

##### Before Fix

```csharp
// :x: PROBLEM: Incomplete resilience configuration
public class MyErrorHandler : IPipelineErrorHandler
{
    public async Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        return error switch
        {
            TimeoutException => PipelineErrorDecision.RestartNode,  // Intent is clear
            _ => PipelineErrorDecision.FailPipeline
        };
    }
}
```

##### After Fix

```csharp
// :heavy_check_mark: CORRECT: Complete resilience configuration
[ResilientExecution(
    MaxRetryCount = 3,
    BaseDelay = TimeSpan.FromSeconds(1),
    MaxDelay = TimeSpan.FromMinutes(1),
    MaxMaterializedItems = 1000)]
public class MyErrorHandler : IPipelineErrorHandler
{
    public async Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        return error switch
        {
            TimeoutException => PipelineErrorDecision.RestartNode,
            _ => PipelineErrorDecision.FailPipeline
        };
    }
}
```

### Using Code Fixes

#### In Visual Studio

1. **Analyzer Warning Appears**: A wavy underline appears under the problematic code
2. **Light Bulb Icon**: Click the light bulb or press `Ctrl+.` (period)
3. **Select Fix**: Choose from the available code fix options
4. **Apply Fix**: The fix is automatically applied to your code

#### In VS Code

1. **Analyzer Warning**: A red or yellow underline appears
2. **Quick Fix**: Click the light bulb icon or press `Ctrl+.` (period)
3. **Choose Fix**: Select the appropriate fix from the dropdown
4. **Preview Changes**: Review the proposed changes before applying

#### In Rider

1. **Warning Highlight**: Code is highlighted with a warning indicator
2. **Alt+Enter**: Press `Alt+Enter` to open quick fixes
3. **Select Fix**: Choose the desired code fix from the list
4. **Apply**: The fix is applied automatically

### Code Fix Best Practices

#### Review Before Applying

While code fixes are helpful, always review the generated code:

1. **Understand the Change**: Know what the fix is doing
2. **Validate the Logic**: Ensure it matches your requirements
3. **Test Thoroughly**: Verify the fix works in your scenario
4. **Consider Alternatives**: Sometimes a different approach is better

#### Customizing Fixes

Code fixes provide sensible defaults, but you may need to customize:

```csharp
// Code fix provides this:
maxMaterializedItems: 1000

// You might customize to:
maxMaterializedItems: GetOptimalBatchSize(); // Your custom logic
```

#### Combining Fixes

Some issues require multiple fixes:

```csharp
// Original problematic code
var retryOptions = new PipelineRetryOptions(
    maxRetryCount: 3,
    baseDelay: TimeSpan.FromSeconds(1));

// Apply NP9501 fix first
var retryOptions = new PipelineRetryOptions(
    maxRetryCount: 3,
    baseDelay: TimeSpan.FromSeconds(1),
    maxMaterializedItems: 1000);

// Then apply NP9504 fix if needed
var retryOptions = new PipelineRetryOptions(
    maxRetryCount: 3,
    baseDelay: TimeSpan.FromSeconds(1),
    maxDelay: TimeSpan.FromMinutes(5), // Added by NP9504 fix
    maxMaterializedItems: 1000);
```

### Limitations

Code fix providers have some limitations:

1. **Context-Dependent**: Fixes are based on static analysis, not runtime behavior
2. **Conservative Values**: Suggested values are conservative for safety
3. **Simple Scenarios**: Complex refactoring scenarios may need manual intervention
4. **Business Logic**: Fixes can't understand your specific business requirements

### Extending Code Fixes

You can extend code fix behavior through configuration:

```ini
# .editorconfig
# Customize default values for code fixes
dotnet_diagnostic.NP9501.code_fix.default_max_materialized = 500
dotnet_diagnostic.NP9502.code_fix.cpu_bound_parallelism = processor_count
dotnet_diagnostic.NP9503.code_fix.timeout_per_item = 10ms
```

### Troubleshooting Code Fixes

#### Fix Not Available

If a code fix doesn't appear:

1. **Check Analyzer Version**: Ensure you have the latest analyzer package
2. **Verify Context**: Some fixes only apply in specific contexts
3. **Check Syntax**: The code must match the expected pattern exactly
4. **Restart IDE**: Sometimes a restart is needed after package updates

#### Fix Produces Errors

If a code fix introduces errors:

1. **Report the Issue**: File a bug report with example code
2. **Use Manual Fix**: Apply the suggested pattern manually
3. **Check Dependencies**: Ensure all required packages are installed
4. **Verify Compatibility**: Check for version conflicts

### Contributing

Code fix providers are open source and contributions are welcome:

1. **Fork the Repository**: Get the source code from GitHub
2. **Identify Improvement**: Find a scenario that needs better fixes
3. **Implement Fix**: Add or improve code fix logic
4. **Add Tests**: Ensure comprehensive test coverage
5. **Submit Pull Request**: Contribute back to the community

## Configuration

Enable or disable code fixes in your `.editorconfig`:

```ini
# Enable all code fixes (default)
dotnet_code_fix.enable = true

# Disable specific code fix providers
dotnet_code_fix.NP9501.enable = false
dotnet_code_fix.NP9502.enable = false

# Performance-related code fixes
dotnet_code_fix.NP9101.enable = true
dotnet_code_fix.NP9102.enable = true
dotnet_code_fix.NP9103.enable = true
dotnet_code_fix.NP9104.enable = true
dotnet_code_fix.NP9201.enable = true
dotnet_code_fix.NP9202.enable = true
dotnet_code_fix.NP9203.enable = true
dotnet_code_fix.NP9204.enable = true
dotnet_code_fix.NP9205.enable = true

# Reliability and data processing code fixes
dotnet_code_fix.NP9301.enable = true
dotnet_code_fix.NP9302.enable = true
dotnet_code_fix.NP9303.enable = true

# Best practice and configuration code fixes
dotnet_code_fix.NP9401.enable = true
dotnet_code_fix.NP9001.enable = true

# Customize code fix behavior
dotnet_code_fix.NP9501.default_value = 500
dotnet_code_fix.NP9502.strategy = conservative
dotnet_code_fix.NP9101.prefer_async_overloads = true
dotnet_code_fix.NP9201.use_imperative_alternatives = true
dotnet_code_fix.NP9202.use_string_builder_threshold = 3
dotnet_code_fix.NP9203.generate_records = true
```

## See Also

- [Configuration Analyzers](./configuration.md) - Understanding the issues code fixes address
- [Performance Analyzers](./performance.md) - Performance-related code fixes
- [Best Practice Analyzers](./best-practices.md) - Framework convention fixes