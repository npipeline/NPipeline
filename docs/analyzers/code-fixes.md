---
title: Code Fix Providers
description: Automated code fixes for common analyzer issues in NPipeline.
sidebar_position: 7
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

#### NP9002: Unbounded Materialization Configuration

This code fix automatically adds appropriate `MaxMaterializedItems` values to `PipelineRetryOptions` constructors.

**Before Fix:**

```csharp
var retryOptions = new PipelineRetryOptions(
    maxRetryCount: 3,
    baseDelay: TimeSpan.FromSeconds(1),
    maxDelay: TimeSpan.FromMinutes(1));
```

**After Fix:**

```csharp
var retryOptions = new PipelineRetryOptions(
    maxRetryCount: 3,
    baseDelay: TimeSpan.FromSeconds(1),
    maxDelay: TimeSpan.FromMinutes(1),
    maxMaterializedItems: 1000);
```

#### NP9003: Inappropriate Parallelism Configuration

This code fix suggests appropriate parallelism values based on workload type and system characteristics.

**Before Fix:**

```csharp
builder.AddTransform<CpuTransform, Input, Output>("transform")
    .WithParallelism(Environment.ProcessorCount * 4);
```

**After Fix:**

```csharp
builder.AddTransform<CpuTransform, Input, Output>("transform")
    .WithParallelism(Environment.ProcessorCount);
```

#### NP9004: Batching Configuration Mismatch

This code fix aligns batch sizes with appropriate timeout values based on processing characteristics.

**Before Fix:**

```csharp
var batchingOptions = new BatchingOptions(
    batchSize: 1000,
    timeout: TimeSpan.FromMilliseconds(100));
```

**After Fix:**

```csharp
var batchingOptions = new BatchingOptions(
    batchSize: 1000,
    timeout: TimeSpan.FromSeconds(5));
```

#### NP9005: Timeout Configuration Issues

This code fix suggests appropriate timeout values based on operation type and characteristics.

**Before Fix:**

```csharp
builder.AddTransform<DatabaseTransform, Input, Output>("transform")
    .WithTimeout(TimeSpan.Zero);
```

**After Fix:**

```csharp
builder.AddTransform<DatabaseTransform, Input, Output>("transform")
    .WithTimeout(TimeSpan.FromSeconds(30));
```

#### NP9101: Blocking Operations in Async Methods

This code fix automatically converts blocking operations in async methods to proper async patterns.

**Before Fix:**

```csharp
public async Task<string> ProcessDataAsync()
{
    var task = SomeOperationAsync();
    return task.Result;
}
```

**After Fix:**

```csharp
public async Task<string> ProcessDataAsync()
{
    var task = SomeOperationAsync();
    return await task;
}
```

#### NP9107: Non-Streaming Patterns in SourceNode

This code fix converts non-streaming SourceNode implementations to proper streaming patterns using IAsyncEnumerable.

**Before Fix:**

```csharp
public class BadSourceNode : SourceNode<Output>
{
    public override IDataPipe<Output> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        var items = LoadAllItems();
        return new StreamingDataPipe<Output>(items.ToAsyncEnumerable());
    }
}
```

**After Fix:**

```csharp
public class GoodSourceNode : SourceNode<Output>
{
    public override IDataPipe<Output> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        return new StreamingDataPipe<Output>(GetItemsAsync(cancellationToken));
    }
    
    private async IAsyncEnumerable<Output> GetItemsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in _dataSource.GetItemsAsync(cancellationToken))
        {
            yield return ProcessItem(item);
        }
    }
}
```

#### NP9404: Direct Dependency Instantiation

This code fix converts direct dependency instantiation to proper constructor injection patterns.

**Before Fix:**

```csharp
public class BadTransform : TransformNode<Input, Output>
{
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        var processor = new DataProcessor();
        var result = await processor.ProcessAsync(input);
        return new Output(result);
    }
}
```

**After Fix:**

```csharp
public class GoodTransform : TransformNode<Input, Output>
{
    private readonly IDataProcessor _processor;
    
    public GoodTransform(IDataProcessor processor)
    {
        _processor = processor;
    }
    
    protected override async Task<Output> ExecuteAsync(Input input, PipelineContext context, CancellationToken cancellationToken)
    {
        var result = await _processor.ProcessAsync(input);
        return new Output(result);
    }
}
```

#### NP9301: SinkNode Input Not Consumed

This code fix adds proper input consumption patterns to SinkNode implementations.

**Before Fix:**

```csharp
public class BadSinkNode : SinkNode<Input>
{
    protected override async Task ExecuteAsync(IDataPipe<Input> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await Task.Delay(TimeSpan.FromSeconds(1), cancellationToken);
    }
}
```

**After Fix:**

```csharp
public class GoodSinkNode : SinkNode<Input>
{
    protected override async Task ExecuteAsync(IDataPipe<Input> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            await ProcessItemAsync(item, cancellationToken);
        }
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

While code fixes are helpful, always review the generated code to ensure it matches your requirements.

#### Customizing Fixes

Code fixes provide sensible defaults, but you may customize them based on your specific needs and constraints.

#### Combining Fixes

Some issues may require multiple fixes to be applied in sequence. The order generally doesn't matter, but test after applying fixes.

### Configuration

Enable or disable code fixes in your `.editorconfig`:

```ini
# Enable all code fixes (default)
dotnet_code_fix.enable = true

# Disable specific code fix providers
dotnet_code_fix.NP9002.enable = false
dotnet_code_fix.NP9003.enable = false
```

## See Also

- [Configuration Analyzers](./configuration.md) - Understanding the issues code fixes address
- [Performance Analyzers](./performance.md) - Performance-related code fixes
- [Best Practice Analyzers](./best-practices.md) - Framework convention fixes
