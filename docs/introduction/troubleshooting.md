---
title: Troubleshooting Guide
description: Common NPipeline issues and solutions.
sidebar_position: 11
---

# Troubleshooting Guide

>:information_source: **Related Documentation**
>If you're experiencing issues with **resilience configuration** (retries, node restarts, materialization), see [Resilience Troubleshooting](../core-concepts/resilience/troubleshooting.md) instead.

## Pipeline Execution Issues

### Pipeline doesn't execute

**Symptoms:** Pipeline runs but nothing happens.

**Common Causes:**

1. **Sinks not configured**

```csharp
// ❌ BAD - Source and transform but no sink
class MyPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<MySource, MyData>();
        var transform = builder.AddTransform<MyTransform, MyData, ProcessedData>();
        builder.Connect(source, transform);
        // Missing sink connection!
    }
}

// ✅ GOOD - Complete pipeline with sink
class MyPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<MySource, MyData>();
        var transform = builder.AddTransform<MyTransform, MyData, ProcessedData>();
        var sink = builder.AddSink<MySink, ProcessedData>();
        builder.Connect(source, transform);
        builder.Connect(transform, sink); // ✅ Sink connected
    }
}
```

2. **Source not yielding data**

Verify your source node returns data:

```csharp
public override IDataPipe<T> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
{
    static IAsyncEnumerable<T> GetDataAsync(CancellationToken ct)
    {
        return GetData();

        async IAsyncEnumerable<T> GetData()
        {
            // Add logging to verify data is being yielded
            await foreach (var item in GetSourceData(ct))
            {
                Console.WriteLine($"Yielding: {item}"); // ✅ Verify data
                yield return item;
            }
        }
    }

    return new StreamingDataPipe<T>(GetDataAsync(cancellationToken));
}
```

3. **Transform returning empty**

Ensure transform yields data for each input:

```csharp
// ❌ BAD - Might not yield for all items
public override Task<Item> ExecuteAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
{
    if (item.IsSpecial)
        return Task.FromResult(item);
    // Returns null implicitly for others ❌
}

// ✅ GOOD - Explicit for all paths
public override Task<Item> ExecuteAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
{
    if (item.IsSpecial)
        return Task.FromResult(Transform(item));
    else
        return Task.FromResult(item); // ✅ Always return
}
```

### Pipeline throws "Node not registered" exception

**Symptoms:** `InvalidOperationException: Node type not found in registry`

**Cause:** Nodes not registered with dependency injection.

**Solution - Assembly Scanning (Automatic Discovery):**

```csharp
// ❌ WRONG - No assembly specified
services.AddNPipeline();

// ✅ CORRECT - Include your assembly
services.AddNPipeline(Assembly.GetExecutingAssembly());

// ✅ CORRECT - Include multiple assemblies if nodes are in different projects
services.AddNPipeline(
    Assembly.GetExecutingAssembly(),
    typeof(ExternalNode).Assembly
);
```

**Solution - Fluent Configuration (Explicit Registration):**

If you prefer not to use reflection or have a specific set of nodes, use the fluent API:

```csharp
// ✅ CORRECT - Explicit registration without reflection
services.AddNPipeline(builder => builder
    .AddNode<MySourceNode>()
    .AddNode<MyTransformNode>()
    .AddNode<MySinkNode>()
    .AddPipeline<MyPipelineDefinition>()
);
```

### Pipeline connection fails silently

**Symptoms:** Nodes connect but data doesn't flow.

**Cause:** Incompatible types or incorrect graph structure.

**Solution:** Verify type compatibility:

```csharp
// ❌ BAD - Type mismatch
var source = builder.AddSource<SourceNode<int>, int>();
var transform = builder.AddTransform<TransformNode<string, int>, string, int>(); // ❌ Expects string
builder.Connect(source, transform); // Type mismatch!

// ✅ GOOD - Matching types
var source = builder.AddSource<SourceNode<int>, int>();
var transform = builder.AddTransform<TransformNode<int, int>, int, int>(); // ✅ Expects int
builder.Connect(source, transform);
```

## Performance Issues

### Pipeline is very slow

**Diagnosis Steps:**

1. **Measure each stage:**

```csharp
public override async Task<Item> ExecuteAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
{
    var sw = Stopwatch.StartNew();
    try
    {
        var result = await SlowOperationAsync(item, cancellationToken);
        sw.Stop();
        if (sw.ElapsedMilliseconds > 1000)
            logger.LogWarning($"Slow operation: {sw.ElapsedMilliseconds}ms");
        return result;
    }
    catch (Exception ex)
    {
        logger.LogError(ex, $"Operation failed after {sw.ElapsedMilliseconds}ms");
        throw;
    }
}
```

2. **Check for blocking operations:**

```csharp
// ❌ BAD - Blocking I/O
public override Task<Item> ExecuteAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
{
    var data = database.GetData(item.Id); // ❌ Synchronous blocking
    return Task.FromResult(Transform(data));
}

// ✅ GOOD - Async I/O
public override Task<Item> ExecuteAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
{
    var data = await database.GetDataAsync(item.Id, cancellationToken); // ✅ Async
    return Task.FromResult(Transform(data));
}
```

3. **Enable parallelism if CPU-bound:**

```csharp
services.AddNPipelineParallelism();

// In pipeline definition:
builder.WithParallelOptions(new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount });
```

### Memory usage constantly grows

**Symptoms:** Application memory increases over time.

**Common Causes:**

1. **Loading all data into memory:**

```csharp
// ❌ BAD - Loads all records at once
async IAsyncEnumerable<Item> ReadAsync()
{
    var allRecords = database.GetAllRecords().ToList(); // ❌ Everything in memory!
    foreach (var record in allRecords)
        yield return record;
}

// ✅ GOOD - Streams from database
async IAsyncEnumerable<Item> ReadAsync()
{
    var reader = database.GetRecords(); // ✅ Lazy enumerable
    await foreach (var record in reader)
        yield return record;
}
```

2. **Not disposing resources:**

```csharp
// ❌ BAD - Connections not disposed
public override async Task ExecuteAsync(IDataPipe<Item> input, PipelineContext context, CancellationToken cancellationToken)
{
    var connection = new SqlConnection(_connString);
    await connection.OpenAsync(cancellationToken);
    // ... use connection
    // Missing dispose!
}

// ✅ GOOD - Properly disposed
public override async Task ExecuteAsync(IDataPipe<Item> input, PipelineContext context, CancellationToken cancellationToken)
{
    using var connection = new SqlConnection(_connString);
    await connection.OpenAsync(cancellationToken);
    // ... use connection
    // Disposed when out of scope
}
```

3. **Accumulating state in context:**

```csharp
// ❌ BAD - Context grows unbounded
public override Task<Item> ExecuteAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
{
    if (!context.Items.ContainsKey("cache"))
        context.Items["cache"] = new Dictionary<int, Item>();

    var cache = context.Items["cache"] as Dictionary<int, Item>;
    cache[item.Id] = item; // ❌ Cache grows forever!
    return Task.FromResult(item);
}

// ✅ GOOD - Limited cache or external state
public override Task<Item> ExecuteAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
{
    // Use bounded cache or external storage
    _cache.AddOrUpdate(item.Id, item,
        (_, old) => item,
        TimeSpan.FromMinutes(5)); // ✅ Bounded by time
    return Task.FromResult(item);
}
```

## Error Handling Issues

### Exceptions are silently swallowed

**Symptoms:** Pipeline runs without errors but data isn't processed.

**Cause:** Caught and not re-thrown or logged.

**Solution:**

```csharp
// ❌ BAD - Silent failures
try
{
    return await ProcessAsync(item);
}
catch (Exception ex)
{
    // ❌ Swallowed silently
}

// ✅ GOOD - Logged and re-thrown
try
{
    return await ProcessAsync(item);
}
catch (Exception ex)
{
    logger.LogError(ex, "Processing failed for item");
    throw; // ✅ Re-throw or handle explicitly
}
```

> **💡 Tip:** If you encounter error codes in your exceptions (e.g., `[NP0301]`), see the [Error Codes Reference](../reference/error-codes.md) for detailed explanations and solutions.

### Cancellation not working

**Symptoms:** Pipeline ignores cancellation requests.

**Cause:** Not checking the cancellation token.

**Solution:**

```csharp
// ❌ BAD - Ignores cancellation
public override async Task<Item> ExecuteAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
{
    for (int i = 0; i < 1000000; i++)
    {
        await Task.Delay(10); // ❌ No cancellation check
    }
    return item;
}

// ✅ GOOD - Checks cancellation
public override async Task<Item> ExecuteAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
{
    for (int i = 0; i < 1000000; i++)
    {
        cancellationToken.ThrowIfCancellationRequested(); // ✅ Check token
        await Task.Delay(10, cancellationToken);
    }
    return item;
}
```

### Retry mechanism not triggering

**Symptoms:** Transient errors cause pipeline to fail instead of retry.

**Solution:** Implement explicit retry logic:

```csharp
private async Task<T> ExecuteWithRetryAsync<T>(
    Func<CancellationToken, Task<T>> operation,
    int maxRetries = 3,
    CancellationToken cancellationToken = default)
{
    int retryCount = 0;

    while (true)
    {
        try
        {
            return await operation(cancellationToken);
        }
        catch (IOException) when (retryCount < maxRetries) // ✅ Retry on transient
        {
            retryCount++;
            var delay = TimeSpan.FromSeconds(Math.Pow(2, retryCount - 1));
            logger.LogWarning($"Transient error, retrying in {delay.TotalSeconds}s");
            await Task.Delay(delay, cancellationToken);
        }
    }
}
```

## Data Issues

### Data transformed incorrectly

**Symptoms:** Output data is malformed or incomplete.

**Diagnosis:**

1. **Add detailed logging:**

```csharp
public override Task<Item> ExecuteAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
{
    logger.LogDebug($"Input: {JsonSerializer.Serialize(item)}");
    var result = Transform(item);
    logger.LogDebug($"Output: {JsonSerializer.Serialize(result)}");
    return Task.FromResult(result);
}
```

2. **Verify with unit tests:**

```csharp
[Fact]
public async Task TransformHandlesNullValues()
{
    var transform = new MyTransform();
    var context = new PipelineContext();

    var input = new Item { Value = null };
    var output = await transform.ExecuteAsync(input, context, CancellationToken.None);

    Assert.NotNull(output);
    Assert.Null(output.Value);
}
```

### Missing data in output

**Symptoms:** Some input records don't appear in output.

**Common Causes:**

1. **Transform filtering unintentionally:**

```csharp
// ❌ BAD - Implicit filtering
public override Task<Item> ExecuteAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
{
    if (item.IsValid)
        return Task.FromResult(Transform(item));
    // ❌ Null returned for invalid items
}

// ✅ GOOD - Explicit handling
public override Task<Item> ExecuteAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
{
    if (!item.IsValid)
    {
        logger.LogWarning($"Invalid item skipped: {item.Id}");
        throw new ValidationException(); // ✅ Fail fast or log
    }
    return Task.FromResult(Transform(item));
}
```

2. **Async enumerable not fully consumed:**

```csharp
// ❌ BAD - Only reads first item
var result = await source.ExecuteAsync(context, CancellationToken.None);
var first = (await result.GetAsyncEnumerator().MoveNextAsync()).Current;

// ✅ GOOD - Consumes all items
var result = await source.ExecuteAsync(context, CancellationToken.None);
await foreach (var item in result)
{
    // Process all items
}
```

## Configuration Issues

### Dependency Injection not finding nodes

**Symptoms:** `ServiceCollection doesn't contain...` or similar.

**Cause:** Assembly scanning not including node locations.

**Solution:**

```csharp
// Include all assemblies containing nodes
services.AddNPipeline(
    typeof(MyNode).Assembly,
    typeof(AnotherNode).Assembly
);

// Or use the entry assembly
services.AddNPipeline(Assembly.GetEntryAssembly()!);
```

### Configuration values not applying

**Symptoms:** Settings are defined but not used by nodes.

**Solution:** Inject `IOptions<T>`:

```csharp
public class MyTransform : TransformNode<Item, Item>
{
    private readonly IOptions<MySettings> _options;

    public MyTransform(IOptions<MySettings> options)
    {
        _options = options;
    }

    public override Task<Item> ExecuteAsync(Item item, PipelineContext context, CancellationToken cancellationToken)
    {
        var timeout = _options.Value.Timeout; // ✅ Use injected settings
        return Task.FromResult(item);
    }
}
```

## Testing Issues

### Test pipeline doesn't execute expected nodes

**Symptoms:** Test passes but you're unsure nodes actually ran.

**Solution:** Add verification logging:

```csharp
[Fact]
public async Task PipelineProcessesData()
{
    var source = new InMemorySourceNode<int> { Data = new[] { 1, 2, 3 } };
    var sink = new InMemorySinkNode<int>();

    var context = new PipelineContext();
    var runner = new PipelineRunner();

    // Register nodes
    context.Items["source"] = source;
    context.Items["sink"] = sink;

    await runner.RunAsync<MyPipeline>(context);

    // Verify sink received data
    var results = await sink.Completion;
    Assert.Equal(3, results.Count); // ✅ Verify processing occurred
}
```

### Mocks not being used in pipeline

**Symptoms:** Test uses mock but real service is called.

**Cause:** Registration order matters in DI - services added first take precedence.

**Solution - Assembly Scanning Approach:**

```csharp
var mockService = new Mock<IMyService>();
var services = new ServiceCollection();

// Register mock BEFORE AddNPipeline so it's used
services.AddSingleton(mockService.Object);
services.AddNPipeline(Assembly.GetExecutingAssembly());

var provider = services.BuildServiceProvider();
var runner = provider.GetRequiredService<IPipelineRunner>();
```

**Solution - Fluent Configuration Approach:**

With fluent configuration, you can explicitly control which nodes/services are registered:

```csharp
var mockService = new Mock<IMyService>();
var services = new ServiceCollection();

// Register mock BEFORE AddNPipeline fluent builder
services.AddSingleton(mockService.Object);

// Explicitly register only the nodes you want, skipping real implementations
services.AddNPipeline(builder => builder
    .AddNode<MySourceNode>()
    .AddNode<MyTransformNode>()
    .AddNode<MySinkNode>()
    .AddPipeline<MyPipelineDefinition>()
    // No assembly scanning means no other nodes auto-registered
);

var provider = services.BuildServiceProvider();
var runner = provider.GetRequiredService<IPipelineRunner>();
```
```

## Getting Help

If you can't resolve the issue:

1. **Check the logs** - Enable debug logging:

```csharp
services.AddLogging(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Debug));
```

2. **Review similar examples** - See [Common Patterns](../core-concepts/common-patterns.md)

3. **Check [FAQ](faq.md)** - Common questions answered

4. **Review [Error Handling](../core-concepts/resilience/error-handling-guide.md)** - Error-specific guidance

5. **Look up error codes** - See [Error Codes Reference](../reference/error-codes.md) for NP error codes (e.g., `[NP0101]`)

6. **Check source code** - Inspect node implementations in `/src/NPipeline`

## Next Steps

* **[FAQ](faq.md)**: Common questions and answers
* **[Error Handling](../core-concepts/resilience/error-handling-guide.md)**: Comprehensive error handling guide
* **[Getting Help](#getting-help)**: How to get support for unresolved issues


