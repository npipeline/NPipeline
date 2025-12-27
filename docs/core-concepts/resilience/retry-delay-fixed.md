---
title: Fixed Delay
description: Configure fixed delay retry strategy for simple retry scenarios in NPipeline
sidebar_position: 8.3
---

# Fixed Delay

Fixed delay provides the simplest retry strategy—waiting the same amount of time between every retry. Ideal for file system operations and scenarios where consistent behavior is preferred.

## When to Use

Use fixed delay when:
- Processing file system operations
- Dealing with expected uniform recovery times
- You need simplicity and predictability
- Testing retry behavior (without jitter)

## Why This Matters

**File system operations** typically have uniform recovery times—a file handle releases immediately or within milliseconds. Unlike distributed services that need exponential backoff, file operations benefit from consistent, simple retry timing.

**Fixed delays provide:**
- **Simplicity**: Easy to understand and reason about
- **Predictability**: Testing becomes deterministic (no jitter variability)
- **Efficiency**: No overhead from calculating exponential growth

**When not to use:**
- External APIs: Use exponential backoff instead
- Databases: Use linear backoff instead
- Distributed systems: Use exponential backoff with jitter

## Basic Configuration

```csharp
using NPipeline;
using NPipeline.Pipeline;

var context = PipelineContext.Default;
context.UseFixedDelay(TimeSpan.FromSeconds(2));
```

**Parameters:**
- **delay**: The constant delay between retries

## Delay Progression

With fixed delay of 2 seconds:

| Attempt | Delay |
|---------|-------|
| 1 | 2 seconds |
| 2 | 2 seconds |
| 3 | 2 seconds |
| 4 | 2 seconds |
| 5 | 2 seconds |

Always the same—predictable and simple.

## Common Configurations

### File System Operations (Recommended)

```csharp
context.UseFixedDelay(TimeSpan.FromSeconds(2));
```

**Rationale:**
- File handles typically release immediately
- 2 seconds is reasonable for lock recovery
- Simple and effective

### Quick Retry (Network Hiccups)

```csharp
context.UseFixedDelay(TimeSpan.FromMilliseconds(500));
```

**Use when:**
- Expected immediate recovery
- Dealing with brief network glitches
- Need responsive behavior

### Slow Retry (Heavy Load)

```csharp
context.UseFixedDelay(TimeSpan.FromSeconds(10));
```

**Use when:**
- System under heavy load
- Can tolerate longer waits
- Upstream service needs more recovery time

## Testing (Preferred)

Fixed delay is perfect for unit and integration tests because results are deterministic:

```csharp
[Fact]
public async Task RetryBehavior_WithFixedDelay()
{
    // No jitter = predictable timing for tests
    var context = CreateTestContext();
    context.UseFixedDelay(TimeSpan.FromMilliseconds(10));

    var stopwatch = Stopwatch.StartNew();
    await ExecuteWithRetries(context, maxRetries: 3);
    stopwatch.Stop();

    // With 3 retries and 10ms delay each:
    // Expected minimum: 30ms (3 × 10ms)
    Assert.True(stopwatch.ElapsedMilliseconds >= 30);
}
```

## With Jitter

While fixed delay is deterministic, adding jitter prevents synchronized retries in production:

```csharp
var context = PipelineContext.Default;
context.UseFixedDelay(
    TimeSpan.FromSeconds(2),
    jitterStrategy: JitterStrategies.FullJitter());
```

**Effect with 2s fixed delay + jitter:**
- Actual delays will vary between 0-2 seconds randomly
- Prevents "thundering herd" when multiple clients retry

## Pipeline Integration

```csharp
public sealed class FileProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Configure fixed delay for file operations
        context.UseFixedDelay(TimeSpan.FromSeconds(2));

        var source = builder.AddSource<FileSource, FileData>("file-source");
        var transform = builder.AddTransform<FileTransform, FileData, ProcessedData>("transform");
        var sink = builder.AddSink<FileSink, ProcessedData>("file-sink");

        builder.Connect(source, transform);
        builder.Connect(transform, sink);

        builder.WithRetryOptions(new PipelineRetryOptions(
            MaxItemRetries: 3,
            MaxNodeRestartAttempts: 2,
            MaxSequentialNodeAttempts: 5
        ));
    }
}
```

## Comparison with Other Strategies

| Strategy | Pattern | Best For |
|----------|---------|----------|
| Fixed | ▁▁▁▁▁ | Files, predictability, tests |
| Linear | ╱╱╱╱╱ | Databases, lock contention |
| Exponential | ╱╱╱╱╱ (steep) | APIs, distributed services |

## Best Practices

1. **Use for Testing**: Fixed delays make tests deterministic
2. **Use for Files**: File I/O recovery is typically uniform
3. **Add Jitter in Production**: Prevent synchronized retries
4. **Keep Delay Reasonable**: 1-5 seconds for most scenarios
5. **Monitor Performance**: Ensure delays don't bottleneck pipeline
6. **Consider Context**: Different operations may need different delays

## Real-World Example

```csharp
public sealed class FileProcessingExample
{
    public async Task ProcessLargeFileAsync()
    {
        var context = PipelineContext.Default;
        
        // Use fixed delay for file operations
        context.UseFixedDelay(TimeSpan.FromSeconds(1));
        
        var definition = new FileProcessingPipeline();
        var runner = PipelineRunner.Create();
        
        await runner.RunAsync(definition, context);
    }
}
```

## Related Topics

- [Retry Configuration](retries.md) - Overall retry configuration options
- [Retry Delays](retry-delays.md) - Overview of all retry delay strategies
- [Exponential Backoff](retry-delay-exponential.md) - Exponential delay progression
- [Linear Backoff](retry-delay-linear.md) - Linear delay progression
- [Advanced Patterns](retry-delay-advanced.md) - Custom strategies and patterns
- [Testing Retries](retry-delay-testing.md) - Testing retry behavior
- [Monitoring Retries](retry-delay-monitoring.md) - Observing retry metrics
