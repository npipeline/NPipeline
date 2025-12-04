---
title: Cancellation Model
description: How cancellation tokens propagate through NPipeline.
sidebar_position: 7
---

# Cancellation Model

Cancellation in NPipeline is cooperative and propagates through all nodes, allowing graceful shutdown at any point.

## Token Propagation

Cancellation tokens flow from the top-level execution down to every node:

```text
PipelineRunner.ExecuteAsync(cancellationToken)
        ↓
    Source.Initialize(cancellationToken)
        ↓
    Transform.ProcessAsync(item, cancellationToken)
        ↓
    Sink.ProcessAsync(item, cancellationToken)
        ↓
    [Cancellation propagates when token is cancelled]
```

**Implementation:**

```csharp
// User initiates cancellation
var cts = new CancellationTokenSource();
var executionTask = runner.ExecuteAsync(pipeline, context, cts.Token);

// Later, request cancellation
cts.Cancel();

// Each node receives cancellation token
try
{
    await executionTask;
}
catch (OperationCanceledException)
{
    // Graceful shutdown
}
```

## Node Responsibilities

Each node must respect the cancellation token:

### Source Node

Check token before reading each batch:

```csharp
public class FileSourceNode : ISourceNode<string>
{
    public IDataPipe<string> Initialize(
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        return new StreamingDataPipe<string>(ReadLines(cancellationToken));
    }
    
    private async IAsyncEnumerable<string> ReadLines([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var file = File.OpenRead("data.txt");
        using var reader = new StreamReader(file);
        
        while (!reader.EndOfStream)
        {
            cancellationToken.ThrowIfCancellationRequested(); // Check token
            
            var line = await reader.ReadLineAsync();
            if (line != null)
            {
                yield return line;
            }
        }
    }
}
```

### Transform Node

Check token and pass it forward:

```csharp
public class TransformNode : ITransformNode<string, int>
{
    public async IAsyncEnumerable<int> ProcessAsync(
        string input,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested(); // Check token
        
        var result = await LongRunningProcessAsync(cancellationToken); // Pass token
        yield return result;
    }
}
```

### Sink Node

Respect token during processing:

```csharp
public class SinkNode : ISinkNode<int>
{
    public async Task ExecuteAsync(
        IAsyncEnumerable<int> input,
        PipelineContext context,
        CancellationToken cancellationToken = default)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested(); // Check token
            
            await ProcessAsync(item, cancellationToken);
        }
    }
}
```

## Common Patterns

**Timeout:**

```csharp
var cts = CancellationTokenSource.CreateLinkedTokenSource(
    existingToken,
    new CancellationTokenSource(TimeSpan.FromSeconds(30)).Token);

await runner.ExecuteAsync(pipeline, context, cts.Token);
```

**Manual Cancellation:**

```csharp
var cts = new CancellationTokenSource();

var executionTask = runner.ExecuteAsync(pipeline, context, cts.Token);

await Task.Delay(5000);
cts.Cancel(); // Stop after 5 seconds

await executionTask; // Wait for graceful shutdown
```

**Partial Processing:**

```csharp
var cts = new CancellationTokenSource();

_ = Task.Run(async () =>
{
    await foreach (var result in runner.StreamAsync(pipeline, context, cts.Token))
    {
        if (result.ShouldStop)
        {
            cts.Cancel();
        }
    }
});
```

## Cancellation with Error Handling

Cancellation and errors work together:

```csharp
try
{
    await foreach (var item in pipeline.WithCancellation(cancellationToken))
    {
        // Process item
    }
}
catch (OperationCanceledException)
{
    // Cancellation requested
    logger.LogInformation("Pipeline cancelled");
}
catch (Exception ex)
{
    // Error occurred
    logger.LogError(ex, "Pipeline failed");
}
```

## Performance Implications

Frequent cancellation checks have minimal overhead:

```csharp
// Efficient - part of cancellation token implementation
cancellationToken.ThrowIfCancellationRequested();

// Avoid in hot loops if performance critical
for (int i = 0; i < 1_000_000; i++)
{
    // Check outside loop if possible
    cancellationToken.ThrowIfCancellationRequested();
    
    // Expensive work...
}
```

## Next Steps

- **[Performance Characteristics](performance-characteristics.md)** - Understand cancellation performance impact
- **[Extension Points](extension-points.md)** - Implement custom cancellation strategies
