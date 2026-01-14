---
title: Frequently Asked Questions
description: Answers to common questions about NPipeline.
sidebar_position: 3
---

# Frequently Asked Questions

## General Questions

### What is NPipeline?

NPipeline is a high-performance, graph-based streaming data pipeline library for .NET. It allows you to build complex data processing workflows by defining interconnected nodes (sources, transforms, and sinks) that process data efficiently without loading entire datasets into memory.

### When should I use NPipeline?

NPipeline is ideal for:

- ETL (Extract, Transform, Load) pipelines
- Real-time data processing
- Batch file processing (CSV, JSON, XML)
- Data validation and cleansing
- API data integration
- Any scenario requiring efficient data flow processing

### What makes NPipeline different from other pipeline libraries?

NPipeline offers:

- **Graph-based architecture** for clear data flow visualization
- **High performance** with minimal memory allocations
- **Type safety** with compile-time validation
- **Flexibility** to mix sequential and parallel execution
- **Production-ready** error handling and resilience
- **Testability** with built-in testing utilities

### Does NPipeline support streaming data?

Yes! NPipeline is designed around streaming data using `IAsyncEnumerable<T>`. Data flows through the pipeline without loading everything into memory at once.

## Installation & Setup

### Which version of .NET is supported?

NPipeline requires **.NET 8.0, 9.0, or 10.0**.

### Do I need to install all the extensions?

No. The core `NPipeline` package is all you need. Extensions are optional:

- **DependencyInjection** - For managing dependencies
- **Parallelism** - For parallel execution
- **Testing** - For unit testing pipelines
- **Connectors** - For pre-built source/sink nodes

### Can I use NPipeline with ASP.NET Core?

Yes! NPipeline integrates well with ASP.NET Core via dependency injection. See [Dependency Injection](../extensions/dependency-injection.md) for details.

### How do I set up logging?

Integrate with Microsoft.Extensions.Logging:

```csharp
services.AddLogging(builder => builder.AddConsole());
services.AddNPipeline(Assembly.GetExecutingAssembly());
```

Then inject `ILogger<T>` into your nodes.

## Architecture & Concepts

### What's the difference between IPipeline and IPipelineDefinition?

- **IPipelineDefinition** - A blueprint that defines your pipeline structure
- **IPipeline** - The compiled, runnable instance created from a definition
- You implement `IPipelineDefinition`, then `PipelineRunner` executes it

### Can a pipeline have multiple sources?

Yes. Multiple source nodes can connect to the same transform or separate transforms. See [Common Patterns](../core-concepts/common-patterns.md) for examples.

### What's the purpose of PipelineContext?

`PipelineContext` carries runtime information through the pipeline:

- Cancellation tokens
- Shared state between nodes
- Logging and observability services
- Custom application data

### How does data flow between nodes?

Data flows through `IDataPipe<T>` objects:

1. Source nodes produce an `IDataPipe<T>`
2. Transforms consume it and produce a new `IDataPipe<T>`
3. Sinks consume the final `IDataPipe<T>`

## Core Concepts

### What's the difference between batching and aggregation?

**Batching** groups items for operational efficiency - to optimize interactions with external systems like databases or APIs. It looks at the wall clock and says "every N items or every X seconds, send what we have."

**Aggregation** groups items for data correctness - to handle out-of-order or late-arriving events in event-time windows. It uses event timestamps, not arrival times, and can wait for latecomers within a configured grace period.

**Use batching when:**

- External systems work more efficiently with bulk operations
- You need to reduce API call overhead
- Order/timing of items doesn't affect correctness

**Use aggregation when:**

- Events can arrive out of order or late
- You need time-windowed summaries or counts
- Results must be correct despite late-arriving data

For a detailed comparison and decision framework, see [Grouping Strategies: Batching vs Aggregation](../core-concepts/grouping-strategies.md).

### When should I use ValueTask vs Task in transforms?

Use `ValueTask<T>` when your transform can complete synchronously in common cases (cache hits, simple calculations). Use `Task<T>` when your transform is almost always asynchronous.

**ValueTask benefits:**

- Zero heap allocations for synchronous completions
- Eliminates up to 90% of GC pressure in high-cache-hit scenarios
- Seamlessly transitions to async when needed

**Example pattern:**

```csharp
public override ValueTask<UserData> ExecuteAsync(string userId, PipelineContext context, CancellationToken cancellationToken)
{
    // Fast path: cache hit - no Task allocation
    if (_cache.TryGetValue(userId, out var cached))
        return new ValueTask<UserData>(cached);
    
    // Slow path: async database call
    return new ValueTask<UserData>(FetchAndCacheAsync(userId, cancellationToken));
}
```

For complete implementation guidelines and performance impact analysis, see [Synchronous Fast Paths and ValueTask Optimization](../advanced-topics/synchronous-fast-paths.md).

### Do I need ResilientExecutionStrategy for retries?

No, `ResilientExecutionStrategy` is specifically for **node-level restarts**, not basic item retries. There are two different retry mechanisms:

**Item-level retries** (no ResilientExecutionStrategy needed):

- Retry individual failed items
- Configured via `PipelineRetryOptions.MaxItemRetries`
- Handled in node error handlers with `NodeErrorDecision.Retry`

**Node-level restarts** (requires ResilientExecutionStrategy):

- Restart entire node streams on failure
- Configured via `PipelineRetryOptions.MaxNodeRestartAttempts`
- Requires three mandatory components:
  1. Node wrapped with `ResilientExecutionStrategy`
  2. `MaxNodeRestartAttempts > 0` in retry options
  3. `MaxMaterializedItems` set to a positive number (not null)

For detailed configuration requirements, see [Getting Started with Resilience](../core-concepts/resilience/getting-started.md).

## Performance

### How do I know if my pipeline is optimized?

Use these approaches to verify pipeline optimization:

1. **Enable build-time analyzers** to catch performance anti-patterns:

   ```csharp
   // In your .csproj
   <PackageReference Include="NPipeline.Analyzers" Version="*" />
   ```

2. **Check for common issues**:
   - Blocking operations in async methods (NP9102)
   - Missing ValueTask optimizations (NP9209)
   - Non-streaming patterns in source nodes (NP9211)
   - Swallowed cancellation exceptions (NP9103)

3. **Benchmark critical paths** with BenchmarkDotNet:

   ```csharp
   [MemoryDiagnoser]
   public class MyTransformBenchmarks
   {
       [Benchmark]
       public async Task Transform() => await _transform.ProcessAsync(_data);
   }
   ```

4. **Monitor runtime metrics**:
   - GC pressure and allocation rates
   - Throughput vs. latency trade-offs
   - Memory usage patterns

For comprehensive performance best practices, see [Performance Hygiene](../advanced-topics/performance-hygiene.md) and [Performance Analyzers](../analyzers/performance.md).

### What's the memory overhead of materialization?

Materialization buffers items in memory to enable replay functionality during node restarts. The memory overhead depends on:

1. **Item size**: Larger items consume more memory per buffered item
2. **Buffer limit**: `MaxMaterializedItems` determines maximum items buffered
3. **Buffer duration**: How many seconds of data you need to replay

**Example calculations:**

- Small objects (100 bytes): 10,000 items ≈ 1MB
- Medium objects (1KB): 1,000 items ≈ 1MB
- Large objects (10KB): 100 items ≈ 1MB

**Configuration guidance:**

```csharp
var options = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,
    MaxMaterializedItems: 1000  // Adjust based on item size and memory budget
);
```

For detailed memory calculations and configuration examples, see [Materialization & Buffering](../core-concepts/resilience/materialization.md).

## Troubleshooting

### My node restarts aren't working

Node restarts require **three mandatory components**. If any are missing, restarts silently fail:

1. **ResilientExecutionStrategy not applied**:

   ```csharp
   // REQUIRED
   var nodeHandle = builder
       .AddTransform<MyTransform, Input, Output>("myNode")
       .WithExecutionStrategy(builder, 
           new ResilientExecutionStrategy(new SequentialExecutionStrategy()));
   ```

2. **MaxNodeRestartAttempts not configured**:

   ```csharp
   // REQUIRED - must be > 0
   var options = new PipelineRetryOptions(
       MaxItemRetries: 3,
       MaxNodeRestartAttempts: 2,  // ← Must be set
       MaxMaterializedItems: 1000
   );
   ```

3. **MaxMaterializedItems is null** (most common issue):

   ```csharp
   // REQUIRED - must be positive number, not null
   var options = new PipelineRetryOptions(
       MaxItemRetries: 3,
       MaxNodeRestartAttempts: 2,
       MaxMaterializedItems: 1000  // ← CRITICAL: null disables restarts
   );
   ```

**Verification checklist:**

- [ ] Node wrapped with ResilientExecutionStrategy
- [ ] MaxNodeRestartAttempts > 0
- [ ] MaxMaterializedItems is set to positive number
- [ ] Error handler returns PipelineErrorDecision.RestartNode

For the complete checklist and troubleshooting guide, see [Getting Started with Resilience](../core-concepts/resilience/getting-started.md).

### My parallel pipeline is slower than sequential

Common parallelism anti-patterns that can make parallel pipelines slower:

1. **Resource contention**:
   - Multiple threads competing for same database connection
   - Shared resources without proper synchronization
   - Too high degree of parallelism causing context switching overhead

2. **I/O-bound work with excessive parallelism**:

   ```csharp
   // WRONG: Too much parallelism for I/O work
   new ParallelOptions { MaxDegreeOfParallelism = 100 }
   
   // BETTER: Match to I/O capacity
   new ParallelOptions { MaxDegreeOfParallelism = 4 }
   ```

3. **Unnecessary ordering preservation**:

   ```csharp
   // SLOWER: Preserves order (default)
   new ParallelOptions { PreserveOrdering = true }
   
   // FASTER: When order doesn't matter
   new ParallelOptions { PreserveOrdering = false }
   ```

4. **Improper queue configuration**:
   - Unbounded queues causing memory pressure
   - Too small queues causing blocking

**Optimization steps:**

1. Start with `MaxDegreeOfParallelism = Environment.ProcessorCount`
2. Set `PreserveOrdering = false` if order isn't required
3. Configure appropriate queue limits with `MaxQueueLength`
4. Profile to identify actual bottlenecks

For detailed parallelism configuration and best practices, see [Parallelism](../extensions/parallelism/index.md).

## Error Handling

### What happens if a transform throws an exception?

By default, the exception propagates up and stops the pipeline. Handle errors explicitly:

```csharp
try
{
    return await ProcessAsync(item);
}
catch (Exception ex)
{
    logger.LogError(ex, "Processing failed");
    throw; // or handle gracefully
}
```

### How do I implement retries?

NPipeline provides several built-in retry mechanisms:

**1. Item-level retries** - Retry individual items that fail:

```csharp
// Configure max retries per item
builder.WithRetryOptions(opt => opt.With(maxItemRetries: 3));

// In your error handler, return Retry for transient failures
public Task<NodeErrorDecision> HandleAsync(...)
{
    if (exception is TransientException)
        return Task.FromResult(NodeErrorDecision.Retry);

    return Task.FromResult(NodeErrorDecision.Skip);
}
```

**2. Node-level restarts** - Restart entire node streams on failure:

```csharp
// Enable resilience and configure restart limits
var myNode = builder
    .AddTransform<MyTransform, Input, Output>("myTransform")
    .WithResilience(builder)
    .WithRetryOptions(builder, opt => opt.With(
        maxNodeRestartAttempts: 3,
        maxMaterializedItems: 5000
    ));

// In your pipeline error handler, return RestartNode
public Task<PipelineErrorDecision> HandleNodeFailureAsync(...)
{
    if (exception is TimeoutException)
        return Task.FromResult(PipelineErrorDecision.RestartNode);

    return Task.FromResult(PipelineErrorDecision.FailPipeline);
}
```

**3. Circuit breaker** - Prevent cascading failures:

```csharp
// Configure circuit breaker through PipelineRetryOptions
var retryOptions = new PipelineRetryOptions(
    MaxItemRetries: 3,
    MaxNodeRestartAttempts: 2,
    MaxSequentialNodeAttempts: 5,
    CircuitBreakerOptions: new PipelineCircuitBreakerOptions(
        failureThreshold: 5,
        openDuration: TimeSpan.FromMinutes(1),
        samplingWindow: TimeSpan.FromMinutes(5),
        thresholdType: CircuitBreakerThresholdType.ConsecutiveFailures
    )
);
var context = PipelineContext.WithRetry(retryOptions);
```

For advanced patterns beyond simple retry counts, integrate external libraries like [Polly](https://github.com/App-vNext/Polly):

```csharp
// Exponential backoff with Polly
var policy = Policy
    .Handle<HttpRequestException>()
    .WaitAndRetryAsync(3, attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)));

await policy.ExecuteAsync(async () => await CallServiceAsync());
```

See [Configuring Retries](../core-concepts/resilience/error-handling.md#configuring-retries) for detailed examples and best practices.

## Error Codes Reference

### What do the error codes like [NP0101] mean?

NPipeline uses standardized error codes (NP prefix) to help you quickly identify and resolve issues:

- **NP01xx** - Graph Validation Errors
- **NP02xx** - Type Mismatch and Conversion Errors  
- **NP03xx** - Execution Errors
- **NP04xx** - Configuration Errors
- **NP05xx** - Resource and Capacity Errors

See the [Error Codes Reference](./error-codes.md) for a complete list of all error codes, their causes, and solutions.

### How do I handle invalid data?

Route invalid data to a separate error stream or log it:

```csharp
public override Task<Order> ExecuteAsync(Order item, PipelineContext context, CancellationToken cancellationToken)
{
    if (item.Price < 0)
    {
        logger.LogWarning($"Invalid price: {item.Price}");
        throw new ValidationException("Price cannot be negative");
    }
    return Task.FromResult(item);
}
```

## Testing

### How do I test a pipeline?

Use `InMemorySourceNode` and `InMemorySinkNode`:

```csharp
var context = new PipelineContext();
context.Items[typeof(InMemorySourceNode<int>).FullName!] = new[] { 1, 2, 3 };

var runner = PipelineRunner.Create();
await runner.RunAsync<TestPipeline>(context);

// Retrieve results
var sink = context.Items[typeof(InMemorySinkNode<int>).FullName!] as InMemorySinkNode<int>;
var results = await sink!.Completion;
```

See [Testing Pipelines](../extensions/testing/index.md) for details.

### Can I test individual nodes?

Yes. Call `ExecuteAsync` directly with test data:

```csharp
var transform = new MyTransform();
var context = new PipelineContext();
var result = await transform.ExecuteAsync(testData, context, CancellationToken.None);
Assert.Equal(expected, result);
```

### How do I mock external dependencies?

Use dependency injection with mock services:

```csharp
var mockService = new Mock<IMyService>();
var node = new MyNode(mockService.Object);
```

## Integration

### How do I use NPipeline with a database?

Implement custom source and sink nodes:

```csharp
public class DatabaseSource : SourceNode<Customer>
{
    private readonly string _connectionString;

    public override IDataPipe<Customer> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        static IAsyncEnumerable<Customer> ReadAsync(string connStr, CancellationToken ct)
        {
            return Read();

            async IAsyncEnumerable<Customer> Read()
            {
                using var connection = new SqlConnection(connStr);
                await connection.OpenAsync(ct);
                // Read and yield customers
            }
        }

        return new StreamingDataPipe<Customer>(ReadAsync(_connectionString, cancellationToken));
    }
}
```

### How do I use NPipeline with Message Queues (like RabbitMQ)?

Implement a source node that reads from the queue:

```csharp
public class QueueSource : SourceNode<Message>
{
    private readonly IQueueClient _client;

    public override IDataPipe<Message> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        static IAsyncEnumerable<Message> ReadAsync(IQueueClient client, CancellationToken ct)
        {
            return Read();

            async IAsyncEnumerable<Message> Read()
            {
                while (!ct.IsCancellationRequested)
                {
                    var message = await client.ReceiveAsync(ct);
                    if (message != null)
                    {
                        yield return message;
                    }
                }
            }
        }

        return new StreamingDataPipe<Message>(ReadAsync(_client, cancellationToken));
    }
}
```

### Can I use NPipeline with dependency injection containers?

Yes, with the `NPipeline.Extensions.DependencyInjection` package:

```csharp
services.AddNPipeline(Assembly.GetExecutingAssembly());
// Your nodes are automatically registered and resolved
```

## See Also

- **[Getting Started](../getting-started/index.md)** - Installation and quick start
- **[Common Patterns](../core-concepts/common-patterns.md)** - See practical examples
- **[Best Practices](../core-concepts/best-practices.md)** - Design guidelines
- **[Troubleshooting](./troubleshooting.md)** - Common issues and solutions
