---
title: Best Practices
description: Guidelines and considerations for optimizing parallelism in your NPipeline pipelines.
sidebar_position: 4
---

# Best Practices and Considerations

This section provides guidelines and considerations for effectively using parallelism in your NPipeline pipelines.

## Key Considerations

### Degree of Parallelism

Carefully choose the `MaxDegreeOfParallelism`. Too high a value can lead to excessive resource consumption (CPU, memory, threads) and diminish returns due to context switching overhead. Too low a value might underutilize available resources.

**Guidelines:**

- Start with `ProcessorCount` for CPU-bound workloads
- Use `ProcessorCount * 2-4` for I/O-bound workloads
- Profile your application to find the optimal balance
- Consider the nature of your workload (CPU vs. I/O bound)

### Thread Safety

Ensure that any shared state or external resources accessed by your parallel nodes are thread-safe. If your nodes are pure functions (operating only on their input and producing output without side effects), this is less of a concern.

**Guidelines:**

- Use [`IPipelineStateManager`](./thread-safety.md) for shared state
- Avoid accessing `context.Items` or `context.Parameters` during parallel processing
- Use `lock` or `Interlocked` for node-level synchronization
- Keep critical sections small and fast

### Order Preservation

By default, NPipeline maintains the order of items even when processing them in parallel. If order is not critical and you need maximum throughput, you can configure nodes to not preserve order by setting `PreserveOrdering = false`.

**Guidelines:**

- Preserve ordering only when downstream processing requires it
- Consider disabling ordering for aggregation or statistics collection
- Be aware that non-ordered output may require downstream handling

### Resource Contention

Be aware of potential bottlenecks when multiple parallel tasks try to access the same limited resource (e.g., a single database connection, a slow API).

**Guidelines:**

- Identify shared resources in your pipeline
- Use connection pooling for database access
- Implement rate limiting for external API calls
- Consider batching requests to reduce contention

### Debugging

Debugging parallel code can be more complex. Ensure you have good logging and monitoring in place.

**Guidelines:**

- Include thread IDs in log messages for tracking
- Use structured logging with correlation IDs
- Monitor queue depths and worker utilization
- Set up alerts for abnormal patterns

## Best Practices

### Identify Parallelizable Work

Apply parallelism to parts of your pipeline where operations are independent and computationally intensive.

**Characteristics of parallelizable work:**

- Operations that don't depend on each other's results
- CPU-intensive transformations
- I/O operations that can be performed concurrently
- Data processing that can be split into independent chunks

### Start Small

Begin with a low degree of parallelism and incrementally increase it while monitoring performance metrics (CPU, memory, throughput) to find the optimal balance.

**Recommended approach:**

1. Start with `MaxDegreeOfParallelism = 2`
2. Measure baseline performance
3. Incrementally increase parallelism (4, 8, 16, etc.)
4. Monitor performance metrics at each step
5. Stop when you see diminishing returns

### Profile

Use profiling tools to identify bottlenecks and ensure that parallelism is indeed improving performance.

**Key metrics to monitor:**

- CPU utilization per core
- Memory usage and garbage collection
- Queue depth and worker utilization
- End-to-end latency
- Throughput (items per second)

### Use Appropriate Workload Types

Leverage the built-in workload type presets for common scenarios:

| Workload Type | Use Case |
|---------------|----------|
| `General` | Mixed CPU and I/O workloads |
| `CpuBound` | Mathematical computations, data processing |
| `IoBound` | File I/O, database operations |
| `NetworkBound` | HTTP calls, remote service calls |

### Validate Configuration

Always validate your pipeline configuration before running in production:

```csharp
var builder = new PipelineBuilder();
// ... build pipeline ...

var result = builder.Validate();
if (!result.IsValid)
{
    // Handle errors
}

if (result.Warnings.Count > 0)
{
    // Review warnings
}
```

### Handle Errors Gracefully

Implement proper error handling for parallel operations:

```csharp
public class ResilientTransform : TransformNode<int, int>
{
    public override async ValueTask<int> TransformAsync(
        int input,
        PipelineContext context,
        CancellationToken ct)
    {
        try
        {
            return await ProcessAsync(input, ct);
        }
        catch (Exception ex)
        {
            context.Logger.LogError(ex, "Error processing item {Item}", input);
            throw; // Or handle based on your requirements
        }
    }
}
```

### Monitor and Tune

Continuously monitor your parallel pipelines and tune configuration based on observed behavior:

- Adjust `MaxDegreeOfParallelism` based on CPU utilization
- Tune queue lengths based on memory constraints
- Adjust `MetricsInterval` based on monitoring needs
- Review and update workload types as requirements change

## Common Pitfalls to Avoid

### Over-Parallelization

Using too high a degree of parallelism can degrade performance due to:

- Context switching overhead
- Thread pool starvation
- Increased memory usage
- Resource contention

**Solution:** Profile and find the optimal parallelism level for your workload.

### Ignoring Thread Safety

Failing to properly synchronize access to shared state leads to:

- Data races and corruption
- Non-deterministic behavior
- Hard-to-reproduce bugs

**Solution:** Always use thread-safe mechanisms for shared state. See [Thread Safety Guidelines](./thread-safety.md).

### Unbounded Queues

Not setting queue limits can cause:

- Unbounded memory growth
- Out-of-memory errors under load
- System instability

**Solution:** Always set `MaxQueueLength` for high-parallelism scenarios.

### Preserving Order Unnecessarily

Preserving order when not needed causes:

- Increased latency
- Higher memory usage
- Reduced throughput

**Solution:** Disable ordering (`PreserveOrdering = false`) when order doesn't matter.

### Blocking Operations in Parallel Workers

Performing blocking operations in parallel workers can:

- Reduce parallelism benefits
- Cause thread pool starvation
- Increase latency

**Solution:** Use async/await for I/O operations and avoid blocking calls.

## Performance Optimization Checklist

- [ ] Profile baseline performance before adding parallelism
- [ ] Choose appropriate workload type for your scenario
- [ ] Start with low parallelism and incrementally increase
- [ ] Set appropriate queue limits to prevent unbounded growth
- [ ] Disable order preservation when not needed
- [ ] Implement thread-safe shared state access
- [ ] Add comprehensive logging and monitoring
- [ ] Validate configuration before production deployment
- [ ] Monitor performance metrics and tune accordingly
- [ ] Handle errors gracefully

By strategically applying parallelism and following these best practices, you can significantly boost the processing capabilities of your NPipelines for demanding workloads.

## Related Topics

- **[Thread Safety Guidelines](./thread-safety.md)**: Comprehensive guide to thread safety and shared state management
- **[Configuration](./configuration.md)**: Learn about different configuration APIs and validation
- **[Validation](./validation.md)**: Learn about parallel configuration validation rules
- **[Dependency Injection](../dependency-injection.md)**: Learn how to integrate NPipeline with dependency injection frameworks
- **[Testing Pipelines](../testing/index.md)**: Understand how to effectively test your parallel pipelines
