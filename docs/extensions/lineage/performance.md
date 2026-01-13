---
title: Lineage Performance
description: Performance characteristics, benchmarks, and optimization strategies for NPipeline Lineage extension.
---

# Lineage Performance

This guide covers performance characteristics, benchmarks, and optimization strategies for NPipeline Lineage extension.

## Performance Characteristics

### Overhead Breakdown

Lineage tracking introduces overhead at several points in pipeline execution:

| Component | Impact | Notes |
|-----------|--------|-------|
| LineagePacket creation | Per item | One-time allocation |
| Hop recording | Per hop | Metadata updates |
| Sampling check | Per hop | Hash or random operation |
| Dictionary lookup | Per hop | ConcurrentDictionary access |
| Sink export | Per item | Depends on sink implementation |

**Note:** Total overhead scales with the number of items processed and the number of hops in the pipeline.

### Estimated Performance Impact

Performance impact varies based on configuration and pipeline characteristics:

| Configuration | Relative Impact | Notes |
|--------------|-----------------|---------|
| Without lineage (baseline) | None | No tracking overhead |
| 100% tracking, no redaction | Highest | Complete visibility |
| 10% sampling, no redaction | Low | Good balance |
| 1% sampling, no redaction | Minimal | Minimal overhead |
| 10% sampling, with redaction | Low | Reduces memory usage |

**Important:** Actual performance impact depends on data sizes, pipeline complexity, and hardware. Measure performance in your specific environment to determine the actual impact.

## Memory Usage

### Per-Item Memory

Memory usage scales with data size and number of hops:

```csharp
// Approximate memory per item
var perItemMemory = 
    sizeof(Guid) +                          // LineageId: 16 bytes
    sizeof(List<string>) +                   // TraversalPath: list overhead
    sizeof(List<LineageHop>) +              // LineageHops: list overhead
    (hopCount * perHopOverhead) +           // Per hop: metadata
    (dataRedacted ? 0 : dataSize);         // Data: 0 or actual size
```

**Factors affecting memory:**

- Data size (unless redacted)
- Number of hops in the pipeline
- Number of items sampled
- Metadata stored per hop

### Per-Pipeline Memory

Fixed overhead per pipeline execution:

| Component | Memory | Notes |
|-----------|---------|-------|
| LineageCollector | Dictionary overhead | Scales with items |
| LineageOptions | Configuration | Negligible |
| Total | Varies | Depends on items sampled |

### Memory Scaling

Memory usage scales linearly with:

- **Number of items sampled**: Each sampled item adds memory
- **Number of hops**: Each hop adds metadata
- **Data size**: Proportional to actual data (unless redacted)

**Formula:**

```
TotalMemory = PipelineOverhead + (SampledItems × PerItemMemory)
```

**Note:** Use sampling and redaction to control memory usage. The materialization cap provides additional protection against excessive memory consumption.

## CPU Impact

### Per-Operation Costs

| Operation | Cost | Frequency | Notes |
|-----------|------|-----------|-------|
| LineagePacket creation | Per item | One-time | Allocation |
| Sampling check | Per hop | Hash or random | Fast operation |
| Hop recording | Per hop | List operations | Metadata updates |
| Dictionary lookup | Per hop | ConcurrentDictionary | Efficient access |
| Sink export | Per item | Depends on sink | I/O bound |

### Hash vs Random Sampling

| Sampling Type | Characteristics | Use Case |
|--------------|-----------------|-----------|
| Deterministic (hash) | Consistent items across runs | Debugging, compliance |
| Random | Different items each run | Monitoring, analytics |

**Note:** Both sampling methods have similar CPU characteristics. Hash-based sampling provides consistency across runs.

## Optimization Strategies

### 1. Use Sampling

The most effective optimization is sampling:

```csharp
// Production: 1% sampling
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 100;
    options.DeterministicSampling = true;
});

// High-volume: 0.1% sampling
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 1000;
    options.DeterministicSampling = false;
});
```

**Impact:**

- Reduces memory usage proportionally
- Reduces CPU overhead proportionally
- Maintains representative visibility

### 2. Enable Data Redaction

Redact data when possible:

```csharp
builder.EnableItemLevelLineage(options =>
{
    options.RedactData = true;  // Reduces memory usage
});
```

**Impact:**

- Reduces memory usage by not storing actual data
- No CPU impact
- Maintains all metadata

### 3. Use Materialization Cap

Limit in-memory storage:

```csharp
builder.EnableItemLevelLineage(options =>
{
    options.MaterializationCap = 10000;  // Default
    options.OverflowPolicy = LineageOverflowPolicy.Degrade;
});
```

**Impact:**

- Predictable memory usage
- Prevents out-of-memory errors
- Graceful degradation

### 4. Use Async Sinks

Implement async sink operations:

```csharp
public sealed class DatabaseLineageSink : ILineageSink
{
    public async Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
    {
        // Non-blocking async I/O
        await _database.SaveChangesAsync(cancellationToken);
    }
}
```

**Impact:**

- Non-blocking to pipeline execution
- Better throughput
- No pipeline stalls

### 5. Batch Sink Operations

Batch multiple lineage records:

```csharp
public sealed class BatchedLineageSink : ILineageSink
{
    private readonly List<LineageInfo> _batch = new();
    private readonly int _batchSize;

    public Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
    {
        lock (_batch)
        {
            _batch.Add(lineageInfo);
            
            if (_batch.Count >= _batchSize)
            {
                return FlushBatchAsync(cancellationToken);
            }
        }
        return Task.CompletedTask;
    }

    private async Task FlushBatchAsync(CancellationToken cancellationToken)
    {
        List<LineageInfo> itemsToFlush;
        lock (_batch)
        {
            itemsToFlush = _batch.ToList();
            _batch.Clear();
        }
        
        await _database.BulkInsertAsync(itemsToFlush, cancellationToken);
    }
}
```

**Impact:**

- Reduces database round trips
- Better throughput
- Lower CPU overhead

### 6. Use Degrade Overflow Policy

Default policy provides best balance:

```csharp
options.OverflowPolicy = LineageOverflowPolicy.Degrade;
```

**Impact:**

- Predictable memory usage
- Maintains visibility
- Automatic cleanup

### 7. Minimize Hop Count

Reduce pipeline complexity:

```csharp
// Complex: Many hops
[Source] → [Transform1] → [Transform2] → [Transform3] → [Sink]

// Simplified: Fewer hops
[Source] → [CombinedTransform] → [Sink]
```

**Impact:**

- Reduces lineage overhead
- Faster pipeline execution
- Lower memory usage

## Benchmarking

### Measuring Lineage Overhead

```csharp
using BenchmarkDotNet.Attributes;

[MemoryDiagnoser]
public class LineageBenchmarks
{
    private PipelineRunner _runner = null!;
    private PipelineContext _context = null!;
    
    [GlobalSetup]
    public void Setup()
    {
        _runner = PipelineRunner.Create();
        _context = new PipelineContext();
    }
    
    [Benchmark(Baseline = true)]
    public async Task NoLineage()
    {
        var builder = new PipelineBuilder("TestPipeline");
        // No lineage enabled
        await _runner.RunAsync(builder.Build(), _context);
    }
    
    [Benchmark]
    public async Task WithLineage_100Percent()
    {
        var builder = new PipelineBuilder("TestPipeline");
        builder.EnableItemLevelLineage(options =>
        {
            options.SampleEvery = 1;  // 100%
        });
        await _runner.RunAsync(builder.Build(), _context);
    }
    
    [Benchmark]
    public async Task WithLineage_10Percent()
    {
        var builder = new PipelineBuilder("TestPipeline");
        builder.EnableItemLevelLineage(options =>
        {
            options.SampleEvery = 10;  // 10%
        });
        await _runner.RunAsync(builder.Build(), _context);
    }
    
    [Benchmark]
    public async Task WithLineage_Redacted()
    {
        var builder = new PipelineBuilder("TestPipeline");
        builder.EnableItemLevelLineage(options =>
        {
            options.SampleEvery = 1;
            options.RedactData = true;
        });
        await _runner.RunAsync(builder.Build(), _context);
    }
}
```

**Note:** Run benchmarks in your specific environment with representative data to measure actual performance impact.

## Real-World Scenarios

### Scenario 1: Production ETL Pipeline

**Requirements:**

- Process items at production volume
- Multiple nodes in pipeline
- Need compliance tracking

**Configuration:**

```csharp
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 1;  // 100% for compliance
    options.RedactData = true;  // Sensitive data
    options.MaterializationCap = 100000;  // All items
    options.OverflowPolicy = LineageOverflowPolicy.Degrade;
});
```

**Considerations:**

- CPU overhead is present but acceptable for compliance
- Memory usage scales with number of items processed
- Throughput impact depends on pipeline complexity and data sizes

### Scenario 2: High-Volume Analytics Pipeline

**Requirements:**

- High throughput processing
- Multiple nodes in pipeline
- Need monitoring, not compliance

**Configuration:**

```csharp
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 1000;  // 0.1% sampling
    options.RedactData = true;
    options.MaterializationCap = 1000;  // Small cap
    options.OverflowPolicy = LineageOverflowPolicy.Drop;
});
```

**Considerations:**

- Minimal CPU overhead with aggressive sampling
- Low memory footprint with small cap
- Throughput impact is negligible

### Scenario 3: Development/Debugging Pipeline

**Requirements:**

- Process items for testing
- Multiple nodes in pipeline
- Need complete visibility

**Configuration:**

```csharp
builder.EnableItemLevelLineage(options =>
{
    options.SampleEvery = 1;  // 100% for debugging
    options.RedactData = false;  // Keep data for inspection
    options.MaterializationCap = int.MaxValue;  // No cap
    options.OverflowPolicy = LineageOverflowPolicy.Materialize;
});
```

**Considerations:**

- CPU overhead is acceptable for development
- Memory usage scales with test data size
- Throughput is not critical for development

## Performance Monitoring

### Track Lineage Performance

Monitor lineage-specific metrics:

```csharp
public sealed class LineagePerformanceSink : ILineageSink
{
    private readonly ILogger _logger;
    private readonly Stopwatch _sw = new();

    public LineagePerformanceSink(ILogger<LineagePerformanceSink> logger)
    {
        _logger = logger;
    }

    public async Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
    {
        _sw.Restart();
        
        // Actual sink operation
        await _database.SaveAsync(lineageInfo, cancellationToken);
        
        var elapsed = _sw.ElapsedMilliseconds;
        
        if (elapsed > 100)
        {
            _logger.LogWarning(
                "Slow lineage export: {ElapsedMs}ms for {LineageId}",
                elapsed,
                lineageInfo.LineageId);
        }
    }
}
```

### Metrics to Track

- **Lineage export time**: Time to write to sinks
- **Memory usage**: Collector memory over time
- **Sampling rate**: Actual vs configured
- **Overflow events**: How often cap is reached
- **Sink errors**: Failed exports

## Best Practices

### 1. Profile Before Optimizing

Measure before making changes:

```csharp
// Use BenchmarkDotNet or similar tools
[Benchmark]
public async Task Baseline()
{
    await RunPipeline(withLineage: false);
}

[Benchmark]
public async Task WithLineage()
{
    await RunPipeline(withLineage: true);
}
```

### 2. Start Conservative, Adjust Later

Begin with low sampling rate:

```csharp
options.SampleEvery = 100;  // Conservative start
```

Monitor and adjust based on requirements.

### 3. Use Appropriate Overflow Policy

Choose policy based on scenario:

| Scenario | Policy |
|-----------|----------|
| Production | Degrade |
| Development | Materialize |
| High-volume | Drop |
| Compliance | Degrade |

### 4. Implement Async Sinks

Always use async operations in sinks:

```csharp
public async Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
{
    await _repository.SaveAsync(lineageInfo, cancellationToken);
}
```

### 5. Monitor Memory Usage

Track collector memory:

```csharp
var collector = serviceProvider.GetRequiredService<ILineageCollector>();
var lineageCount = collector.GetAllLineageInfo().Count;
var estimatedMemory = lineageCount * 750;  // Approximate bytes

_logger.LogInformation(
    "Lineage memory: {Count} items, ~{MemoryMB} MB",
    lineageCount,
    estimatedMemory / (1024 * 1024));
```

**Note:** Actual memory usage depends on data sizes and pipeline complexity. Monitor in production to understand real memory consumption.

## Related Topics

- **[Getting Started](./getting-started.md)** - Installation and basic setup
- **[Configuration](./configuration.md)** - Configuration options and settings
- **[Architecture](./architecture.md)** - Internal architecture and design decisions
- **[Use Cases](./use-cases.md)** - Common use cases and examples
