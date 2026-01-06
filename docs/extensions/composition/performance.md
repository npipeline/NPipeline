# Performance Optimization

## Overview

Composite pipelines introduce some overhead compared to flat pipelines. This guide helps you understand the performance characteristics and optimize for your use case.

## Performance Characteristics

### Overhead Breakdown

Per-item overhead when using composite nodes:

| Component | Time | Memory | Impact |
|-----------|------|--------|--------|
| Context Creation | ~1-2μs | ~1-5KB | Per item per level |
| Input/Output Transfer | ~0.5μs | 0 (zero-copy) | Per item per level |
| Pipeline Runner Reuse | 0 | 0 (shared) | No impact |
| Sub-Pipeline Execution | Variable | Variable | Depends on nodes |

**Total Overhead:** ~2-3μs per item per nesting level (excluding actual processing)

### Comparison: Flat vs Composite

```csharp
// Flat pipeline
[Source] → [Transform1] → [Transform2] → [Transform3] → [Sink]
Overhead: ~0μs (baseline)

// Composite pipeline (1 level)
[Source] → [Composite{Transform1 → Transform2 → Transform3}] → [Sink]
Overhead: ~2-3μs per item

// Nested composite (2 levels)
[Source] → [Composite{Composite{Transform1 → Transform2} → Transform3}] → [Sink]
Overhead: ~4-6μs per item
```

## When to Use Composition

### Good Use Cases (✅)

Composition is beneficial when:

1. **Modularity > Raw Speed**
   ```csharp
   // Good: Reusable validation across multiple pipelines
   builder.AddComposite<Data, ValidatedData, ValidationPipeline>("validate");
   ```

2. **Low-Throughput Pipelines**
   ```csharp
   // Good: Processing < 1000 items/sec
   // Overhead is negligible compared to processing time
   ```

3. **Complex Business Logic**
   ```csharp
   // Good: Complex logic benefits from modular structure
   builder.AddComposite<Order, ProcessedOrder, OrderProcessingPipeline>("process");
   ```

4. **Testing and Maintenance**
   ```csharp
   // Good: Sub-pipelines can be tested independently
   // Easier to maintain and debug
   ```

### Avoid Composition (❌)

Composition may not be ideal when:

1. **Ultra-High Throughput**
   ```csharp
   // Bad: Processing millions of items/sec
   // Consider flattening the pipeline
   ```

2. **Simple Linear Processing**
   ```csharp
   // Bad: Simple transform doesn't need composition
   builder.AddComposite<int, int, MultiplyByTwoPipeline>("double");
   
   // Better: Direct transform
   builder.AddTransform<MultiplyByTwo, int, int>("double");
   ```

3. **Excessive Nesting**
   ```csharp
   // Bad: 5+ levels of nesting
   // Flatten or refactor to reduce levels
   ```

## Optimization Techniques

### 1. Minimize Context Inheritance

Use default (no inheritance) when possible:

```csharp
✅ Optimized: No inheritance overhead
builder.AddComposite<T, T, SubPipeline>(
    contextConfiguration: CompositeContextConfiguration.Default);

❌ Slow: Copying large context dictionaries
var context = new PipelineContext();
// Add 1000 parameters
for (int i = 0; i < 1000; i++)
{
    context.Parameters[$"key_{i}"] = $"value_{i}";
}

builder.AddComposite<T, T, SubPipeline>(
    contextConfiguration: CompositeContextConfiguration.InheritAll);
// Each item copies 1000 parameters!
```

**Benchmark Results:**
- No inheritance: ~1μs per item
- InheritAll (empty context): ~1μs per item
- InheritAll (1000 parameters): ~50μs per item

### 2. Reduce Nesting Depth

Flatten deep hierarchies:

```csharp
❌ Slow: 4 levels deep
[A] → [Composite{[B] → [Composite{[C] → [Composite{[D]}]}]}]
Overhead: ~8-12μs per item

✅ Fast: 2 levels
[A] → [Composite{[B] → [C] → [D]}]
Overhead: ~2-3μs per item

✅ Fastest: Flat
[A] → [B] → [C] → [D]
Overhead: ~0μs
```

### 3. Batch Processing

Process multiple items in sub-pipelines:

```csharp
// Instead of: One item at a time
builder.AddComposite<Item, ProcessedItem, ItemPipeline>("process");

// Consider: Batching items
builder.AddComposite<List<Item>, List<ProcessedItem>, BatchPipeline>("process-batch");

public class BatchPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var input = builder.AddSource<PipelineInputSource<List<Item>>, List<Item>>("input");
        var process = builder.AddTransform<BatchProcessor, List<Item>, List<ProcessedItem>>("process");
        var output = builder.AddSink<PipelineOutputSink<List<ProcessedItem>>, List<ProcessedItem>>("output");
        
        builder.Connect(input, process);
        builder.Connect(process, output);
    }
}

// Amortize overhead across multiple items
```

**Benchmark Results:**
- Single item: ~3μs overhead per item
- Batch of 10: ~0.3μs overhead per item
- Batch of 100: ~0.03μs overhead per item

### 4. Reuse Pipeline Definitions

Pipeline definitions are lightweight; the runner is shared:

```csharp
✅ Good: Same definition, multiple instances
public class ParentPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var validate1 = builder.AddComposite<Data, Data, ValidationPipeline>("validate1");
        var validate2 = builder.AddComposite<Data, Data, ValidationPipeline>("validate2");
        var validate3 = builder.AddComposite<Data, Data, ValidationPipeline>("validate3");
        
        // All share the same PipelineRunner instance (efficient)
    }
}
```

### 5. Async Processing

Use async operations in transforms:

```csharp
public class AsyncTransform : TransformNode<Data, Data>
{
    private readonly HttpClient _client;
    
    public override async Task<Data> ExecuteAsync(Data input, PipelineContext context, CancellationToken ct)
    {
        // Efficient async I/O
        var result = await _client.GetAsync($"/api/data/{input.Id}", ct);
        var enriched = await result.Content.ReadAsAsync<EnrichedData>();
        
        return input with { Enriched = enriched };
    }
}
```

## Benchmarking

### Measuring Composite Overhead

```csharp
using BenchmarkDotNet.Attributes;

[MemoryDiagnoser]
public class CompositionBenchmarks
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
    public async Task FlatPipeline()
    {
        await _runner.RunAsync<FlatPipeline>(_context);
    }
    
    [Benchmark]
    public async Task CompositePipeline_1Level()
    {
        await _runner.RunAsync<CompositePipeline1Level>(_context);
    }
    
    [Benchmark]
    public async Task CompositePipeline_2Levels()
    {
        await _runner.RunAsync<CompositePipeline2Levels>(_context);
    }
    
    [Benchmark]
    public async Task CompositePipeline_NoInheritance()
    {
        await _runner.RunAsync<CompositePipelineNoInheritance>(_context);
    }
    
    [Benchmark]
    public async Task CompositePipeline_InheritAll()
    {
        await _runner.RunAsync<CompositePipelineInheritAll>(_context);
    }
}
```

**Example Results:**
```
| Method                              | Mean     | Error   | StdDev  | Ratio | Gen0   | Allocated |
|------------------------------------ |---------:|--------:|--------:|------:|-------:|----------:|
| FlatPipeline                        | 100.0 μs | 1.5 μs  | 1.4 μs  | 1.00  | 2.0    | 8 KB      |
| CompositePipeline_1Level            | 103.0 μs | 1.6 μs  | 1.5 μs  | 1.03  | 2.5    | 10 KB     |
| CompositePipeline_2Levels           | 106.0 μs | 1.7 μs  | 1.6 μs  | 1.06  | 3.0    | 12 KB     |
| CompositePipeline_NoInheritance     | 102.5 μs | 1.6 μs  | 1.5 μs  | 1.03  | 2.5    | 10 KB     |
| CompositePipeline_InheritAll        | 110.0 μs | 2.0 μs  | 1.9 μs  | 1.10  | 3.5    | 15 KB     |
```

### Profiling Tools

Use these tools to identify bottlenecks:

1. **BenchmarkDotNet** - Detailed performance benchmarks
2. **dotnet-trace** - CPU profiling
3. **dotnet-counters** - Real-time metrics
4. **Visual Studio Profiler** - Memory and CPU analysis

## Real-World Scenarios

### Scenario 1: Data Enrichment Pipeline

**Requirements:**
- Process 10,000 items/sec
- Each item requires external API calls
- Need modular validation and enrichment

**Analysis:**
```csharp
// Composite overhead: ~3μs per item
// API call: ~50-100ms per item
// Overhead is 0.003% of total time - negligible

✅ Composition is appropriate here
```

**Implementation:**
```csharp
public class EnrichmentPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<DataSource, Item>("source");
        
        // Validate (composite)
        var validate = builder.AddComposite<Item, ValidatedItem, ValidationPipeline>("validate");
        
        // Enrich (composite, parallel API calls)
        var enrich = builder.AddComposite<ValidatedItem, EnrichedItem, EnrichmentSubPipeline>("enrich");
        
        var sink = builder.AddSink<DatabaseSink, EnrichedItem>("sink");
        
        builder.Connect(source, validate);
        builder.Connect(validate, enrich);
        builder.Connect(enrich, sink);
    }
}
```

### Scenario 2: High-Throughput Log Processing

**Requirements:**
- Process 1,000,000 items/sec
- Simple parsing and filtering
- Need high performance

**Analysis:**
```csharp
// Composite overhead: ~3μs per item
// Total processing time: ~5μs per item
// Overhead is 60% of total time - significant!

❌ Composition may not be appropriate here
```

**Alternative:**
```csharp
// Use flat pipeline for maximum performance
public class LogProcessingPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<LogSource, LogEntry>("source");
        
        // Inline transforms instead of composite
        var parse = builder.AddTransform<Parser, LogEntry, ParsedEntry>("parse");
        var filter = builder.AddTransform<Filter, ParsedEntry, ParsedEntry>("filter");
        var transform = builder.AddTransform<Transformer, ParsedEntry, ProcessedEntry>("transform");
        
        var sink = builder.AddSink<LogSink, ProcessedEntry>("sink");
        
        builder.Connect(source, parse);
        builder.Connect(parse, filter);
        builder.Connect(filter, transform);
        builder.Connect(transform, sink);
    }
}
```

### Scenario 3: ETL Pipeline

**Requirements:**
- Process 100,000 items/hour (27 items/sec)
- Complex transformations
- Need testing and maintainability

**Analysis:**
```csharp
// Composite overhead: ~3μs per item
// Total processing time: ~50ms per item
// Overhead is 0.006% of total time - negligible

✅ Composition is excellent here
```

**Implementation:**
```csharp
public class ETLPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<DatabaseSource, RawData>("source");
        
        // Extract (composite)
        var extract = builder.AddComposite<RawData, ExtractedData, ExtractionPipeline>("extract");
        
        // Transform (composite)
        var transform = builder.AddComposite<ExtractedData, TransformedData, TransformationPipeline>("transform");
        
        // Load (composite)
        var load = builder.AddComposite<TransformedData, LoadResult, LoadPipeline>("load");
        
        var sink = builder.AddSink<ResultSink, LoadResult>("sink");
        
        builder.Connect(source, extract);
        builder.Connect(extract, transform);
        builder.Connect(transform, load);
        builder.Connect(load, sink);
    }
}
```

## Monitoring and Observability

### Track Composite Node Performance

```csharp
public class MonitoredCompositeTransform<TIn, TOut, TDefinition> : CompositeTransformNode<TIn, TOut, TDefinition>
    where TDefinition : IPipelineDefinition, new()
{
    private readonly IMetrics _metrics;
    
    public override async Task<TOut> ExecuteAsync(TIn item, PipelineContext context, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        
        try
        {
            var result = await base.ExecuteAsync(item, context, ct);
            _metrics.RecordSuccess(typeof(TDefinition).Name, sw.Elapsed);
            return result;
        }
        catch (Exception ex)
        {
            _metrics.RecordFailure(typeof(TDefinition).Name, sw.Elapsed);
            throw;
        }
    }
}
```

### Metrics to Track

- **Execution Time**: Per composite node and sub-pipeline
- **Throughput**: Items/sec through composite nodes
- **Error Rate**: Failures in sub-pipelines
- **Memory Usage**: Context size and allocation rate
- **Nesting Depth**: Current and maximum depth

## Summary

### Key Takeaways

1. **Overhead is Minimal**: ~2-3μs per item per nesting level
2. **Context Copies Matter**: Large inherited contexts can slow things down
3. **Flat is Fastest**: But composition brings other benefits
4. **Profile First**: Measure before optimizing
5. **Context Matters**: Overhead impact depends on total processing time

### Decision Matrix

| Throughput | Processing Time | Recommendation |
|------------|----------------|----------------|
| < 10K items/sec | Any | ✅ Use composition freely |
| 10K-100K items/sec | > 1ms/item | ✅ Use composition |
| 10K-100K items/sec | < 100μs/item | ⚠️ Measure overhead |
| > 100K items/sec | < 10μs/item | ❌ Avoid composition |
| > 100K items/sec | > 100μs/item | ✅ Use composition |

### Optimization Priority

1. **Correctness** - Get it working correctly first
2. **Modularity** - Make it maintainable and testable
3. **Performance** - Optimize only if measurements show it's needed

> "Premature optimization is the root of all evil" - Donald Knuth

Focus on clean, modular code with composition. Optimize only when profiling shows composition overhead is a bottleneck in your specific use case.
