using NPipeline;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using System.Collections.Concurrent;
using Shared;

namespace Sample_02_HighPerformanceTransform;

/// <summary>
/// This sample demonstrates performance-optimized transformer nodes for high-throughput ETL pipelines.
/// 
/// Key patterns shown:
/// 1. Using ValueTask&lt;T&gt; instead of Task&lt;T&gt; to eliminate heap allocations for synchronous fast paths
/// 2. Implementing cache-based transforms with minimal GC pressure
/// 3. Balancing between synchronous caching and asynchronous fallback paths
/// 
/// In pipelines processing millions of items per second, these patterns can significantly
/// reduce GC pressure and improve overall throughput.
/// </summary>
public class Program
{
    public static async Task Main()
    {
        var runner = new PipelineRunner();
        await runner.RunAsync<HighPerformancePipeline>();

        Console.WriteLine();
        Console.WriteLine("HighPerformanceTransform pipeline completed successfully");
    }
}

public class HighPerformancePipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<DataSource, int>();
        
        // Use the performance-optimized transformer
        var transform = builder.AddTransform<CachedLookupTransform, int, string>();
        
        var sink = builder.AddSink<OutputSink, string>();

        builder.Connect(source, transform);
        builder.Connect(transform, sink);
    }
}

/// <summary>
/// Simple data source that generates a sequence of numbers.
/// </summary>
public class DataSource : SourceNode<int>
{
    private readonly int _count;

    public DataSource(int count = 1000)
    {
        _count = count;
    }

    public override Task<IDataPipe<int>> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        // Generate numbers with some repetition to simulate cache hits
        var data = Enumerable.Range(0, _count)
            .Select(i => i % 10) // Create repetition to trigger cache hits
            .ToList();

        return Task.FromResult<IDataPipe<int>>(
            new StreamingDataPipe<int>(data.ToAsyncEnumerable(), "Number Stream")
        );
    }
}

/// <summary>
/// High-performance transformer demonstrating the ValueTask pattern.
/// 
/// This transform uses a cache for lookups. When the value is in cache (common case),
/// it returns immediately via ValueTask without any heap allocation.
/// Only on cache miss does it perform async work.
/// </summary>
public class CachedLookupTransform : TransformNode<int, string>
{
    private readonly ConcurrentDictionary<int, string> _cache = new();
    private int _cacheHits = 0;
    private int _cacheMisses = 0;

    /// <summary>
    /// Using ValueTask&lt;T&gt; instead of Task&lt;T&gt; provides significant performance benefits:
    /// 
    /// - Synchronous fast path (cache hit): No heap allocation, just return the cached value wrapped in a ValueTask struct
    /// - Asynchronous slow path (cache miss): Fall back to async lookup and allocation
    /// 
    /// In high-volume scenarios, cache hits dominate, so avoiding allocations for the fast path
    /// dramatically reduces GC pressure and improves throughput.
    /// </summary>
    public override ValueTask<string> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
    {
        // Fast path: Check cache first
        if (_cache.TryGetValue(item, out var cachedValue))
        {
            Interlocked.Increment(ref _cacheHits);
            // Return immediately with no Task allocation - just the struct wrapper
            return new ValueTask<string>(cachedValue);
        }

        // Slow path: Async lookup and cache
        Interlocked.Increment(ref _cacheMisses);
        return new ValueTask<string>(LookupAndCacheAsync(item, cancellationToken));
    }

    private async Task<string> LookupAndCacheAsync(int id, CancellationToken cancellationToken)
    {
        // Simulate an async lookup (e.g., database query)
        await Task.Delay(1, cancellationToken);

        var result = $"Item_{id:D3}";
        _cache.TryAdd(id, result);
        return result;
    }

    public override async Task DisposeAsync()
    {
        // Log cache statistics
        Console.WriteLine($"\nCache Statistics:");
        Console.WriteLine($"  Hits:   {_cacheHits}");
        Console.WriteLine($"  Misses: {_cacheMisses}");
        Console.WriteLine($"  Hit Rate: {(_cacheHits * 100.0 / (_cacheHits + _cacheMisses)):F1}%");

        await base.DisposeAsync();
    }
}

/// <summary>
/// Output sink that collects and displays results.
/// </summary>
public class OutputSink : SinkNode<string>
{
    public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context,
        CancellationToken cancellationToken)
    {
        Console.WriteLine("OUTPUT (first 20 items)");
        Console.WriteLine("=======================");

        int count = 0;
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            if (count < 20)
            {
                Console.WriteLine($"{count:D4}: {item}");
            }
            count++;
        }

        Console.WriteLine($"... and {Math.Max(0, count - 20)} more items");
        Console.WriteLine("=======================");
        Console.WriteLine($"Total items processed: {count}");
    }
}
