using System.Diagnostics;
using System.Text;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_PerformanceOptimization.Nodes;

/// <summary>
///     Baseline transform node using traditional Task-based approach.
///     This node serves as the baseline for performance comparison with optimized approaches.
///     It uses standard Task-based async patterns without any special optimizations.
/// </summary>
public class TaskBasedTransform : TransformNode<PerformanceDataItem, ProcessedPerformanceItem>
{
    private readonly Dictionary<int, string> _cache = new();
    private long _totalOperations;

    /// <summary>
    ///     Gets the total number of operations processed.
    /// </summary>
    public long TotalOperations => _totalOperations;

    /// <summary>
    ///     Processes the performance data item using standard Task-based approach.
    ///     This represents the baseline implementation without optimizations.
    /// </summary>
    public override async Task<ProcessedPerformanceItem> ExecuteAsync(PerformanceDataItem item, PipelineContext context, CancellationToken cancellationToken)
    {
        var memoryBefore = GC.GetTotalMemory(false);
        var stopwatch = Stopwatch.StartNew();

        Interlocked.Increment(ref _totalOperations);

        try
        {
            // Standard async processing without optimizations
            var processedData = await ProcessDataStandardAsync(item, cancellationToken);

            stopwatch.Stop();
            var memoryAfter = GC.GetTotalMemory(false);

            return new ProcessedPerformanceItem
            {
                OriginalId = item.Id,
                ProcessedData = processedData,
                ProcessingTime = stopwatch.Elapsed,
                UsedSynchronousPath = false, // Always async in baseline
                UsedValueTask = false, // Uses Task, not ValueTask
                ProcessingComplexity = item.ProcessingComplexity,
                MemoryAllocatedBytes = memoryAfter - memoryBefore,
            };
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            Console.WriteLine($"Error in TaskBasedTransform: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    ///     Standard data processing without optimizations.
    ///     This represents typical async code patterns that might be found in production.
    /// </summary>
    private async Task<string> ProcessDataStandardAsync(PerformanceDataItem item, CancellationToken cancellationToken)
    {
        // Check cache (simple implementation without optimization considerations)
        if (_cache.TryGetValue(item.Id, out var cached))
            return $"Cached_Task_{cached}";

        // Simulate processing delay for all operations (no sync fast path)
        await Task.Delay(item.ProcessingComplexity * 5, cancellationToken);

        // Standard string processing with multiple allocations
        var processed = item.Data.ToUpperInvariant();
        processed = Convert.ToBase64String(Encoding.UTF8.GetBytes(processed));

        // Additional processing based on complexity
        for (var i = 0; i < item.ProcessingComplexity; i++)
        {
            // Each iteration creates new string allocations
            processed = Convert.ToHexString(Encoding.UTF8.GetBytes(processed));
            processed = processed.ToLowerInvariant();
            processed = Convert.ToBase64String(Encoding.UTF8.GetBytes(processed));
        }

        // Cache the result (simple caching without memory optimization)
        var result = $"Task_{item.Id}_{processed[..Math.Min(50, processed.Length)]}";
        _cache[item.Id] = result;

        return result;
    }

    /// <summary>
    ///     Simulates I/O-bound operations that would typically require async.
    /// </summary>
    private async Task SimulateIOOperationAsync(int complexity, CancellationToken cancellationToken)
    {
        // Simulate database or network call
        await Task.Delay(complexity * 10, cancellationToken);

        // Simulate some CPU work after I/O
        var data = new byte[complexity * 100];

        for (var i = 0; i < data.Length; i++)
        {
            data[i] = (byte)(i % 256);
        }

        // Force some memory allocations
        var result = Convert.ToBase64String(data);
        _ = result.Length; // Use the result to avoid optimization
    }

    /// <summary>
    ///     Resets the operation counter for benchmarking.
    /// </summary>
    public void ResetCounter()
    {
        Interlocked.Exchange(ref _totalOperations, 0);
        _cache.Clear();
    }

    /// <summary>
    ///     Gets baseline performance statistics.
    /// </summary>
    public string GetBaselineStats()
    {
        return $"Task-based Baseline: {_totalOperations:N0} operations processed, {_cache.Count:N0} items cached";
    }
}
