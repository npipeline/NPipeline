using System.Diagnostics;
using System.Text;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_09_PerformanceOptimization.Nodes;

/// <summary>
///     Transform node that demonstrates ValueTask vs Task performance comparison.
///     This node shows how ValueTask can reduce allocations for operations that complete synchronously.
/// </summary>
public class ValueTaskComparisonTransform : TransformNode<PerformanceDataItem, ProcessedPerformanceItem>
{
    private readonly bool _useValueTask;
    private long _totalAllocations;

    public ValueTaskComparisonTransform(bool useValueTask = true)
    {
        _useValueTask = useValueTask;
    }

    /// <summary>
    ///     Gets the total memory allocations tracked by this node.
    /// </summary>
    public long TotalAllocations => _totalAllocations;

    /// <summary>
    ///     Processes the performance data item using either Task or ValueTask based on configuration.
    /// </summary>
    public override async Task<ProcessedPerformanceItem> ExecuteAsync(PerformanceDataItem item, PipelineContext context, CancellationToken cancellationToken)
    {
        var memoryBefore = GC.GetTotalMemory(false);
        var stopwatch = Stopwatch.StartNew();

        try
        {
            // Simulate processing based on complexity
            var processedData = await ProcessDataAsync(item, cancellationToken);

            stopwatch.Stop();
            var memoryAfter = GC.GetTotalMemory(false);
            Interlocked.Add(ref _totalAllocations, memoryAfter - memoryBefore);

            return new ProcessedPerformanceItem
            {
                OriginalId = item.Id,
                ProcessedData = processedData,
                ProcessingTime = stopwatch.Elapsed,
                UsedSynchronousPath = false, // This is the async path
                UsedValueTask = _useValueTask,
                ProcessingComplexity = item.ProcessingComplexity,
                MemoryAllocatedBytes = memoryAfter - memoryBefore,
            };
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            Console.WriteLine($"Error in ValueTaskComparisonTransform: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    ///     Simulates data processing with complexity-based delay.
    /// </summary>
    private async Task<string> ProcessDataAsync(PerformanceDataItem item, CancellationToken cancellationToken)
    {
        // Simulate processing time based on complexity
        if (item.ProcessingComplexity > 5)
            await Task.Delay(item.ProcessingComplexity * 10, cancellationToken);

        // Simulate some CPU work
        var result = item.Data.ToUpperInvariant();

        for (var i = 0; i < item.ProcessingComplexity; i++)
        {
            result = Convert.ToBase64String(Encoding.UTF8.GetBytes(result));
        }

        return $"Processed_{item.Id}_{result[..Math.Min(50, result.Length)]}";
    }

    /// <summary>
    ///     Synchronous processing for simple operations.
    /// </summary>
    private string ProcessDataSynchronously(PerformanceDataItem item)
    {
        // Simple synchronous processing
        var result = item.Data.ToUpperInvariant();

        for (var i = 0; i < item.ProcessingComplexity; i++)
        {
            result = Convert.ToBase64String(Encoding.UTF8.GetBytes(result));
        }

        return $"SyncProcessed_{item.Id}_{result[..Math.Min(50, result.Length)]}";
    }

    /// <summary>
    ///     Complex processing that requires async operations.
    /// </summary>
    private async Task<ProcessedPerformanceItem> ExecuteComplexProcessingAsync(PerformanceDataItem item, Stopwatch stopwatch, long memoryBefore,
        CancellationToken cancellationToken)
    {
        var processedData = await ProcessDataAsync(item, cancellationToken);

        stopwatch.Stop();
        var memoryAfter = GC.GetTotalMemory(false);
        Interlocked.Add(ref _totalAllocations, memoryAfter - memoryBefore);

        return new ProcessedPerformanceItem
        {
            OriginalId = item.Id,
            ProcessedData = processedData,
            ProcessingTime = stopwatch.Elapsed,
            UsedSynchronousPath = false,
            UsedValueTask = true,
            ProcessingComplexity = item.ProcessingComplexity,
            MemoryAllocatedBytes = memoryAfter - memoryBefore,
        };
    }

    /// <summary>
    ///     Resets the allocation counter for benchmarking.
    /// </summary>
    public void ResetAllocationCounter()
    {
        Interlocked.Exchange(ref _totalAllocations, 0);
    }
}
