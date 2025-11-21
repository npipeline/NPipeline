using System.Diagnostics;
using System.Text;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_09_PerformanceOptimization.Nodes;

/// <summary>
///     Transform node that demonstrates synchronous fast path optimization.
///     This node shows how to optimize performance by using synchronous paths for simple operations
///     and avoiding unnecessary async overhead.
/// </summary>
public class SynchronousFastPathTransform : TransformNode<PerformanceDataItem, ProcessedPerformanceItem>
{
    private readonly Dictionary<int, string> _cache = new();
    private long _asynchronousOperations;
    private long _synchronousOperations;

    /// <summary>
    ///     Gets the count of synchronous operations performed.
    /// </summary>
    public long SynchronousOperations => _synchronousOperations;

    /// <summary>
    ///     Gets the count of asynchronous operations performed.
    /// </summary>
    public long AsynchronousOperations => _asynchronousOperations;

    /// <summary>
    ///     Gets the percentage of operations that used the synchronous fast path.
    /// </summary>
    public double SynchronousPathPercentage
    {
        get
        {
            var total = _synchronousOperations + _asynchronousOperations;

            return total > 0
                ? (double)_synchronousOperations / total * 100
                : 0;
        }
    }

    /// <summary>
    ///     Processes the performance data item using optimal sync/async paths based on complexity.
    /// </summary>
    public override async Task<ProcessedPerformanceItem> ExecuteAsync(PerformanceDataItem item, PipelineContext context, CancellationToken cancellationToken)
    {
        var memoryBefore = GC.GetTotalMemory(false);
        var stopwatch = Stopwatch.StartNew();

        try
        {
            var processedData = await ProcessWithOptimalPathAsync(item, cancellationToken);

            stopwatch.Stop();
            var memoryAfter = GC.GetTotalMemory(false);

            return new ProcessedPerformanceItem
            {
                OriginalId = item.Id,
                ProcessedData = processedData,
                ProcessingTime = stopwatch.Elapsed,
                UsedSynchronousPath = item.ProcessingComplexity <= 3,
                UsedValueTask = false, // This transform focuses on sync/async paths
                ProcessingComplexity = item.ProcessingComplexity,
                MemoryAllocatedBytes = memoryAfter - memoryBefore,
            };
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            Console.WriteLine($"Error in SynchronousFastPathTransform: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    ///     Chooses the optimal processing path based on item complexity.
    /// </summary>
    private async Task<string> ProcessWithOptimalPathAsync(PerformanceDataItem item, CancellationToken cancellationToken)
    {
        if (item.ProcessingComplexity <= 3)
        {
            // Simple operations use synchronous path
            Interlocked.Increment(ref _synchronousOperations);
            return ProcessSynchronously(item);
        }

        // Complex operations use asynchronous path
        Interlocked.Increment(ref _asynchronousOperations);
        return await ProcessAsynchronously(item, cancellationToken);
    }

    /// <summary>
    ///     Synchronous processing for simple operations - avoids async overhead completely.
    /// </summary>
    private string ProcessSynchronously(PerformanceDataItem item)
    {
        // Check cache first (synchronous)
        if (_cache.TryGetValue(item.Id, out var cached))
            return $"Cached_Sync_{cached}";

        // Simple CPU-bound operations that don't need async
        var processed = item.Data.ToUpperInvariant();

        // Simple mathematical operations
        var hash = ComputeSimpleHash(processed);
        var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(processed));

        // Cache the result
        var result = $"Sync_{item.Id}_{hash}_{encoded[..Math.Min(20, encoded.Length)]}";
        _cache[item.Id] = result;

        return result;
    }

    /// <summary>
    ///     Asynchronous processing for complex operations.
    /// </summary>
    private async Task<string> ProcessAsynchronously(PerformanceDataItem item, CancellationToken cancellationToken)
    {
        // Simulate I/O-bound operations
        if (item.ProcessingComplexity > 7)
            await Task.Delay(item.ProcessingComplexity * 5, cancellationToken);

        // Complex CPU-bound operations
        var processed = item.Data.ToUpperInvariant();

        // More complex mathematical operations
        var hash = ComputeComplexHash(processed, item.ProcessingComplexity);
        var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(processed));

        // Simulate additional processing
        for (var i = 0; i < item.ProcessingComplexity; i++)
        {
            encoded = Convert.ToHexString(Encoding.UTF8.GetBytes(encoded));
        }

        return $"Async_{item.Id}_{hash}_{encoded[..Math.Min(30, encoded.Length)]}";
    }

    /// <summary>
    ///     Simple hash computation for synchronous fast path.
    /// </summary>
    private static int ComputeSimpleHash(string input)
    {
        var hash = 0;

        foreach (var c in input)
        {
            hash = (hash * 31 + c) % 1000000;
        }

        return hash;
    }

    /// <summary>
    ///     Complex hash computation for asynchronous path.
    /// </summary>
    private static string ComputeComplexHash(string input, int complexity)
    {
        // Use more complex hashing for async path
        var bytes = Encoding.UTF8.GetBytes(input);
        var hashBytes = new byte[complexity * 4];

        for (var i = 0; i < complexity; i++)
        {
            var hash = BitConverter.GetBytes(BitConverter.ToInt32(bytes, i % (bytes.Length - 3)) * (i + 1));
            Array.Copy(hash, 0, hashBytes, i * 4, Math.Min(4, hashBytes.Length - i * 4));
        }

        return Convert.ToHexString(hashBytes);
    }

    /// <summary>
    ///     Complex processing that requires async operations.
    /// </summary>
    private async Task<ProcessedPerformanceItem> ExecuteComplexProcessingAsync(PerformanceDataItem item, Stopwatch stopwatch, long memoryBefore,
        CancellationToken cancellationToken)
    {
        var processedData = await ProcessAsynchronously(item, cancellationToken);

        stopwatch.Stop();
        var memoryAfter = GC.GetTotalMemory(false);
        Interlocked.Increment(ref _asynchronousOperations);

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
    ///     Resets the operation counters for benchmarking.
    /// </summary>
    public void ResetCounters()
    {
        Interlocked.Exchange(ref _synchronousOperations, 0);
        Interlocked.Exchange(ref _asynchronousOperations, 0);
        _cache.Clear();
    }
}
