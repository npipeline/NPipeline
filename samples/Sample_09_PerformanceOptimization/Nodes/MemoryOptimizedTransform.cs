using System.Buffers;
using System.Diagnostics;
using System.Text;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_09_PerformanceOptimization.Nodes;

/// <summary>
///     Transform node that demonstrates memory allocation reduction techniques.
///     This node shows how to minimize memory allocations through pooling, span usage, and other optimizations.
/// </summary>
public class MemoryOptimizedTransform : TransformNode<PerformanceDataItem, ProcessedPerformanceItem>
{
    private readonly ArrayPool<byte> _bytePool = ArrayPool<byte>.Shared;
    private readonly ArrayPool<char> _charPool = ArrayPool<char>.Shared;
    private readonly Dictionary<int, string> _processingCache = new();
    private long _poolRentals;
    private long _totalAllocationsAvoided;

    /// <summary>
    ///     Gets the total number of memory allocations avoided through optimization.
    /// </summary>
    public long TotalAllocationsAvoided => _totalAllocationsAvoided;

    /// <summary>
    ///     Gets the total number of array pool rentals.
    /// </summary>
    public long PoolRentals => _poolRentals;

    /// <summary>
    ///     Processes the performance data item with memory optimization techniques.
    /// </summary>
    public override async Task<ProcessedPerformanceItem> ExecuteAsync(PerformanceDataItem item, PipelineContext context, CancellationToken cancellationToken)
    {
        var memoryBefore = GC.GetTotalMemory(false);
        var stopwatch = Stopwatch.StartNew();

        try
        {
            var processedData = await ProcessWithMemoryOptimizationAsync(item, cancellationToken);

            stopwatch.Stop();
            var memoryAfter = GC.GetTotalMemory(false);
            var memoryDelta = memoryAfter - memoryBefore;

            // Estimate allocations avoided (this is a rough calculation for demonstration)
            var estimatedAvoided = EstimateAllocationsAvoided(item);
            Interlocked.Add(ref _totalAllocationsAvoided, estimatedAvoided);

            return new ProcessedPerformanceItem
            {
                OriginalId = item.Id,
                ProcessedData = processedData,
                ProcessingTime = stopwatch.Elapsed,
                UsedSynchronousPath = item.ProcessingComplexity <= 2, // Very simple operations sync
                UsedValueTask = false, // This transform focuses on memory optimization
                ProcessingComplexity = item.ProcessingComplexity,
                MemoryAllocatedBytes = memoryDelta,
            };
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            Console.WriteLine($"Error in MemoryOptimizedTransform: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    ///     Processes data with memory optimization techniques.
    /// </summary>
    private async Task<string> ProcessWithMemoryOptimizationAsync(PerformanceDataItem item, CancellationToken cancellationToken)
    {
        // Check cache first to avoid reprocessing
        if (_processingCache.TryGetValue(item.Id, out var cached))
            return $"Cached_Optimized_{cached}";

        // Use array pooling for temporary buffers
        var buffer = _bytePool.Rent(1024);
        Interlocked.Increment(ref _poolRentals);

        try
        {
            // Use spans to avoid allocations
            var dataSpan = item.Data;

            // Process using span-based operations
            var processed = await ProcessWithSpanAsync(item.Data, buffer, item.ProcessingComplexity, cancellationToken);

            // Cache the result
            _processingCache[item.Id] = processed;

            return $"Optimized_{item.Id}_{processed}";
        }
        finally
        {
            _bytePool.Return(buffer);
        }
    }

    /// <summary>
    ///     Synchronous processing with memory optimization.
    /// </summary>
    private string ProcessSynchronouslyWithOptimization(PerformanceDataItem item)
    {
        if (_processingCache.TryGetValue(item.Id, out var cached))
            return $"Cached_SyncOptimized_{cached}";

        // Use stack allocation for small buffers
        Span<char> charBuffer = stackalloc char[256];

        // Process using stack-allocated buffer
        var data = item.Data;
        var processed = ProcessWithStackSpan(data, charBuffer);

        _processingCache[item.Id] = processed;

        return $"SyncOptimized_{item.Id}_{processed}";
    }

    /// <summary>
    ///     Processes data using span-based operations with async support.
    /// </summary>
    private async Task<string> ProcessWithSpanAsync(string data, byte[] buffer, int complexity, CancellationToken cancellationToken)
    {
        // Simulate async work for complex operations
        if (complexity > 5)
            await Task.Delay(complexity * 2, cancellationToken);

        // Convert string to bytes for processing
        var dataBytes = Encoding.UTF8.GetBytes(data);
        var byteCount = dataBytes.Length;

        if (byteCount > buffer.Length)
        {
            // Rent a larger buffer if needed
            var largerBuffer = _bytePool.Rent(byteCount);
            Interlocked.Increment(ref _poolRentals);

            try
            {
                Array.Copy(dataBytes, largerBuffer, byteCount);
                return ProcessBytesInPlace(largerBuffer, byteCount, complexity);
            }
            finally
            {
                _bytePool.Return(largerBuffer);
            }
        }

        Array.Copy(dataBytes, buffer, byteCount);
        return ProcessBytesInPlace(buffer, byteCount, complexity);
    }

    /// <summary>
    ///     Processes data using stack-allocated span for simple operations.
    /// </summary>
    private string ProcessWithStackSpan(string data, Span<char> buffer)
    {
        // Simple transformation using stack-allocated buffer
        var length = Math.Min(data.Length, buffer.Length);

        // Convert to uppercase in-place
        for (var i = 0; i < length; i++)
        {
            buffer[i] = char.ToUpperInvariant(data[i]);
        }

        // Create result from the span
        return new string(buffer[..length]);
    }

    /// <summary>
    ///     Processes bytes in-place to avoid additional allocations.
    /// </summary>
    private string ProcessBytesInPlace(byte[] buffer, int length, int complexity)
    {
        // Process bytes in-place
        for (var i = 0; i < length; i++)
        {
            buffer[i] = (byte)(buffer[i] ^ (complexity & 0xFF)); // Simple XOR transformation
        }

        // Convert back to string
        var result = Encoding.UTF8.GetString(buffer, 0, length);

        // Apply additional processing based on complexity
        for (var i = 0; i < complexity; i++)
        {
            result = Convert.ToBase64String(Encoding.UTF8.GetBytes(result));
        }

        return result[..Math.Min(50, result.Length)];
    }

    /// <summary>
    ///     Complex processing with memory optimization.
    /// </summary>
    private async Task<ProcessedPerformanceItem> ExecuteComplexMemoryOptimizedProcessingAsync(PerformanceDataItem item, Stopwatch stopwatch, long memoryBefore,
        CancellationToken cancellationToken)
    {
        var processedData = await ProcessWithMemoryOptimizationAsync(item, cancellationToken);

        stopwatch.Stop();
        var memoryAfter = GC.GetTotalMemory(false);
        var memoryDelta = memoryAfter - memoryBefore;

        var estimatedAvoided = EstimateAllocationsAvoided(item);
        Interlocked.Add(ref _totalAllocationsAvoided, estimatedAvoided);

        return new ProcessedPerformanceItem
        {
            OriginalId = item.Id,
            ProcessedData = processedData,
            ProcessingTime = stopwatch.Elapsed,
            UsedSynchronousPath = false,
            UsedValueTask = true,
            ProcessingComplexity = item.ProcessingComplexity,
            MemoryAllocatedBytes = memoryDelta,
        };
    }

    /// <summary>
    ///     Estimates the number of allocations avoided through optimization techniques.
    /// </summary>
    private static long EstimateAllocationsAvoided(PerformanceDataItem item)
    {
        // Rough estimation based on what would have been allocated without optimization
        var estimatedAllocations = 0L;

        // String allocations avoided through pooling
        estimatedAllocations += item.Data.Length * 2;

        // Array allocations avoided through pooling
        estimatedAllocations += 1024; // Estimated buffer size

        // Additional allocations based on complexity
        estimatedAllocations += item.ProcessingComplexity * 100;

        return estimatedAllocations;
    }

    /// <summary>
    ///     Resets the optimization counters for benchmarking.
    /// </summary>
    public void ResetCounters()
    {
        Interlocked.Exchange(ref _totalAllocationsAvoided, 0);
        Interlocked.Exchange(ref _poolRentals, 0);
        _processingCache.Clear();
    }

    /// <summary>
    ///     Gets memory optimization statistics.
    /// </summary>
    public string GetOptimizationStats()
    {
        return
            $"Memory Optimization Stats: {_totalAllocationsAvoided:N0} allocations avoided, {_poolRentals:N0} pool rentals, {_processingCache.Count:N0} cached items";
    }
}
