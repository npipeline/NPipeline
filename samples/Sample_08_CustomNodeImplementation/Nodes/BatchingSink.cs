using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_08_CustomNodeImplementation.Models;

namespace Sample_08_CustomNodeImplementation.Nodes;

/// <summary>
///     Custom sink node with batching capabilities for improved performance.
///     This node demonstrates how to implement a sink that batches output data
///     for better throughput and resource utilization.
/// </summary>
/// <remarks>
///     This implementation demonstrates:
///     - Custom sink node implementation with batching
///     - Performance optimization through batching
///     - Configurable batch sizes and timeouts
///     - Structured code for testability
/// </remarks>
public class BatchingSink : SinkNode<ProcessedSensorData>
{
    private readonly TimeSpan _batchTimeout = TimeSpan.FromSeconds(2);
    private readonly List<ProcessedSensorData> _currentBatch;
    private readonly int _maxBatchSize = 10;
    private int _batchCount;
    private readonly Timer? _batchTimer;
    private bool _disposed;
    private DateTime _lastBatchFlush = DateTime.UtcNow;
    private int _totalItemsProcessed;

    /// <summary>
    ///     Initializes a new instance of the BatchingSink class.
    /// </summary>
    public BatchingSink()
    {
        _currentBatch = new List<ProcessedSensorData>();
        Console.WriteLine("Initializing BatchingSink with performance optimization...");

        // Initialize batch timer to flush batches based on time
        _batchTimer = new Timer(FlushBatchByTimer, null, _batchTimeout, _batchTimeout);

        Console.WriteLine($"BatchingSink initialized - Max batch size: {_maxBatchSize}, Batch timeout: {_batchTimeout.TotalSeconds}s");
    }

    /// <summary>
    ///     Processes processed sensor data by batching it for improved performance.
    /// </summary>
    /// <param name="input">The data pipe containing processed sensor data to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task representing the sink execution.</returns>
    public override async Task ExecuteAsync(IDataPipe<ProcessedSensorData> input, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Starting to process data in BatchingSink...");

        // Process all items from the input pipe
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Add item to current batch
            _currentBatch.Add(item);
            _totalItemsProcessed++;

            // Flush batch if it reaches the maximum size
            if (_currentBatch.Count >= _maxBatchSize)
                await FlushBatch("size limit reached");

            // Log progress periodically
            if (_totalItemsProcessed % 25 == 0)
                Console.WriteLine($"BatchingSink: Processed {_totalItemsProcessed} items, created {_batchCount} batches");
        }

        // Flush any remaining items in the final batch
        if (_currentBatch.Count > 0)
            await FlushBatch("pipeline completion");

        Console.WriteLine($"BatchingSink: Completed processing - {_totalItemsProcessed} items in {_batchCount} batches");
    }

    /// <summary>
    ///     Flushes the current batch of data.
    /// </summary>
    /// <param name="reason">The reason for flushing the batch.</param>
    /// <returns>A Task representing the flush operation.</returns>
    private async Task FlushBatch(string reason)
    {
        if (_currentBatch.Count == 0)
            return;

        _batchCount++;
        var batchId = $"BATCH-{_batchCount:D4}";

        // Create a batch object
        var batch = new ProcessedDataBatch
        {
            BatchId = batchId,
            CreatedAt = DateTime.UtcNow,
            Data = _currentBatch.ToList(), // Create a copy of the current batch
            BatchMetadata = new Dictionary<string, object>
            {
                ["FlushReason"] = reason,
                ["ItemCount"] = _currentBatch.Count,
                ["ProcessingTimeMs"] = DateTime.UtcNow - _lastBatchFlush,
                ["AverageProcessingTime"] = _currentBatch.Average(item =>
                    item.ProcessingMetadata.TryGetValue("ProcessingTimeMs", out var time)
                        ? Convert.ToDouble(time)
                        : 0),
            },
        };

        // Process the batch (in a real scenario, this might be writing to a database, file, or API)
        await ProcessBatch(batch);

        // Clear the current batch and update the last flush time
        _currentBatch.Clear();
        _lastBatchFlush = DateTime.UtcNow;

        Console.WriteLine($"BatchingSink: Flushed batch {batchId} with {batch.Data.Count} items ({reason})");
    }

    /// <summary>
    ///     Processes a batch of data (simulates writing to external storage).
    /// </summary>
    /// <param name="batch">The batch to process.</param>
    /// <returns>A Task representing the batch processing operation.</returns>
    private async Task ProcessBatch(ProcessedDataBatch batch)
    {
        // Simulate batch processing time (e.g., writing to database, API call, etc.)
        await Task.Delay(20); // Simulate 20ms of I/O time

        // In a real implementation, this would write the batch to external storage
        // For this sample, we'll just log some statistics about the batch

        var successCount = batch.Data.Count(item => item.Status == ProcessingStatus.Success);
        var errorCount = batch.Data.Count - successCount;

        var averageValue = batch.Data.Average(item => item.ProcessedValue);
        var minValue = batch.Data.Min(item => item.ProcessedValue);
        var maxValue = batch.Data.Max(item => item.ProcessedValue);

        Console.WriteLine($"  Batch {batch.BatchId} Statistics:");
        Console.WriteLine($"    Items: {batch.Data.Count} (Success: {successCount}, Errors: {errorCount})");
        Console.WriteLine($"    Values: Avg={averageValue:F2}, Min={minValue:F2}, Max={maxValue:F2}");
        Console.WriteLine($"    Processing time: {batch.BatchMetadata["ProcessingTimeMs"]}");
    }

    /// <summary>
    ///     Timer callback to flush batches based on time.
    /// </summary>
    /// <param name="state">Timer state (unused).</param>
    private void FlushBatchByTimer(object? state)
    {
        if (_currentBatch.Count > 0)
        {
            // Note: In a real implementation, you'd want to handle this asynchronously
            // For this sample, we'll just note that a timeout flush would occur
            Console.WriteLine($"BatchingSink: Timer triggered - would flush batch with {_currentBatch.Count} items");
        }
    }

    /// <summary>
    ///     Gets batch statistics for monitoring.
    /// </summary>
    /// <returns>A dictionary containing batch statistics.</returns>
    public Dictionary<string, object> GetBatchStatistics()
    {
        return new Dictionary<string, object>
        {
            ["TotalItemsProcessed"] = _totalItemsProcessed,
            ["BatchCount"] = _batchCount,
            ["AverageBatchSize"] = _batchCount > 0
                ? (double)_totalItemsProcessed / _batchCount
                : 0,
            ["MaxBatchSize"] = _maxBatchSize,
            ["BatchTimeoutSeconds"] = _batchTimeout.TotalSeconds,
            ["CurrentBatchSize"] = _currentBatch.Count,
            ["LastBatchFlush"] = _lastBatchFlush,
        };
    }

    /// <summary>
    ///     Asynchronously disposes of resources used by the batching sink node.
    /// </summary>
    /// <returns>A ValueTask representing the asynchronous dispose operation.</returns>
    public override async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            Console.WriteLine("Disposing BatchingSink...");
            Console.WriteLine($"Final batch statistics: {string.Join(", ", GetBatchStatistics().Select(kvp => $"{kvp.Key}={kvp.Value}"))}");

            // Dispose the timer
            _batchTimer?.Dispose();

            // Clear the current batch
            _currentBatch.Clear();

            _disposed = true;
            GC.SuppressFinalize(this);
            await base.DisposeAsync();
        }
    }
}
