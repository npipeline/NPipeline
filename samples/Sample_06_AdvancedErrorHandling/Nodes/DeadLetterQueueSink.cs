using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_06_AdvancedErrorHandling.Nodes;

/// <summary>
///     Represents a failed item with metadata for dead letter queue processing.
/// </summary>
/// <param name="OriginalItem">The original item that failed processing.</param>
/// <param name="Exception">The exception that caused the failure.</param>
/// <param name="FailureTime">The timestamp when the failure occurred.</param>
/// <param name="ProcessingStage">The stage where the failure occurred.</param>
/// <param name="RetryCount">The number of retry attempts made.</param>
public record DeadLetterItem(
    SourceData OriginalItem,
    Exception Exception,
    DateTime FailureTime,
    string ProcessingStage,
    int RetryCount = 0
);

/// <summary>
///     Sink node that captures failed items for later processing in a dead letter queue.
///     This node demonstrates how to implement a dead letter queue pattern for handling
///     items that cannot be processed successfully after retries.
/// </summary>
public class DeadLetterQueueSink : SinkNode<SourceData>
{
    private readonly List<DeadLetterItem> _deadLetterQueue = new();
    private readonly List<SourceData> _successfullyProcessed = new();

    /// <summary>
    ///     Processes the input strings, capturing failed items in the dead letter queue.
    /// </summary>
    /// <param name="input">The data pipe containing input strings to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task representing the sink execution.</returns>
    public override async Task ExecuteAsync(IDataPipe<SourceData> input, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Starting Dead Letter Queue Sink processing...");

        var processedCount = 0;
        var failedCount = 0;

        // Use await foreach to consume all messages from the input pipe
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                // Simulate final processing with potential failures
                await ProcessItemWithDeadLetterHandling(item, cancellationToken);

                _successfullyProcessed.Add(item);
                processedCount++;
                Console.WriteLine($"[DEAD LETTER] Successfully processed item: {item.Id}");
            }
            catch (Exception ex)
            {
                failedCount++;

                var deadLetterItem = new DeadLetterItem(
                    item,
                    ex,
                    DateTime.UtcNow,
                    "DeadLetterQueueSink" // This would be tracked in a real implementation
                );

                _deadLetterQueue.Add(deadLetterItem);
                Console.WriteLine($"[DEAD LETTER] Added to dead letter queue: {item.Id} - {ex.Message}");

                // Don't re-throw exceptions here - continue processing other items
                // This is a dead letter queue pattern where we capture failures and continue
            }
        }

        // Output processing summary
        Console.WriteLine();
        Console.WriteLine("=== DEAD LETTER QUEUE SUMMARY ===");
        Console.WriteLine($"Successfully processed: {processedCount}");
        Console.WriteLine($"Failed items: {failedCount}");
        Console.WriteLine($"Dead letter queue size: {_deadLetterQueue.Count}");

        if (_deadLetterQueue.Count > 0)
        {
            Console.WriteLine();
            Console.WriteLine("Dead Letter Items:");

            foreach (var deadItem in _deadLetterQueue.TakeLast(5)) // Show last 5 for brevity
            {
                Console.WriteLine($"  - ID: {deadItem.OriginalItem.Id}");
                Console.WriteLine($"    Error: {deadItem.Exception.Message}");
                Console.WriteLine($"    Time: {deadItem.FailureTime:yyyy-MM-dd HH:mm:ss}");
                Console.WriteLine($"    Stage: {deadItem.ProcessingStage}");
            }

            if (_deadLetterQueue.Count > 5)
                Console.WriteLine($"  ... and {_deadLetterQueue.Count - 5} more items");
        }

        Console.WriteLine("===================================");

        // Demonstrate dead letter queue processing options
        await DemonstrateDeadLetterQueueOptions();
    }

    /// <summary>
    ///     Simulates processing with dead letter queue handling.
    /// </summary>
    /// <param name="item">The item to process.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    private async Task ProcessItemWithDeadLetterHandling(SourceData item, CancellationToken cancellationToken)
    {
        // Simulate processing delay
        await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken);

        // Simulate final processing failures (10% chance)
        var random = new Random(item.Id.GetHashCode());

        if (random.NextDouble() < 0.1)
            throw new InvalidOperationException($"Final processing failed for item {item.Id}");

        // Process the item successfully
        Console.WriteLine($"[DEAD LETTER] Final processing completed for: {item.Id}");
    }

    /// <summary>
    ///     Demonstrates options for handling dead letter queue items.
    /// </summary>
    private async Task DemonstrateDeadLetterQueueOptions()
    {
        if (_deadLetterQueue.Count == 0)
        {
            Console.WriteLine("No items in dead letter queue - all processing successful!");
            return;
        }

        Console.WriteLine();
        Console.WriteLine("=== DEAD LETTER QUEUE PROCESSING OPTIONS ===");
        Console.WriteLine();

        Console.WriteLine("In a production environment, you could:");
        Console.WriteLine("1. Retry dead letter items with exponential backoff");
        Console.WriteLine("2. Send dead letter items to a separate processing pipeline");
        Console.WriteLine("3. Store dead letter items to a database or file system");
        Console.WriteLine("4. Send notifications for manual review");
        Console.WriteLine("5. Implement custom recovery logic based on error types");
        Console.WriteLine();

        Console.WriteLine("Example retry logic (simulated):");

        // Simulate retrying some dead letter items
        var retryableItems = _deadLetterQueue
            .Where(item => item.Exception is InvalidOperationException)
            .Take(2) // Retry first 2 for demonstration
            .ToList();

        if (retryableItems.Count > 0)
        {
            Console.WriteLine($"Retrying {retryableItems.Count} items from dead letter queue...");

            foreach (var item in retryableItems)
            {
                Console.WriteLine($"  Retrying item: {item.OriginalItem.Id}");

                // In a real implementation, you would retry the processing here
                // For demo purposes, we just simulate the retry
                await Task.Delay(TimeSpan.FromMilliseconds(100));
                Console.WriteLine($"  Retry completed for: {item.OriginalItem.Id}");
            }
        }

        Console.WriteLine("===============================================");
    }

    /// <summary>
    ///     Gets the current dead letter queue items for external processing.
    /// </summary>
    /// <returns>A read-only list of dead letter items.</returns>
    public IReadOnlyList<DeadLetterItem> GetDeadLetterItems()
    {
        return _deadLetterQueue.AsReadOnly();
    }

    /// <summary>
    ///     Gets the count of successfully processed items.
    /// </summary>
    /// <returns>The number of successfully processed items.</returns>
    public int GetSuccessfullyProcessedCount()
    {
        return _successfullyProcessed.Count;
    }
}
