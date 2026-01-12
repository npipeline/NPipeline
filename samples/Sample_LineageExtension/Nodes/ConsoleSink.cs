using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_LineageExtension.Nodes;

/// <summary>
///     Sink node that writes processed orders to the console.
///     Demonstrates how lineage information can be displayed alongside data.
/// </summary>
public class ConsoleSink : SinkNode<ProcessedOrder>
{
    private readonly string _name;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ConsoleSink" /> class.
    /// </summary>
    /// <param name="name">Optional name for this sink instance.</param>
    public ConsoleSink(string name = "ConsoleSink")
    {
        _name = name;
    }

    /// <summary>
    ///     Writes processed orders to the console.
    /// </summary>
    /// <param name="input">The input data pipe containing processed orders.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    public override async Task ExecuteAsync(IDataPipe<ProcessedOrder> input, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"[{_name}] Starting to write processed orders...");

        var count = 0;
        var successCount = 0;
        var failedCount = 0;
        var rejectedCount = 0;
        var manualReviewCount = 0;
        var deadLetterCount = 0;

        await foreach (var processedOrder in input.WithCancellation(cancellationToken))
        {
            count++;

            // Update statistics
            switch (processedOrder.Result)
            {
                case ProcessingResult.Success:
                    successCount++;
                    break;
                case ProcessingResult.Failed:
                    failedCount++;
                    break;
                case ProcessingResult.Rejected:
                    rejectedCount++;
                    break;
                case ProcessingResult.ManualReview:
                    manualReviewCount++;
                    break;
                case ProcessingResult.DeadLetter:
                    deadLetterCount++;
                    break;
            }

            // Display order information
            var order = processedOrder.ValidatedOrder.EnrichedOrder.Order;
            var customer = processedOrder.ValidatedOrder.EnrichedOrder.Customer;

            Console.WriteLine($"[{_name}] Order #{order.OrderId}: {customer.FullName} - {order.TotalAmount:C} -> {processedOrder.Result}");

            // Display validation errors if any
            if (!processedOrder.ValidatedOrder.IsValid && processedOrder.ValidatedOrder.ValidationErrors.Count > 0)
            {
                Console.WriteLine($"  Validation Errors: {string.Join(", ", processedOrder.ValidatedOrder.ValidationErrors)}");
            }

            // Display notes if any
            if (!string.IsNullOrWhiteSpace(processedOrder.Notes))
            {
                Console.WriteLine($"  Notes: {processedOrder.Notes}");
            }
        }

        Console.WriteLine($"[{_name}] Finished writing {count} processed orders:");
        Console.WriteLine($"  Success: {successCount}");
        Console.WriteLine($"  Failed: {failedCount}");
        Console.WriteLine($"  Rejected: {rejectedCount}");
        Console.WriteLine($"  Manual Review: {manualReviewCount}");
        Console.WriteLine($"  Dead Letter: {deadLetterCount}");
    }
}