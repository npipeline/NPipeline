using NPipeline.Nodes;
using NPipeline.Pipeline;
using Polly;
using Polly.Retry;

namespace Sample_AdvancedErrorHandling.Nodes;

/// <summary>
///     Transform node that implements advanced retry strategies using Polly.
///     This node demonstrates how to handle transient failures with exponential backoff.
/// </summary>
public class RetryTransform : TransformNode<SourceData, SourceData>
{
    private readonly AsyncRetryPolicy<SourceData> _retryPolicy;

    /// <summary>
    ///     Initializes a new instance of the RetryTransform with configured retry policies.
    /// </summary>
    public RetryTransform()
    {
        // Configure advanced retry policy with exponential backoff
        _retryPolicy = Policy<SourceData>
            .Handle<InvalidOperationException>() // Retry on specific exceptions
            .Or<TimeoutException>() // Also retry on timeouts
            .Or<HttpRequestException>() // Retry on HTTP-related errors
            .WaitAndRetryAsync(
                3,
                retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), // Exponential backoff: 2s, 4s, 8s
                (outcome, timespan, retryAttempt, context) =>
                {
                    Console.WriteLine(
                        $"[RETRY] Attempt {retryAttempt} after {timespan.TotalSeconds}s delay due to: {outcome.Exception?.Message ?? "Unknown error"}");
                }
            );
    }

    /// <summary>
    ///     Processes the input data with retry capabilities.
    /// </summary>
    /// <param name="item">The input SourceData to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the processed SourceData.</returns>
    public override async Task<SourceData> ExecuteAsync(SourceData item, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"[RETRY] Processing item: {item.Id}");

        try
        {
            // Execute the processing with retry policy
            var result = await _retryPolicy.ExecuteAsync(async () => { return await ProcessWithPotentialFailure(item, cancellationToken); });

            Console.WriteLine($"[RETRY] Successfully processed item: {item.Id}");
            return result;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[RETRY] Failed to process item {item.Id} after all retries: {ex.Message}");

            // Don't re-throw here - let the item pass through to demonstrate other features
            // In a real scenario, you might want to handle this differently
            return item with { Content = $"{item.Content} (retry-failed-but-continued)" };
        }
    }

    /// <summary>
    ///     Simulates processing with potential failures for demonstration purposes.
    /// </summary>
    /// <param name="item">The item to process.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The processed item.</returns>
    private async Task<SourceData> ProcessWithPotentialFailure(SourceData item, CancellationToken cancellationToken)
    {
        // Simulate processing delay
        await Task.Delay(TimeSpan.FromMilliseconds(100), cancellationToken);

        // Simulate occasional failures (5% chance - much lower)
        var random = new Random(item.Id.GetHashCode()); // Deterministic based on item ID

        if (random.NextDouble() < 0.05)
            throw new InvalidOperationException($"Simulated processing failure for item {item.Id}");

        // Process the item (in this case, just return it with updated content)
        return item with { Content = $"{item.Content} (retry-processed)" };
    }
}
