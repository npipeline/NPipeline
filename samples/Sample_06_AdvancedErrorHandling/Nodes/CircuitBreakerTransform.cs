using NPipeline.Nodes;
using NPipeline.Pipeline;
using Polly;
using Polly.CircuitBreaker;

namespace Sample_06_AdvancedErrorHandling.Nodes;

/// <summary>
///     Transform node that implements circuit breaker patterns using Polly.
///     This node demonstrates how to prevent cascading failures by temporarily stopping
///     processing when failure thresholds are exceeded.
/// </summary>
public class CircuitBreakerTransform : TransformNode<SourceData, SourceData>
{
    private readonly AsyncCircuitBreakerPolicy _circuitBreakerPolicy;
    private int _failureCount;
    private int _successCount;

    /// <summary>
    ///     Initializes a new instance of CircuitBreakerTransform with configured circuit breaker policies.
    /// </summary>
    public CircuitBreakerTransform()
    {
        // Configure circuit breaker policy
        _circuitBreakerPolicy = Policy
            .Handle<InvalidOperationException>()
            .Or<TimeoutException>()
            .CircuitBreakerAsync(
                5, // Increased to 5 failures
                TimeSpan.FromSeconds(5), // Reduced to 5 seconds
                (exception, breakDelay) =>
                {
                    Console.WriteLine($"[CIRCUIT BREAKER] Circuit broken due to: {exception.Message}");
                    Console.WriteLine($"[CIRCUIT BREAKER] Circuit will remain open for {breakDelay.TotalSeconds} seconds");
                },
                () => { Console.WriteLine("[CIRCUIT BREAKER] Circuit reset - processing resumed"); },
                () => { Console.WriteLine("[CIRCUIT BREAKER] Circuit half-open - testing with single request"); }
            );
    }

    /// <summary>
    ///     Processes input data with circuit breaker protection.
    /// </summary>
    /// <param name="item">The input SourceData to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the processed SourceData.</returns>
    public override async Task<SourceData> ExecuteAsync(SourceData item, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"[CIRCUIT BREAKER] Processing item: {item.Id}");

        try
        {
            // Execute the processing with circuit breaker policy
            var result = await _circuitBreakerPolicy.ExecuteAsync(async () => { return await ProcessWithPotentialFailure(item, cancellationToken); });

            _successCount++;
            Console.WriteLine($"[CIRCUIT BREAKER] Successfully processed item: {item.Id} (Successes: {_successCount})");
            return result;
        }
        catch (BrokenCircuitException)
        {
            Console.WriteLine($"[CIRCUIT BREAKER] Circuit is OPEN - rejecting item: {item.Id}");

            // Don't re-throw - let the item pass through to demonstrate other features
            return item with { Content = $"{item.Content} (circuit-breaker-open)" };
        }
        catch (Exception ex)
        {
            _failureCount++;
            Console.WriteLine($"[CIRCUIT BREAKER] Failed to process item {item.Id}: {ex.Message} (Failures: {_failureCount})");

            // Don't re-throw - let the item pass through to demonstrate other features
            return item with { Content = $"{item.Content} (circuit-breaker-failed)" };
        }
    }

    /// <summary>
    ///     Simulates processing with potential failures for circuit breaker demonstration.
    /// </summary>
    /// <param name="item">The item to process.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The processed item.</returns>
    private async Task<SourceData> ProcessWithPotentialFailure(SourceData item, CancellationToken cancellationToken)
    {
        // Simulate processing delay
        await Task.Delay(TimeSpan.FromMilliseconds(150), cancellationToken);

        // Simulate failures based on item ID pattern (deterministic for demonstration)
        // Only items ending with 15, 20 will fail (reduced from every 5th item)
        var itemNumber = int.Parse(item.Id.Split('-')[1]);

        if (itemNumber % 10 == 5) // Items 5, 15, 25, etc.
            throw new InvalidOperationException($"Simulated circuit breaker trigger failure for item {item.Id}");

        // Process the item
        return item with { Content = $"{item.Content} (circuit-breaker-processed)" };
    }
}
