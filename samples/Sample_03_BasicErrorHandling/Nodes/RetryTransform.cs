using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_03_BasicErrorHandling.Nodes;

/// <summary>
///     Transform node that implements retry logic with exponential backoff.
///     This node demonstrates how to handle transient failures by retrying operations
///     with increasing delays between attempts.
/// </summary>
public class RetryTransform : TransformNode<string, string>
{
    private readonly Random _random = new();
    private double _backoffMultiplier = 2.0;
    private double _failureRate = 0.4; // 40% chance of failure by default
    private TimeSpan _initialDelay = TimeSpan.FromMilliseconds(100);
    private int _maxRetries = 3;

    /// <summary>
    ///     Initializes a new instance of the RetryTransform class.
    ///     Parameterless constructor for DI compatibility.
    /// </summary>
    public RetryTransform()
    {
    }

    /// <summary>
    ///     Processes the input string with retry logic and exponential backoff.
    /// </summary>
    /// <param name="item">The input string to transform.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the transformed string.</returns>
    public override async Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"RetryTransform: Processing item: {item}");

        var attempt = 0;
        var delay = _initialDelay;

        while (attempt <= _maxRetries)
        {
            attempt++;
            cancellationToken.ThrowIfCancellationRequested();

            try
            {
                // Simulate processing that might fail
                var result = await ProcessWithPotentialFailure(item, cancellationToken);

                if (attempt > 1)
                    Console.WriteLine($"RetryTransform: Success on attempt {attempt} for item: {item}");
                else
                    Console.WriteLine($"RetryTransform: Successfully processed item: {item}");

                return result;
            }
            catch (Exception ex) when (attempt <= _maxRetries)
            {
                Console.WriteLine($"RetryTransform: Attempt {attempt} failed for item '{item}': {ex.Message}");

                if (attempt <= _maxRetries)
                {
                    // Add jitter to prevent thundering herd
                    var jitter = _random.Next(0, (int)(delay.TotalMilliseconds * 0.1));
                    var totalDelay = delay + TimeSpan.FromMilliseconds(jitter);

                    Console.WriteLine($"RetryTransform: Retrying in {totalDelay.TotalMilliseconds}ms (attempt {attempt}/{_maxRetries})");
                    await Task.Delay(totalDelay, cancellationToken);

                    // Exponential backoff
                    delay = TimeSpan.FromMilliseconds(delay.TotalMilliseconds * _backoffMultiplier);
                }
            }
        }

        // All retries exhausted
        var finalException = new InvalidOperationException(
            $"Failed to process item '{item}' after {_maxRetries + 1} attempts");

        Console.WriteLine($"RetryTransform: All retries exhausted for item '{item}'");
        throw finalException;
    }

    /// <summary>
    ///     Simulates processing that might fail randomly.
    /// </summary>
    /// <param name="item">The item to process.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The processed item.</returns>
    /// <exception cref="InvalidOperationException">Thrown randomly to simulate processing failures.</exception>
    private async Task<string> ProcessWithPotentialFailure(string item, CancellationToken cancellationToken)
    {
        // Simulate some processing time
        await Task.Delay(10, cancellationToken);

        // Randomly decide if this processing should fail
        if (ShouldSimulateFailure())
            throw new InvalidOperationException($"Simulated processing failure for item: {item}");

        // Process the item (simple transformation for demonstration)
        return $"[RETRY_PROCESSED] {item}";
    }

    /// <summary>
    ///     Determines whether to simulate a failure based on the failure rate.
    /// </summary>
    /// <returns>True if a failure should be simulated, false otherwise.</returns>
    private bool ShouldSimulateFailure()
    {
        return _random.NextDouble() < _failureRate;
    }

    /// <summary>
    ///     Configures the retry parameters.
    /// </summary>
    /// <param name="maxRetries">Maximum number of retries after the initial attempt.</param>
    /// <param name="initialDelay">Initial delay before the first retry.</param>
    /// <param name="backoffMultiplier">Multiplier for exponential backoff.</param>
    public void ConfigureRetry(int maxRetries, TimeSpan initialDelay, double backoffMultiplier = 2.0)
    {
        if (maxRetries < 0)
            throw new ArgumentOutOfRangeException(nameof(maxRetries), "Max retries must be non-negative");

        if (initialDelay < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(initialDelay), "Initial delay must be non-negative");

        if (backoffMultiplier <= 0)
            throw new ArgumentOutOfRangeException(nameof(backoffMultiplier), "Backoff multiplier must be positive");

        _maxRetries = maxRetries;
        _initialDelay = initialDelay;
        _backoffMultiplier = backoffMultiplier;

        Console.WriteLine(
            $"RetryTransform: Configured with maxRetries={maxRetries}, initialDelay={initialDelay.TotalMilliseconds}ms, backoffMultiplier={backoffMultiplier}");
    }

    /// <summary>
    ///     Sets the failure rate for testing purposes.
    ///     A value between 0.0 (never fails) and 1.0 (always fails).
    /// </summary>
    /// <param name="failureRate">The failure rate to set.</param>
    public void SetFailureRate(double failureRate)
    {
        if (failureRate < 0.0 || failureRate > 1.0)
            throw new ArgumentOutOfRangeException(nameof(failureRate), "Failure rate must be between 0.0 and 1.0");

        _failureRate = failureRate;
        Console.WriteLine($"RetryTransform: Failure rate set to {_failureRate:P1}");
    }
}
