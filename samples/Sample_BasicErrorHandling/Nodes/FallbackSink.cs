using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_BasicErrorHandling.Nodes;

/// <summary>
///     Sink node that implements graceful degradation with primary and fallback output mechanisms.
///     This node demonstrates how to handle output failures by falling back to alternative
///     output methods when the primary mechanism fails.
/// </summary>
public class FallbackSink : SinkNode<string>
{
    private readonly Random _random = new();
    private int _fallbackActivationCount;
    private double _primaryFailureRate = 0.3; // 30% chance of primary output failure
    private int _primarySuccessCount;
    private bool _useFallbackOnly;

    /// <summary>
    ///     Initializes a new instance of the FallbackSink class.
    ///     Parameterless constructor for DI compatibility.
    /// </summary>
    public FallbackSink()
    {
    }

    /// <summary>
    ///     Processes the input strings using primary output with fallback to console output.
    /// </summary>
    /// <param name="input">The data pipe containing input strings to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task representing the sink execution.</returns>
    public override async Task ExecuteAsync(IDataPipe<string> input, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("FallbackSink: Starting to process messages");

        var messageCount = 0;
        var failedMessages = new List<string>();

        // Use await foreach to consume all messages from the input pipe
        await foreach (var message in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();
            messageCount++;

            try
            {
                // Try to process using the primary output mechanism
                await ProcessWithPrimaryOutput(message, cancellationToken);
                _primarySuccessCount++;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"FallbackSink: Primary output failed for message '{message}': {ex.Message}");

                try
                {
                    // Fall back to console output
                    await ProcessWithFallbackOutput(message, cancellationToken);
                    _fallbackActivationCount++;
                    Console.WriteLine($"FallbackSink: Successfully used fallback output for message: {message}");
                }
                catch (Exception fallbackEx)
                {
                    Console.WriteLine($"FallbackSink: Fallback output also failed for message '{message}': {fallbackEx.Message}");
                    failedMessages.Add(message);
                }
            }
        }

        Console.WriteLine(
            $"FallbackSink: Processing complete. Total: {messageCount}, Primary Success: {_primarySuccessCount}, Fallback Used: {_fallbackActivationCount}");

        if (failedMessages.Count > 0)
            Console.WriteLine($"FallbackSink: {failedMessages.Count} messages could not be processed even with fallback");
    }

    /// <summary>
    ///     Processes a message using the primary output mechanism.
    /// </summary>
    /// <param name="message">The message to process.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A Task representing the processing operation.</returns>
    /// <exception cref="InvalidOperationException">Thrown randomly to simulate primary output failures.</exception>
    private async Task ProcessWithPrimaryOutput(string message, CancellationToken cancellationToken)
    {
        // Simulate some processing time
        await Task.Delay(5, cancellationToken);

        // If forced to use fallback only, always fail primary
        if (_useFallbackOnly)
            throw new InvalidOperationException("Primary output disabled - forcing fallback usage");

        // Randomly decide if this primary output should fail
        if (ShouldSimulatePrimaryFailure())
            throw new InvalidOperationException($"Simulated primary output failure for message: {message}");

        // Simulate successful primary output (e.g., writing to a database, file, or external service)
        Console.WriteLine($"[PRIMARY OUTPUT] Processed: {message}");
    }

    /// <summary>
    ///     Processes a message using the fallback output mechanism (console).
    /// </summary>
    /// <param name="message">The message to process.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A Task representing the processing operation.</returns>
    private async Task ProcessWithFallbackOutput(string message, CancellationToken cancellationToken)
    {
        // Simulate some processing time
        await Task.Delay(2, cancellationToken);

        // Fallback output - always succeeds in this implementation
        Console.WriteLine($"[FALLBACK OUTPUT] {message}");
    }

    /// <summary>
    ///     Determines whether to simulate a primary output failure based on the failure rate.
    /// </summary>
    /// <returns>True if a failure should be simulated, false otherwise.</returns>
    private bool ShouldSimulatePrimaryFailure()
    {
        return _random.NextDouble() < _primaryFailureRate;
    }

    /// <summary>
    ///     Sets the primary output failure rate for testing purposes.
    ///     A value between 0.0 (never fails) and 1.0 (always fails).
    /// </summary>
    /// <param name="failureRate">The failure rate to set.</param>
    public void SetPrimaryFailureRate(double failureRate)
    {
        if (failureRate < 0.0 || failureRate > 1.0)
            throw new ArgumentOutOfRangeException(nameof(failureRate), "Failure rate must be between 0.0 and 1.0");

        _primaryFailureRate = failureRate;
        Console.WriteLine($"FallbackSink: Primary failure rate set to {_primaryFailureRate:P1}");
    }

    /// <summary>
    ///     Forces the sink to always use the fallback mechanism.
    ///     Useful for testing fallback behavior.
    /// </summary>
    /// <param name="useFallbackOnly">True to force fallback usage, false for normal operation.</param>
    public void SetFallbackOnly(bool useFallbackOnly)
    {
        _useFallbackOnly = useFallbackOnly;
        Console.WriteLine($"FallbackSink: Fallback-only mode {(useFallbackOnly ? "enabled" : "disabled")}");
    }

    /// <summary>
    ///     Gets the processing statistics for this sink.
    /// </summary>
    /// <returns>A tuple containing primary success count and fallback activation count.</returns>
    public (int PrimarySuccessCount, int FallbackActivationCount) GetStatistics()
    {
        return (_primarySuccessCount, _fallbackActivationCount);
    }

    /// <summary>
    ///     Resets the processing statistics.
    /// </summary>
    public void ResetStatistics()
    {
        _primarySuccessCount = 0;
        _fallbackActivationCount = 0;
        Console.WriteLine("FallbackSink: Statistics reset");
    }
}
