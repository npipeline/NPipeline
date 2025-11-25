namespace NPipeline.Execution.RetryDelay;

/// <summary>
///     A no-operation retry delay strategy that always returns TimeSpan.Zero.
/// </summary>
/// <remarks>
///     <para>
///         This strategy is useful when you want to retry immediately without any delay,
///         typically for testing scenarios or when delays are handled elsewhere.
///     </para>
///     <para>
///         The implementation is thread-safe and stateless, making it safe to share
///         across multiple nodes and concurrent operations.
///     </para>
/// </remarks>
public sealed class NoOpRetryDelayStrategy : IRetryDelayStrategy
{
    /// <summary>
    ///     Gets the singleton instance of the <see cref="NoOpRetryDelayStrategy" />.
    /// </summary>
    /// <remarks>
    ///     Using the singleton instance is recommended for performance and memory efficiency.
    /// </remarks>
    public static readonly NoOpRetryDelayStrategy Instance = new();

    /// <summary>
    ///     Initializes a new instance of the <see cref="NoOpRetryDelayStrategy" /> class.
    /// </summary>
    /// <remarks>
    ///     Private constructor to enforce singleton pattern.
    ///     Use the <see cref="Instance" /> property instead.
    /// </remarks>
    private NoOpRetryDelayStrategy()
    {
    }

    /// <summary>
    ///     Always returns TimeSpan.Zero for any attempt number.
    /// </summary>
    /// <param name="attemptNumber">The current attempt number (ignored).</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>Always returns TimeSpan.Zero.</returns>
    /// <remarks>
    ///     This method respects cancellation requests and will return a cancelled task
    ///     if the cancellation token is triggered before the method completes.
    /// </remarks>
    public ValueTask<TimeSpan> GetDelayAsync(int attemptNumber, CancellationToken cancellationToken = default)
    {
        // Check for cancellation before returning
        if (cancellationToken.IsCancellationRequested)
            return ValueTask.FromCanceled<TimeSpan>(cancellationToken);

        return ValueTask.FromResult(TimeSpan.Zero);
    }
}
