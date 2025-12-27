using NPipeline.Configuration.RetryDelay;

namespace NPipeline.Execution.RetryDelay;

/// <summary>
///     Provides validation methods for retry delay strategies and their configurations.
/// </summary>
/// <remarks>
///     <para>
///         This validator ensures that retry delay strategies and their configurations
///         are valid before they are used in the pipeline. It helps prevent runtime
///         errors by validating parameters early.
///     </para>
///     <para>
///         The validator is designed to be stateless and thread-safe, making it safe
///         to use across multiple threads and operations.
///     </para>
/// </remarks>
public static class RetryDelayStrategyValidator
{
    /// <summary>
    ///     Validates a backoff strategy.
    /// </summary>
    /// <param name="backoffStrategy">The backoff strategy to validate.</param>
    /// <exception cref="ArgumentNullException">Thrown when backoffStrategy is null.</exception>
    public static void ValidateBackoffStrategy(BackoffStrategy backoffStrategy)
    {
        ArgumentNullException.ThrowIfNull(backoffStrategy);
    }

    /// <summary>
    ///     Validates a jitter strategy.
    /// </summary>
    /// <param name="jitterStrategy">The jitter strategy to validate.</param>
    /// <exception cref="ArgumentNullException">Thrown when jitterStrategy is null.</exception>
    public static void ValidateJitterStrategy(JitterStrategy jitterStrategy)
    {
        ArgumentNullException.ThrowIfNull(jitterStrategy);
    }

    /// <summary>
    ///     Validates a retry delay strategy.
    /// </summary>
    /// <param name="retryDelayStrategy">The retry delay strategy to validate.</param>
    /// <exception cref="ArgumentNullException">Thrown when retryDelayStrategy is null.</exception>
    public static void ValidateRetryDelayStrategy(IRetryDelayStrategy retryDelayStrategy)
    {
        ArgumentNullException.ThrowIfNull(retryDelayStrategy);
    }

    /// <summary>
    ///     Validates a retry delay strategy configuration.
    /// </summary>
    /// <param name="configuration">The configuration to validate.</param>
    /// <exception cref="ArgumentNullException">Thrown when configuration is null.</exception>
    /// <exception cref="ArgumentNullException">Thrown when BackoffStrategy is null.</exception>
    public static void ValidateRetryDelayStrategyConfiguration(RetryDelayStrategyConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        if (configuration.BackoffStrategy is null)
            throw new ArgumentNullException(nameof(configuration), "BackoffStrategy cannot be null.");

        ValidateBackoffStrategy(configuration.BackoffStrategy);

        // Only validate jitter strategy if it's present (it's optional)
        if (configuration.JitterStrategy is not null)
            ValidateJitterStrategy(configuration.JitterStrategy);
    }

    /// <summary>
    ///     Validates that an attempt number is within acceptable bounds.
    /// </summary>
    /// <param name="attemptNumber">The attempt number to validate.</param>
    /// <returns>True if the attempt number is valid, false otherwise.</returns>
    /// <remarks>
    ///     Attempt numbers should be non-negative. Negative values are considered invalid
    ///     and typically result in TimeSpan.Zero being returned by strategies.
    /// </remarks>
    public static bool IsValidAttemptNumber(int attemptNumber)
    {
        return attemptNumber >= 0;
    }

    /// <summary>
    ///     Validates that a delay is within acceptable bounds.
    /// </summary>
    /// <param name="delay">The delay to validate.</param>
    /// <returns>True if the delay is valid, false otherwise.</returns>
    /// <remarks>
    ///     Delays should be non-negative. Negative values are considered invalid
    ///     and may cause unexpected behavior.
    /// </remarks>
    public static bool IsValidDelay(TimeSpan delay)
    {
        return delay >= TimeSpan.Zero;
    }

    /// <summary>
    ///     Validates that a delay is within acceptable bounds and throws if not.
    /// </summary>
    /// <param name="delay">The delay to validate.</param>
    /// <param name="parameterName">The name of the parameter being validated.</param>
    /// <exception cref="ArgumentException">Thrown when delay is negative.</exception>
    public static void ValidateDelay(TimeSpan delay, string parameterName)
    {
        if (delay < TimeSpan.Zero)
            throw new ArgumentException("Delay must be non-negative.", parameterName);
    }
}
