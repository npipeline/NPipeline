namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Configuration for retry delay strategies combining backoff and jitter approaches.
/// </summary>
/// <remarks>
///     <para>
///         This configuration combines a backoff strategy (which determines how delays increase over time)
///         with a jitter strategy (which adds randomness to prevent thundering herd problems).
///         The combination provides both controlled delay growth and distributed retry timing.
///     </para>
///     <para>
///         Common combinations:
///         <list type="bullet">
///             <item>
///                 <description>Exponential backoff + Full jitter: Best for distributed systems with transient failures</description>
///             </item>
///             <item>
///                 <description>Linear backoff + Equal jitter: Good for predictable recovery with some randomness</description>
///             </item>
///             <item>
///                 <description>Fixed delay + No jitter: Simple deterministic retry behavior</description>
///             </item>
///         </list>
///     </para>
/// </remarks>
public sealed record RetryDelayStrategyConfiguration(
    BackoffStrategyConfiguration BackoffConfiguration,
    JitterStrategyConfiguration JitterConfiguration)
{
    /// <summary>
    ///     Validates the retry delay strategy configuration.
    /// </summary>
    /// <exception cref="ArgumentNullException">Thrown when BackoffConfiguration or JitterConfiguration is null.</exception>
    /// <exception cref="ArgumentException">Thrown when any configuration parameter is invalid.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the configuration state is invalid.</exception>
    /// <remarks>
    ///     This method validates both the backoff and jitter configurations and ensures
    ///     they are compatible with each other.
    /// </remarks>
    public void Validate()
    {
        if (BackoffConfiguration is null)
            throw new ArgumentNullException(nameof(BackoffConfiguration), "BackoffConfiguration cannot be null.");

        if (JitterConfiguration is null)
            throw new ArgumentNullException(nameof(JitterConfiguration), "JitterConfiguration cannot be null.");

        // Validate individual configurations
        BackoffConfiguration.Validate();
        JitterConfiguration.Validate();

        // Additional cross-validation can be added here if needed
        // For example, certain combinations might not be recommended
    }
}
