namespace NPipeline.Execution.RetryDelay.Jitter;

/// <summary>
///     Configuration parameters for no jitter strategy.
/// </summary>
/// <remarks>
///     <para>
///         No jitter simply returns the base delay without any randomization.
///         This strategy is useful for testing or when deterministic behavior is needed.
///     </para>
///     <para>
///         The formula used is: jitteredDelay = baseDelay
///     </para>
/// </remarks>
public sealed class NoJitterConfiguration
{
    /// <summary>
    ///     Validates the configuration parameters.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    /// <remarks>
    ///     No jitter doesn't require any specific configuration parameters,
    ///     so this method is provided for consistency with other jitter strategies.
    /// </remarks>
    public void Validate()
    {
        // No jitter doesn't require any configuration parameters
        // This method is provided for consistency with other jitter strategies
    }
}
