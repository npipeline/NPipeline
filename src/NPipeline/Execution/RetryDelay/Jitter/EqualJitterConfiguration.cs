namespace NPipeline.Execution.RetryDelay.Jitter;

/// <summary>
///     Configuration parameters for equal jitter strategy.
/// </summary>
/// <remarks>
///     <para>
///         Equal jitter splits the delay equally between a fixed portion and random portion.
///         The formula used is: jitteredDelay = baseDelay/2 + random.Next(0, baseDelay/2)
///     </para>
///     <para>
///         This strategy provides a balance between predictability and randomness,
///         ensuring at least half of the base delay while still adding jitter to prevent
///         thundering herd problems.
///     </para>
/// </remarks>
public sealed class EqualJitterConfiguration
{
    /// <summary>
    ///     Validates the configuration parameters.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    /// <remarks>
    ///     Equal jitter doesn't require any specific configuration parameters,
    ///     so this method is provided for consistency with other jitter strategies.
    /// </remarks>
    public void Validate()
    {
        // Equal jitter doesn't require any configuration parameters
        // This method is provided for consistency with other jitter strategies
    }
}
