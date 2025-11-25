namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Base abstract configuration for jitter strategies used in retry delay calculations.
/// </summary>
/// <remarks>
///     <para>
///         Jitter strategies add randomness to retry delays to prevent thundering herd problems
///         where multiple clients retry simultaneously after a failure. Different jitter strategies
///         provide different trade-offs between randomness and predictability:
///         <list type="bullet">
///             <item>
///                 <description>Full jitter: Maximum randomness, best for preventing thundering herds</description>
///             </item>
///             <item>
///                 <description>Equal jitter: Balanced approach with some predictability</description>
///             </item>
///             <item>
///                 <description>Decorrelated jitter: Adaptive approach based on previous delays</description>
///             </item>
///             <item>
///                 <description>No jitter: Deterministic delays, no randomness</description>
///             </item>
///         </list>
///     </para>
///     <para>
///         All jitter strategy configurations must inherit from this abstract record
///         and provide their specific validation logic.
///     </para>
/// </remarks>
public abstract record JitterStrategyConfiguration
{
    /// <summary>
    ///     Gets the type identifier for the jitter strategy.
    /// </summary>
    /// <remarks>
    ///     This property is used to identify the specific jitter strategy implementation
    ///     when creating retry delay strategies from configuration.
    /// </remarks>
    public abstract string StrategyType { get; }

    /// <summary>
    ///     Validates the configuration parameters specific to the jitter strategy.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the configuration state is invalid.</exception>
    /// <remarks>
    ///     Each concrete jitter strategy configuration should implement this method
    ///     to validate its specific parameters and throw appropriate exceptions
    ///     with meaningful error messages.
    /// </remarks>
    public abstract void Validate();
}