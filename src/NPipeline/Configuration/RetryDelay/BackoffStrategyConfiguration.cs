namespace NPipeline.Configuration.RetryDelay;

/// <summary>
///     Base abstract configuration for backoff strategies used in retry delay calculations.
/// </summary>
/// <remarks>
///     <para>
///         Backoff strategies determine how the delay between retry attempts increases over time.
///         Different strategies are suitable for different scenarios:
///         <list type="bullet">
///             <item>
///                 <description>Exponential backoff: Good for distributed systems with transient failures</description>
///             </item>
///             <item>
///                 <description>Linear backoff: Good for predictable, gradual recovery scenarios</description>
///             </item>
///             <item>
///                 <description>Fixed delay: Good for simple retry scenarios with consistent retry intervals</description>
///             </item>
///         </list>
///     </para>
///     <para>
///         All backoff strategy configurations must inherit from this abstract record
///         and provide their specific validation logic.
///     </para>
/// </remarks>
public abstract record BackoffStrategyConfiguration
{
    /// <summary>
    ///     Gets the type identifier for the backoff strategy.
    /// </summary>
    /// <remarks>
    ///     This property is used to identify the specific backoff strategy implementation
    ///     when creating retry delay strategies from configuration.
    /// </remarks>
    public abstract string StrategyType { get; }

    /// <summary>
    ///     Validates the configuration parameters specific to the backoff strategy.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when any parameter is invalid.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the configuration state is invalid.</exception>
    /// <remarks>
    ///     Each concrete backoff strategy configuration should implement this method
    ///     to validate its specific parameters and throw appropriate exceptions
    ///     with meaningful error messages.
    /// </remarks>
    public abstract void Validate();
}
