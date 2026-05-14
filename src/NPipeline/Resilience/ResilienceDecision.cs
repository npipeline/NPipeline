namespace NPipeline.Resilience;

/// <summary>
///     Unified decision model used by resilience policy execution.
/// </summary>
public enum ResilienceDecision
{
    /// <summary>
    ///     Stop execution and surface the failure.
    /// </summary>
    Fail,

    /// <summary>
    ///     Retry the current operation.
    /// </summary>
    Retry,

    /// <summary>
    ///     Skip the current item and continue.
    /// </summary>
    Skip,

    /// <summary>
    ///     Route the current item to dead-letter handling and continue.
    /// </summary>
    DeadLetter,

    /// <summary>
    ///     Restart the failed node/stream execution.
    /// </summary>
    RestartNode,

    /// <summary>
    ///     Continue pipeline execution without the failed node.
    /// </summary>
    ContinueWithoutNode,
}
