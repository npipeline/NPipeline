namespace NPipeline.Pipeline;

/// <summary>
///     The outcome recorded for a node in the current pipeline run.
/// </summary>
public enum NodeExecutionStatus
{
    /// <summary>
    ///     The node has not finished executing in this run, either because it has not started or because it is
    ///     still running.
    /// </summary>
    Pending = 0,

    /// <summary>
    ///     The node completed successfully.
    /// </summary>
    Completed = 1,

    /// <summary>
    ///     The node failed, after any configured retries were exhausted.
    /// </summary>
    Failed = 2,
}
