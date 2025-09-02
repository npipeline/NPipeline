namespace NPipeline.ErrorHandling;

/// <summary>
///     Specifies the decision to be made after a pipeline-level error has been handled.
/// </summary>
public enum PipelineErrorDecision
{
    /// <summary>
    ///     Indicates that the entire pipeline should fail. This is the default behavior.
    /// </summary>
    FailPipeline,

    /// <summary>
    ///     Indicates that the failed node should be isolated, and the pipeline should continue processing other branches if possible.
    /// </summary>
    ContinueWithoutNode,

    /// <summary>
    ///     Indicates that the pipeline should attempt to restart the failed node and continue execution.
    /// </summary>
    RestartNode,
}
