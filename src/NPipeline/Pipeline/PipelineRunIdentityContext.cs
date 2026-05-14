namespace NPipeline.Pipeline;

/// <summary>
///     Run-level identity state for a pipeline execution.
/// </summary>
public sealed class PipelineRunIdentityContext
{
    internal PipelineRunIdentityContext(DateTime pipelineStartTimeUtc)
    {
        PipelineStartTimeUtc = pipelineStartTimeUtc;
    }

    /// <summary>
    ///     The pipeline-level UTC start timestamp.
    /// </summary>
    public DateTime PipelineStartTimeUtc { get; internal set; }

    /// <summary>
    ///     Unique pipeline identity for this execution context.
    /// </summary>
    public Guid PipelineId { get; internal set; }

    /// <summary>
    ///     Unique run identifier for this pipeline execution.
    /// </summary>
    public Guid RunId { get; internal set; }

    /// <summary>
    ///     Logical pipeline name for this execution context.
    /// </summary>
    public string? PipelineName { get; internal set; }
}
