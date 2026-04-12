namespace NPipeline.Extensions.Composition;

/// <summary>
///     Well-known keys used for composite node context data.
/// </summary>
public static class CompositeContextKeys
{
    /// <summary>
    ///     Key for storing the input item in sub-pipeline context.
    /// </summary>
    public const string InputItem = "__Composite_InputItem";

    /// <summary>
    ///     Key for storing the output item in sub-pipeline context.
    /// </summary>
    public const string OutputItem = "__Composite_OutputItem";

    /// <summary>
    ///     Key for storing the parent node identifier in sub-pipeline context.
    /// </summary>
    public const string ParentNodeId = "__Composite_ParentNodeId";

    /// <summary>
    ///     Key for storing the parent pipeline identifier in sub-pipeline context.
    /// </summary>
    public const string ParentPipelineId = "__Composite_ParentPipelineId";

    /// <summary>
    ///     Key for storing the parent pipeline name in sub-pipeline context.
    /// </summary>
    public const string ParentPipelineName = "__Composite_ParentPipelineName";
}
