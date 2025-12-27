using NPipeline.Execution.Annotations;
using NPipeline.Pipeline;

namespace NPipeline.DataFlow.Branching;

/// <summary>
///     Options controlling branching (multicast) behavior when a node feeds multiple downstream targets.
///     Prefer configuring a global default via PipelineBuilderExtensions.WithGlobalBranchingCapacity, and override per node with WithBranchOptions.
/// </summary>
/// <param name="PerSubscriberBufferCapacity">Optional bounded buffer size per subscriber. Null = unbounded (default).</param>
public sealed record BranchOptions(int? PerSubscriberBufferCapacity = null);

/// <summary>
///     Extension methods for configuring branching options in pipeline builders.
/// </summary>
public static class BranchingPipelineBuilderExtensions
{
    /// <summary>
    ///     Applies branch options for a node whose output is consumed by multiple downstream nodes.
    /// </summary>
    /// <param name="builder">The pipeline builder to configure.</param>
    /// <param name="nodeId">The ID of the node to apply branching options to.</param>
    /// <param name="options">The branching options to apply.</param>
    /// <returns>The configured pipeline builder for method chaining.</returns>
    public static PipelineBuilder WithBranchOptions(this PipelineBuilder builder, string nodeId, BranchOptions options)
    {
        // Re-use execution annotations bag for branch metadata with distinct key prefix
        builder.SetNodeExecutionOption(ExecutionAnnotationKeys.BranchOptionsForNode(nodeId), options);
        return builder;
    }
}
