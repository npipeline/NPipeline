using NPipeline.Configuration;
using NPipeline.Pipeline;

namespace NPipeline.Execution;

/// <summary>
///     Resolves effective retry options for a node using the standard precedence.
/// </summary>
internal static class RetryOptionsResolver
{
    /// <summary>
    ///     Resolves retry options with precedence: node override -> global options.
    /// </summary>
    /// <param name="context">The active pipeline context.</param>
    /// <param name="nodeId">The node identifier.</param>
    /// <returns>The effective retry options for the node.</returns>
    public static PipelineRetryOptions Resolve(PipelineContext context, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(nodeId);

        if (context.NodeRetryOverrides.TryGetValue(nodeId, out var nodeOptions))
            return nodeOptions;

        return context.GlobalRetryOptions;
    }
}
