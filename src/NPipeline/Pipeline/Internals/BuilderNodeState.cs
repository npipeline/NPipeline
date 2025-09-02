using NPipeline.Configuration;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Pipeline.Internals;

/// <summary>
///     Encapsulates node-related state in the PipelineBuilder.
///     This internal class groups all node registration, preconfiguration, and metadata fields.
/// </summary>
internal sealed class BuilderNodeState
{
    /// <summary>
    ///     Dictionary of registered node definitions keyed by node ID.
    /// </summary>
    public Dictionary<string, NodeDefinition> Nodes { get; } = new();

    /// <summary>
    ///     Dictionary of pre-instantiated node instances keyed by node ID.
    /// </summary>
    public Dictionary<string, INode> PreconfiguredNodeInstances { get; } = new();

    /// <summary>
    ///     Dictionary of execution annotations for nodes keyed by node ID.
    /// </summary>
    public Dictionary<string, object> ExecutionAnnotations { get; } = new();

    /// <summary>
    ///     Dictionary of retry option overrides for specific nodes keyed by node ID.
    /// </summary>
    public Dictionary<string, PipelineRetryOptions> RetryOverrides { get; } = new();

    /// <summary>
    ///     Clears all node state.
    /// </summary>
    public void Clear()
    {
        Nodes.Clear();
        PreconfiguredNodeInstances.Clear();
        ExecutionAnnotations.Clear();
        RetryOverrides.Clear();
    }
}
