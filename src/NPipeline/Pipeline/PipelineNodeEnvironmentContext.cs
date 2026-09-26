using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using NPipeline.Execution;
using NPipeline.Nodes;

namespace NPipeline.Pipeline;

/// <summary>
///     Node-scoped execution state and registry services for the current run.
/// </summary>
public sealed class PipelineNodeEnvironmentContext
{
    private readonly ConcurrentDictionary<INode, string> _nodeIdsByInstance =
        new(ReferenceEqualityComparer.Instance);

    private readonly ConcurrentDictionary<string, NodeExecutionStatus> _nodeStatuses =
        new(StringComparer.Ordinal);

    /// <summary>
    ///     Registry for node execution annotations, observability scopes, and runtime annotations.
    /// </summary>
    public NodeExecutionScopeRegistry NodeExecutionScopeRegistry { get; } = new();

    /// <summary>
    ///     Optional preconfigured node instances to seed graph construction.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Keys are node ids, the sanitized, lower-case form of the node's name; an unknown id fails the build. An
    ///         instance supplied here replaces the one the pipeline definition registered for that node.
    ///     </para>
    ///     <para>
    ///         The run owns these instances: it disposes each one when the run ends, however it ends. Supply a fresh
    ///         instance for each run; one reused across runs is already disposed on the second.
    ///     </para>
    /// </remarks>
    public Dictionary<string, INode> PreconfiguredNodeInstances { get; } = new();

    /// <summary>
    ///     Gets the id under which <paramref name="node" /> is running in this pipeline.
    /// </summary>
    /// <param name="node">The node asking, normally <c>this</c>.</param>
    /// <returns>The node's id in the graph.</returns>
    /// <remarks>
    ///     A node's id belongs to the graph, not to the node object and not to the run as a whole, so it is looked up
    ///     by the instance's reference identity. The answer is exact no matter how many nodes are running at once,
    ///     and it costs one dictionary lookup only when a node asks.
    /// </remarks>
    /// <exception cref="InvalidOperationException">
    ///     The node is not part of this run, or the same instance was registered under more than one id. Use
    ///     <see cref="TryGetNodeId" /> where a node may legitimately run outside a pipeline, such as in a unit test.
    /// </exception>
    public string GetNodeId(INode node)
    {
        ArgumentNullException.ThrowIfNull(node);

        if (TryGetNodeId(node, out var nodeId))
            return nodeId;

        throw new InvalidOperationException(ErrorMessages.NodeIdNotResolvable(
            node.GetType().FullName ?? node.GetType().Name,
            _nodeIdsByInstance.IsEmpty));
    }

    /// <summary>
    ///     Gets the id under which <paramref name="node" /> is running, if it can be resolved.
    /// </summary>
    /// <param name="node">The node asking, normally <c>this</c>.</param>
    /// <param name="nodeId">The node's id in the graph, or <see cref="string.Empty" /> when it cannot be resolved.</param>
    /// <returns>
    ///     <see langword="false" /> when the node is not part of this run — it was constructed outside a pipeline, or
    ///     the same instance is registered under more than one id and the question has no single answer.
    /// </returns>
    public bool TryGetNodeId(INode node, [NotNullWhen(true)] out string? nodeId)
    {
        ArgumentNullException.ThrowIfNull(node);

        if (_nodeIdsByInstance.TryGetValue(node, out var found) && found.Length > 0)
        {
            nodeId = found;
            return true;
        }

        nodeId = null;
        return false;
    }

    /// <summary>
    ///     Associates a node instance with the id it runs as, so the node can ask for it with
    ///     <see cref="GetNodeId" />.
    /// </summary>
    /// <param name="nodeId">The node's id in the graph.</param>
    /// <param name="node">The instance occupying that graph position.</param>
    /// <remarks>
    ///     A pipeline run registers its nodes during setup. Call it directly when driving a node outside a run — a
    ///     unit test exercising a node that wants to know its id. Registering the same instance under a second id
    ///     records the ambiguity instead of choosing between them, so asking then reports it rather than answering
    ///     with whichever registration happened to be last.
    /// </remarks>
    public void RegisterNode(string nodeId, INode node)
    {
        ArgumentException.ThrowIfNullOrEmpty(nodeId);
        ArgumentNullException.ThrowIfNull(node);

        _ = _nodeIdsByInstance.AddOrUpdate(
            node,
            nodeId,
            (_, existing) => existing == nodeId
                ? existing
                : string.Empty);
    }

    internal void RegisterNodes(IReadOnlyDictionary<string, INode> nodeInstances)
    {
        foreach (var (nodeId, instance) in nodeInstances)
        {
            RegisterNode(nodeId, instance);
        }
    }

    /// <summary>
    ///     Gets the outcome recorded for <paramref name="nodeId" /> in this run.
    /// </summary>
    /// <param name="nodeId">The node's id in the graph.</param>
    /// <returns>
    ///     <see cref="NodeExecutionStatus.Pending" /> until the node finishes, then
    ///     <see cref="NodeExecutionStatus.Completed" /> or <see cref="NodeExecutionStatus.Failed" />.
    /// </returns>
    public NodeExecutionStatus GetNodeStatus(string nodeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(nodeId);

        return _nodeStatuses.TryGetValue(nodeId, out var status)
            ? status
            : NodeExecutionStatus.Pending;
    }

    /// <summary>
    ///     Enumerates the nodes that have finished, with the outcome recorded for each.
    /// </summary>
    /// <remarks>
    ///     Nodes still pending are absent rather than reported as <see cref="NodeExecutionStatus.Pending" />.
    /// </remarks>
    public IEnumerable<KeyValuePair<string, NodeExecutionStatus>> EnumerateNodeStatuses() => _nodeStatuses;

    /// <summary>
    ///     Records the outcome of a node that has finished executing.
    /// </summary>
    /// <remarks>
    ///     Terminal nodes below a fan-out finish on separate threads, so this is backed by a concurrent dictionary
    ///     rather than the profile-dependent context bags.
    /// </remarks>
    internal void SetNodeStatus(string nodeId, NodeExecutionStatus status)
    {
        _nodeStatuses[nodeId] = status;
    }
}
