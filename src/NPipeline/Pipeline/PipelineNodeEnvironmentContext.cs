using NPipeline.Execution;
using NPipeline.Nodes;

namespace NPipeline.Pipeline;

/// <summary>
///     Node-scoped execution state and registry services for the current run.
/// </summary>
public sealed class PipelineNodeEnvironmentContext
{
    private string _currentNodeId = string.Empty;
    private volatile bool _nodesRunConcurrently;

    /// <summary>
    ///     The ID of the node currently being executed.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         A single field on a context shared by every node in the run, so it is only meaningful while nodes run
    ///         one at a time. It is maintained for node authors who want to know which node they are running as; no
    ///         framework decision depends on it. Framework code receives the node id explicitly — see the
    ///         <c>nodeId</c> parameter on <see cref="Execution.IExecutionStrategy" />.
    ///     </para>
    ///     <para>
    ///         While terminal nodes drain concurrently, the field is frozen rather than written by each of them: see
    ///         <see cref="NodesRunConcurrently" />. A node running in that phase therefore sees a stale id rather than
    ///         another node's, and the field is never left pointing somewhere arbitrary afterwards.
    ///     </para>
    /// </remarks>
    public string CurrentNodeId
    {
        get => _currentNodeId;
        internal set => _currentNodeId = value ?? string.Empty;
    }

    /// <summary>
    ///     Set while more than one node is executing at once, which is the case only when terminal nodes below a
    ///     fan-out are drained together. <see cref="PipelineContext.ScopedNode" /> is inert while this is set, so
    ///     concurrent workers cannot interleave writes to <see cref="CurrentNodeId" />.
    /// </summary>
    internal bool NodesRunConcurrently
    {
        get => _nodesRunConcurrently;
        set => _nodesRunConcurrently = value;
    }

    /// <summary>
    ///     Registry for node execution annotations, observability scopes, and runtime annotations.
    /// </summary>
    public NodeExecutionScopeRegistry NodeExecutionScopeRegistry { get; } = new();

    /// <summary>
    ///     Optional preconfigured node instances to seed graph construction.
    /// </summary>
    public Dictionary<string, INode> PreconfiguredNodeInstances { get; } = new();

    /// <summary>
    ///     Indicates node lifetimes are owned externally (for example by DI container).
    /// </summary>
    public bool DiOwnedNodes { get; set; }
}
