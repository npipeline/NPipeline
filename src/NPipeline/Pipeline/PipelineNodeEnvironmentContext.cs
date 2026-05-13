using NPipeline.Execution;
using NPipeline.Nodes;

namespace NPipeline.Pipeline;

/// <summary>
///     Node-scoped execution state and registry services for the current run.
/// </summary>
public sealed class PipelineNodeEnvironmentContext
{
    private string _currentNodeId = string.Empty;

    /// <summary>
    ///     The ID of the node currently being executed.
    /// </summary>
    public string CurrentNodeId
    {
        get => _currentNodeId;
        internal set => _currentNodeId = value ?? string.Empty;
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
