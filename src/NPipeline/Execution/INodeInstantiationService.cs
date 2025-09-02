using NPipeline.Execution.Plans;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution;

/// <summary>
///     Service responsible for instantiating pipeline nodes and registering stateful nodes.
/// </summary>
public interface INodeInstantiationService
{
    /// <summary>
    ///     Instantiates all nodes in the pipeline graph using the provided node factory.
    /// </summary>
    /// <param name="graph">The pipeline graph containing node definitions.</param>
    /// <param name="nodeFactory">The factory used to create node instances.</param>
    /// <returns>A dictionary mapping node IDs to their instantiated node instances.</returns>
    Dictionary<string, INode> InstantiateNodes(PipelineGraph graph, INodeFactory nodeFactory);

    /// <summary>
    ///     Registers stateful nodes with the state registry if available in the context.
    /// </summary>
    /// <param name="nodeInstances">The dictionary of instantiated nodes.</param>
    /// <param name="context">The pipeline execution context.</param>
    void RegisterStatefulNodes(Dictionary<string, INode> nodeInstances, PipelineContext context);

    /// <summary>
    ///     Builds per-run execution plans binding generic strategies to non-generic delegates.
    /// </summary>
    /// <param name="graph">The pipeline graph.</param>
    /// <param name="nodeInstances">Instantiated nodes keyed by node id.</param>
    /// <returns>Dictionary of execution plans keyed by node id.</returns>
    Dictionary<string, NodeExecutionPlan> BuildPlans(PipelineGraph graph, IReadOnlyDictionary<string, INode> nodeInstances);
}
