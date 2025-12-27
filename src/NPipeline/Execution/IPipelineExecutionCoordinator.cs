using NPipeline.DataFlow;
using NPipeline.Execution.Caching;
using NPipeline.Execution.Plans;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution;

/// <summary>
///     Facade interface that coordinates pipeline execution operations including node execution,
///     topology management, and node instantiation. See <see href="~/docs/reference/api/pipeline-execution-coordinator.md" />
///     for detailed documentation.
/// </summary>
public interface IPipelineExecutionCoordinator
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
    /// <param name="context">The pipeline execution context containing the state registry.</param>
    void RegisterStatefulNodes(Dictionary<string, INode> nodeInstances, PipelineContext context);

    /// <summary>
    ///     Builds per-run execution plans binding generic strategies to non-generic delegates.
    /// </summary>
    /// <param name="graph">The pipeline graph.</param>
    /// <param name="nodeInstances">Instantiated nodes keyed by node id.</param>
    /// <returns>Dictionary of execution plans keyed by node id.</returns>
    Dictionary<string, NodeExecutionPlan> BuildPlans(PipelineGraph graph, IReadOnlyDictionary<string, INode> nodeInstances);

    /// <summary>
    ///     Builds per-run execution plans binding generic strategies to non-generic delegates,
    ///     using the provided cache to retrieve or store compiled plans.
    /// </summary>
    /// <param name="pipelineDefinitionType">The type of the pipeline definition for cache keying.</param>
    /// <param name="graph">The pipeline graph.</param>
    /// <param name="nodeInstances">Instantiated nodes keyed by node id.</param>
    /// <param name="cache">The cache to use for storing and retrieving execution plans.</param>
    /// <returns>Dictionary of execution plans keyed by node id.</returns>
    Dictionary<string, NodeExecutionPlan> BuildPlansWithCache(
        Type pipelineDefinitionType,
        PipelineGraph graph,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IPipelineExecutionPlanCache cache);

    /// <summary>
    ///     Performs topological sort of nodes based on graph dependencies.
    /// </summary>
    /// <param name="graph">The pipeline graph.</param>
    /// <returns>A list of node IDs in topological order.</returns>
    List<string> TopologicalSort(PipelineGraph graph);

    /// <summary>
    ///     Builds an input lookup table for the graph.
    /// </summary>
    /// <param name="graph">The pipeline graph.</param>
    /// <returns>A lookup table mapping target node IDs to their input edges.</returns>
    ILookup<string, Edge> BuildInputLookup(PipelineGraph graph);

    /// <summary>
    ///     Executes a node using a pre-built execution plan.
    /// </summary>
    /// <param name="plan">The execution plan for the node.</param>
    /// <param name="graph">The pipeline graph.</param>
    /// <param name="context">The current pipeline context.</param>
    /// <param name="inputLookup">A lookup for finding input edges for a node.</param>
    /// <param name="nodeOutputs">A dictionary containing the output pipes of already executed nodes.</param>
    /// <param name="nodeInstances">A dictionary of all instantiated nodes in the graph.</param>
    /// <param name="nodeDefinitionMap">A dictionary mapping node IDs to their definitions.</param>
    /// <returns>A task that represents the asynchronous execution of the node.</returns>
    Task ExecuteNodeAsync(
        NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataPipe?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IReadOnlyDictionary<string, NodeDefinition> nodeDefinitionMap);
}
