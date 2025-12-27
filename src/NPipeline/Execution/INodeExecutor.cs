using NPipeline.DataFlow;
using NPipeline.Execution.Plans;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution;

/// <summary>
///     Defines a service responsible for executing a single node within a pipeline graph.
/// </summary>
public interface INodeExecutor
{
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
    Task ExecuteAsync(
        NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataPipe?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IReadOnlyDictionary<string, NodeDefinition> nodeDefinitionMap);
}
