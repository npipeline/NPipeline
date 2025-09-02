using NPipeline.DataFlow;
using NPipeline.Execution.Plans;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Services;

/// <summary>
///     Implementation of IPipelineExecutionCoordinator that coordinates pipeline execution operations
///     including node execution, topology management, and node instantiation.
/// </summary>
public sealed class PipelineExecutionCoordinator(
    INodeExecutor nodeExecutor,
    ITopologyService topologyService,
    INodeInstantiationService nodeInstantiationService) : IPipelineExecutionCoordinator
{
    /// <inheritdoc />
    public Dictionary<string, INode> InstantiateNodes(PipelineGraph graph, INodeFactory nodeFactory)
    {
        return nodeInstantiationService.InstantiateNodes(graph, nodeFactory);
    }

    /// <inheritdoc />
    public void RegisterStatefulNodes(Dictionary<string, INode> nodeInstances, PipelineContext context)
    {
        nodeInstantiationService.RegisterStatefulNodes(nodeInstances, context);
    }

    /// <inheritdoc />
    public Dictionary<string, NodeExecutionPlan> BuildPlans(PipelineGraph graph, IReadOnlyDictionary<string, INode> nodeInstances)
    {
        return nodeInstantiationService.BuildPlans(graph, nodeInstances);
    }

    /// <inheritdoc />
    public List<string> TopologicalSort(PipelineGraph graph)
    {
        return topologyService.TopologicalSort(graph);
    }

    /// <inheritdoc />
    public ILookup<string, Edge> BuildInputLookup(PipelineGraph graph)
    {
        return topologyService.BuildInputLookup(graph);
    }

    /// <inheritdoc />
    public Task ExecuteNodeAsync(
        NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataPipe?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IReadOnlyDictionary<string, NodeDefinition> nodeDefinitionMap)
    {
        return nodeExecutor.ExecuteAsync(
            plan,
            graph,
            context,
            inputLookup,
            nodeOutputs,
            nodeInstances,
            nodeDefinitionMap);
    }
}
