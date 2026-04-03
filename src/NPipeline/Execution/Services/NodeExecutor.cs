using NPipeline.Attributes.Lineage;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution.Plans;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Services;

/// <summary>
///     Service responsible for executing a single node within a pipeline graph.
/// </summary>
public sealed class NodeExecutor(
    ILineageService lineageService,
    IPipeMergeService pipeMergeService,
    DataStreamWrapperService dataStreamWrapperService)
    : INodeExecutor
{
    /// <summary>
    ///     Plan-based execution path: the only execution path for node execution.
    /// </summary>
    public Task ExecuteAsync(
        NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataStream?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IReadOnlyDictionary<string, NodeDefinition> nodeDefinitionMap)
    {
        var nodeDef = nodeDefinitionMap[plan.NodeId];
        var instance = nodeInstances[plan.NodeId];

        return plan.Kind switch
        {
            NodeKind.Source when plan.ExecuteSource is not null =>
                ExecuteSourcePlanAsync(plan, graph, context, nodeOutputs),
            NodeKind.Transform when plan.ExecuteTransform is not null =>
                ExecuteTransformPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),
            NodeKind.StreamTransform when plan.ExecuteTransform is not null =>
                ExecuteTransformPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),
            NodeKind.Tap when plan.ExecuteTransform is not null =>
                ExecuteTransformPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),
            NodeKind.Branch when plan.ExecuteTransform is not null =>
                ExecuteTransformPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),
            NodeKind.Lookup when plan.ExecuteTransform is not null =>
                ExecuteTransformPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),
            NodeKind.Composite when plan.ExecuteTransform is not null =>
                ExecuteTransformPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),
            NodeKind.Batch when plan.ExecuteTransform is not null =>
                ExecuteTransformPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),
            NodeKind.Join when plan.ExecuteJoin is not null =>
                ExecuteJoinPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),
            NodeKind.Aggregate when plan.ExecuteAggregate is not null =>
                ExecuteAggregatePlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef),
            NodeKind.Sink when plan.ExecuteSink is not null =>
                ExecuteSinkPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef),
            NodeKind.CompositeInput when plan.ExecuteSource is not null =>
                ExecuteSourcePlanAsync(plan, graph, context, nodeOutputs),
            NodeKind.CompositeOutput when plan.ExecuteSink is not null =>
                ExecuteSinkPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef),
            _ => throw new NotSupportedException(ErrorMessages.NodeKindNotSupported(plan.Kind.ToString())),
        };
    }

    private async Task ExecuteSourcePlanAsync(NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        IDictionary<string, IDataStream?> nodeOutputs)
    {
        var output = await plan.ExecuteSource!(context, context.CancellationToken);

        if (graph.Lineage.ItemLevelLineageEnabled)
            output = lineageService.WrapSourceStream(output, plan.NodeId, context.PipelineId, context.PipelineName, graph.Lineage.LineageOptions);

        var counter = GetOrCreateCounter(context);
        output = dataStreamWrapperService.WrapWithCountingAndBranching(output, counter, context, graph, plan.NodeId);
        context.RegisterForDisposal(output);
        nodeOutputs[plan.NodeId] = output;
    }

    private async Task ExecuteTransformPlanAsync(NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataStream?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IReadOnlyDictionary<string, NodeDefinition> nodeDefinitionMap,
        NodeDefinition nodeDef,
        INode instance)
    {
        var input = await GetNodeInputAsync(plan.NodeId, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, context.CancellationToken);
        IDataStream transformed;

        if (graph.Lineage.ItemLevelLineageEnabled)
        {
            var adapter = nodeDef.LineageAdapter ?? throw new InvalidOperationException(ErrorMessages.LineageAdapterMissing(plan.NodeId));

            var (unwrapped, rewrap) = adapter(input, plan.NodeId, context.PipelineId, context.PipelineName,
                nodeDef.DeclaredCardinality ?? TransformCardinality.OneToOne, graph.Lineage.LineageOptions, context.CancellationToken);

            var transformTask = plan.ExecuteTransform!(unwrapped, context, context.CancellationToken);

            var raw = transformTask.IsCompletedSuccessfully
                ? transformTask.Result
                : await transformTask.ConfigureAwait(false);

            transformed = rewrap(raw);
        }
        else
        {
            var transformTask = plan.ExecuteTransform!(input, context, context.CancellationToken);

            transformed = transformTask.IsCompletedSuccessfully
                ? transformTask.Result
                : await transformTask.ConfigureAwait(false);
        }

        var counter = GetOrCreateCounter(context);
        transformed = dataStreamWrapperService.WrapWithCountingAndBranching(transformed, counter, context, graph, plan.NodeId);
        var disposable = transformed as IAsyncDisposable;
        context.RegisterForDisposal(disposable);

        nodeOutputs[plan.NodeId] = transformed;
    }

    private async Task ExecuteJoinPlanAsync(NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataStream?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IReadOnlyDictionary<string, NodeDefinition> nodeDefinitionMap,
        NodeDefinition nodeDef,
        INode instance)
    {
        // Gather inputs and merge using existing merge service (still reflection-free path)
        var joinInputPipes = inputLookup[plan.NodeId]
            .Select(edge => nodeOutputs[edge.SourceNodeId] ??
                            throw new InvalidOperationException(ErrorMessages.OutputNotFoundForSourceNode(edge.SourceNodeId)))
            .ToList();

        var merged = await pipeMergeService.MergeAsync(nodeDef, instance, joinInputPipes, context.CancellationToken);
        IDataStream output;

        if (graph.Lineage.ItemLevelLineageEnabled)
        {
            var (unwrappedInput, inputLineageContext) = lineageService.PrepareInputWithLineageContext(merged, context.CancellationToken);
            context.RegisterForDisposal(unwrappedInput as IAsyncDisposable ?? merged);

            var rawOutput = await plan.ExecuteJoin!([unwrappedInput], context, context.CancellationToken);
            var expectedOut = nodeDef.OutputType ?? rawOutput.GetDataType();

            if (rawOutput.GetDataType() != expectedOut)
                rawOutput = AdaptOutput(plan, rawOutput, expectedOut, $"JoinResult_{plan.NodeId}");

            output = lineageService.WrapNodeOutputFromInputLineage(
                rawOutput,
                inputLineageContext,
                plan.NodeId,
                context.PipelineId,
                context.PipelineName,
                graph.Lineage.LineageOptions,
                HopDecisionFlags.Joined,
                nodeDef.LineageMapperType,
                context.CancellationToken);
        }
        else
        {
            output = await plan.ExecuteJoin!([merged], context, context.CancellationToken);

            // Ensure typed output if delegate returned an untyped/object pipe
            if (nodeDef.OutputType is not null && output.GetDataType() != nodeDef.OutputType)
            {
                output = AdaptOutput(plan, output, nodeDef.OutputType, $"JoinResult_{plan.NodeId}");

                // If still mismatched after adaptation, capture diagnostic information
                if (output.GetDataType() != nodeDef.OutputType)
                {
                    var actualType = output.GetType();
                    var ifaceList = string.Join(",", actualType.GetInterfaces().Select(i => i.FullName));

                    throw new InvalidOperationException(
                        $"Join output type mismatch for node {plan.NodeId}. Expected {nodeDef.OutputType}, GetDataType={output.GetDataType()}, PipeType={actualType.FullName}, Ifaces={ifaceList}");
                }
            }
        }

        var counter = GetOrCreateCounter(context);
        output = dataStreamWrapperService.WrapWithCountingAndBranching(output, counter, context, graph, plan.NodeId);
        var disposable = output as IAsyncDisposable;
        context.RegisterForDisposal(disposable);

        nodeOutputs[plan.NodeId] = output;
    }

    private async Task ExecuteAggregatePlanAsync(NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataStream?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IReadOnlyDictionary<string, NodeDefinition> nodeDefinitionMap,
        NodeDefinition nodeDef)
    {
        var input = await GetNodeInputAsync(plan.NodeId, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, context.CancellationToken);
        IDataStream output;

        if (graph.Lineage.ItemLevelLineageEnabled)
        {
            var (unwrappedInput, inputLineageContext) = lineageService.PrepareInputWithLineageContext(input, context.CancellationToken);
            context.RegisterForDisposal(unwrappedInput as IAsyncDisposable ?? input);

            output = await plan.ExecuteAggregate!(unwrappedInput, context, context.CancellationToken);

            // Adapt aggregate output to declared OutputType prior to lineage wrapping so sinks get strongly typed pipes.
            if (nodeDef.OutputType is not null && output.GetDataType() != nodeDef.OutputType)
                output = AdaptOutput(plan, output, nodeDef.OutputType, $"AggregateResult_{plan.NodeId}");

            output = lineageService.WrapNodeOutputFromInputLineage(
                output,
                inputLineageContext,
                plan.NodeId,
                context.PipelineId,
                context.PipelineName,
                graph.Lineage.LineageOptions,
                HopDecisionFlags.Aggregated,
                nodeDef.LineageMapperType,
                context.CancellationToken);
        }
        else
        {
            output = await plan.ExecuteAggregate!(input, context, context.CancellationToken);

            // Ensure output pipe matches declared result type for downstream strict casting (e.g., SinkNode<T>).
            if (nodeDef.OutputType is not null && output.GetDataType() != nodeDef.OutputType)
            {
                output = AdaptOutput(plan, output, nodeDef.OutputType, $"AggregateResult_{plan.NodeId}");

                // If still mismatched after adaptation, capture diagnostic information
                if (output.GetDataType() != nodeDef.OutputType)
                {
                    var actualType = output.GetType();
                    var ifaceList = string.Join(",", actualType.GetInterfaces().Select(i => i.FullName));

                    throw new InvalidOperationException(
                        $"Aggregate output type mismatch for node {plan.NodeId}. Expected {nodeDef.OutputType}, GetDataType={output.GetDataType()}, PipeType={actualType.FullName}, Ifaces={ifaceList}");
                }
            }
        }

        var counter = GetOrCreateCounter(context);
        output = dataStreamWrapperService.WrapWithCountingAndBranching(output, counter, context, graph, plan.NodeId);
        context.RegisterForDisposal(output as IAsyncDisposable ?? input);
        nodeOutputs[plan.NodeId] = output;
    }

    private async Task ExecuteSinkPlanAsync(NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataStream?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IReadOnlyDictionary<string, NodeDefinition> nodeDefinitionMap,
        NodeDefinition nodeDef)
    {
        var input = await GetNodeInputAsync(plan.NodeId, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, context.CancellationToken);
        var effectiveInput = input;

        if (graph.Lineage.ItemLevelLineageEnabled)
        {
            var lineageUnwrap = nodeDef.SinkLineageUnwrap ??
                                throw new InvalidOperationException(ErrorMessages.SinkNodeLineageUnwrapMissing(plan.NodeId));

            effectiveInput = lineageUnwrap(input, context.LineageSink, plan.NodeId, context.PipelineId, context.PipelineName,
                graph.Lineage.LineageOptions, context.CancellationToken);
        }

        await plan.ExecuteSink!(effectiveInput, context, context.CancellationToken).ConfigureAwait(false);
        nodeOutputs[plan.NodeId] = null; // sinks produce no downstream pipe
    }

    private async Task<IDataStream> GetNodeInputAsync(string nodeId, ILookup<string, Edge> inputLookup, IDictionary<string, IDataStream?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances, IReadOnlyDictionary<string, NodeDefinition> nodeDefinitions, CancellationToken cancellationToken)
    {
        var inputEdges = inputLookup[nodeId].ToList();

        if (inputEdges.Count == 0)
            throw new InvalidOperationException(ErrorMessages.NodeMissingInputConnection(nodeId, "unknown", "unknown"));

        var inputPipes = inputEdges.Select(edge =>
        {
            if (nodeOutputs.TryGetValue(edge.SourceNodeId, out var inputData) && inputData is not null)
                return inputData;

            throw new InvalidOperationException(ErrorMessages.OutputNotFoundForSourceNode(edge.SourceNodeId) + $" when processing node '{nodeId}'.");
        }).ToList();

        if (inputPipes.Count == 1)
            return inputPipes[0];

        var nodeDef = nodeDefinitions[nodeId];
        var targetNode = nodeInstances[nodeId];
        return await pipeMergeService.MergeAsync(nodeDef, targetNode, inputPipes, cancellationToken);
    }

    private static IDataStream AdaptOutput(NodeExecutionPlan plan, IDataStream output, Type expectedType, string streamName)
    {
        if (plan.AdaptOutput is null)
        {
            throw new InvalidOperationException(
                $"Node '{plan.NodeId}' returned output type '{output.GetDataType()}', expected '{expectedType}', but no adaptation delegate is available.");
        }

        return plan.AdaptOutput(output, streamName);
    }

    private static StatsCounter GetOrCreateCounter(PipelineContext context)
    {
        context.ProcessedItemsCounter ??= new StatsCounter();
        return context.ProcessedItemsCounter;
    }
}
