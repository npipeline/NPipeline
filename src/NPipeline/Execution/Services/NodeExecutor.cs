using System.Diagnostics;
using NPipeline.Attributes.Lineage;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.DataFlow.Routing;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Lineage;
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
    ILineage lineage,
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
            NodeKind.Source or NodeKind.CompositeInput when plan.ExecuteSource is not null =>
                ExecuteSourcePlanAsync(plan, graph, context, nodeOutputs, instance),

            NodeKind.Transform or NodeKind.StreamTransform or NodeKind.Tap or NodeKind.Branch or NodeKind.Route
                or NodeKind.Lookup or NodeKind.Composite or NodeKind.Batch when plan.ExecuteTransform is not null =>
                ExecuteTransformPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),

            NodeKind.Join when plan.ExecuteJoin is not null =>
                ExecuteJoinPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),

            NodeKind.Aggregate when plan.ExecuteAggregate is not null =>
                ExecuteAggregatePlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),

            NodeKind.Sink or NodeKind.CompositeOutput when plan.ExecuteSink is not null =>
                ExecuteSinkPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),

            _ => throw new NotSupportedException(ErrorMessages.NodeKindNotSupported(plan.Kind.ToString())),
        };
    }

    private async Task ExecuteSourcePlanAsync(NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        IDictionary<string, IDataStream?> nodeOutputs,
        INode instance)
    {
        var output = await plan.ExecuteSource!(instance, context, context.CancellationToken).ConfigureAwait(false);

        if (graph.Lineage.ItemLevelLineageEnabled)
            output = lineage.WrapSourceStream(output, plan.NodeId, context.RunIdentity.PipelineId, context.RunIdentity.PipelineName,
                graph.Lineage.LineageOptions);

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
        var input = await GetNodeInputAsync(plan.NodeId, graph, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, context.CancellationToken)
            .ConfigureAwait(false);

        var strategy = NodeExecutionStrategyResolver.Resolve(nodeDef, instance);
        IDataStream transformed;

        if (graph.Lineage.ItemLevelLineageEnabled)
        {
            var adapter = nodeDef.LineageAdapter ?? throw new InvalidOperationException(ErrorMessages.LineageAdapterMissing(plan.NodeId));

            // Started here, not by the adapter, because only the executor knows the strategy the node runs under.
            LineageNodeOutcomeRegistry.BeginNode(context.RunIdentity.PipelineId, plan.NodeId,
                LineageProvenanceSupport.Reports(strategy, instance), context.Lineage.LineageSink);

            var (unwrapped, rewrap) = adapter(input, plan.NodeId, context.RunIdentity.PipelineId, context.RunIdentity.PipelineName,
                nodeDef.DeclaredCardinality ?? TransformCardinality.OneToOne, graph.Lineage.LineageOptions, context.CancellationToken);

            var transformTask = plan.ExecuteTransform!(instance, strategy, unwrapped, context, context.CancellationToken);

            var raw = transformTask.IsCompletedSuccessfully
                ? transformTask.Result
                : await transformTask.ConfigureAwait(false);

            transformed = rewrap(raw);
        }
        else
        {
            var transformTask = plan.ExecuteTransform!(instance, strategy, input, context, context.CancellationToken);

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
        var joinInputPipes = new List<IDataStream>();
        foreach (var edge in inputLookup[plan.NodeId])
            joinInputPipes.Add(TrackInputFlow(context, plan.NodeId, ResolveEdgeInput(edge, nodeOutputs, plan.NodeId)));

        var merged = await pipeMergeService
            .MergeAsync(nodeDef, instance, joinInputPipes, ExecutionAnnotationsService.GetMergeCapacity(graph, plan.NodeId), context.CancellationToken)
            .ConfigureAwait(false);
        IDataStream output;

        if (graph.Lineage.ItemLevelLineageEnabled)
        {
            var (unwrappedInput, inputLineageContext) = lineage.PrepareInputWithLineageContext(merged, context.CancellationToken);
            context.RegisterForDisposal(unwrappedInput as IAsyncDisposable ?? merged);

            var rawOutput = await plan.ExecuteJoin!(instance, [unwrappedInput], context, context.CancellationToken).ConfigureAwait(false);
            var expectedOut = nodeDef.OutputType ?? rawOutput.GetDataType();

            if (rawOutput.GetDataType() != expectedOut)
                rawOutput = AdaptOutput(plan, rawOutput, expectedOut, $"JoinResult_{plan.NodeId}");

            output = lineage.WrapNodeOutputFromInputLineage(
                rawOutput,
                inputLineageContext,
                plan.NodeId,
                context.RunIdentity.PipelineId,
                context.RunIdentity.PipelineName,
                graph.Lineage.LineageOptions,
                LineageOutcomeReason.Joined,
                nodeDef.LineageMapperType,
                context.CancellationToken);
        }
        else
        {
            output = await plan.ExecuteJoin!(instance, [merged], context, context.CancellationToken).ConfigureAwait(false);

            // Ensure typed output if delegate returned an untyped/object pipe
            if (nodeDef.OutputType is not null && output.GetDataType() != nodeDef.OutputType)
            {
                output = AdaptOutput(plan, output, nodeDef.OutputType, $"JoinResult_{plan.NodeId}");

                if (output.GetDataType() != nodeDef.OutputType)
                {
                    throw new InvalidOperationException(
                        ErrorMessages.NodeOutputTypeMismatch(plan.NodeId, "Join", nodeDef.OutputType, output.GetDataType()));
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
        NodeDefinition nodeDef,
        INode instance)
    {
        var input = TrackInputFlow(context, plan.NodeId,
            await GetNodeInputAsync(plan.NodeId, graph, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, context.CancellationToken)
                .ConfigureAwait(false));

        IDataStream output;

        if (graph.Lineage.ItemLevelLineageEnabled)
        {
            var (unwrappedInput, inputLineageContext) = lineage.PrepareInputWithLineageContext(input, context.CancellationToken);
            context.RegisterForDisposal(unwrappedInput as IAsyncDisposable ?? input);

            output = await plan.ExecuteAggregate!(instance, unwrappedInput, context, context.CancellationToken).ConfigureAwait(false);

            // Adapt aggregate output to declared OutputType prior to lineage wrapping so sinks get strongly typed pipes.
            if (nodeDef.OutputType is not null && output.GetDataType() != nodeDef.OutputType)
                output = AdaptOutput(plan, output, nodeDef.OutputType, $"AggregateResult_{plan.NodeId}");

            output = lineage.WrapNodeOutputFromInputLineage(
                output,
                inputLineageContext,
                plan.NodeId,
                context.RunIdentity.PipelineId,
                context.RunIdentity.PipelineName,
                graph.Lineage.LineageOptions,
                LineageOutcomeReason.Aggregated,
                nodeDef.LineageMapperType,
                context.CancellationToken);
        }
        else
        {
            output = await plan.ExecuteAggregate!(instance, input, context, context.CancellationToken).ConfigureAwait(false);

            // Ensure output pipe matches declared result type for downstream strict casting (e.g., SinkNode<T>).
            if (nodeDef.OutputType is not null && output.GetDataType() != nodeDef.OutputType)
            {
                output = AdaptOutput(plan, output, nodeDef.OutputType, $"AggregateResult_{plan.NodeId}");

                if (output.GetDataType() != nodeDef.OutputType)
                {
                    throw new InvalidOperationException(
                        ErrorMessages.NodeOutputTypeMismatch(plan.NodeId, "Aggregate", nodeDef.OutputType, output.GetDataType()));
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
        NodeDefinition nodeDef,
        INode instance)
    {
        var input = TrackInputFlow(context, plan.NodeId,
            await GetNodeInputAsync(plan.NodeId, graph, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, context.CancellationToken)
                .ConfigureAwait(false));

        var effectiveInput = input;

        if (graph.Lineage.ItemLevelLineageEnabled)
        {
            var lineageUnwrap = nodeDef.SinkLineageUnwrap ??
                                throw new InvalidOperationException(ErrorMessages.SinkNodeLineageUnwrapMissing(plan.NodeId));

            effectiveInput = lineageUnwrap(input, context.Lineage.LineageSink, plan.NodeId, context.RunIdentity.PipelineId, context.RunIdentity.PipelineName,
                graph.Lineage.LineageOptions, context.CancellationToken);
        }

        using var observabilityScope = context.NodeEnvironment.NodeExecutionScopeRegistry.BeginNodeScope(plan.NodeId);
        effectiveInput = NodeTimingDataStreamWrapper.WrapInputWait(effectiveInput, observabilityScope);

        var before = observabilityScope.GetTimingBreakdown();
        var sinkStart = Stopwatch.GetTimestamp();

        try
        {
            await plan.ExecuteSink!(instance, effectiveInput, context, context.CancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            observabilityScope.RecordFailure(ex);
            throw;
        }
        finally
        {
            var sinkElapsed = Stopwatch.GetElapsedTime(sinkStart);

            var after = observabilityScope.GetTimingBreakdown();
            var inputWaitDelta = after.InputWaitDuration - before.InputWaitDuration;
            var outputBlockDelta = after.OutputBlockDuration - before.OutputBlockDuration;
            var exclusiveWork = sinkElapsed - inputWaitDelta - outputBlockDelta;

            if (exclusiveWork > TimeSpan.Zero)
                observabilityScope.AddWork(exclusiveWork);
        }

        nodeOutputs[plan.NodeId] = null; // sinks produce no downstream pipe
    }

    /// <summary>
    ///     Wraps a node's input so that node retry can tell whether the node consumed any, when node retry asked.
    /// </summary>
    /// <remarks>
    ///     Only nodes that drain their input while executing need this: sinks, aggregates, and joins. A transform
    ///     returns its output stream without reading its input, so its execution never consumes input.
    /// </remarks>
    private static IDataStream TrackInputFlow(PipelineContext context, string nodeId, IDataStream input) =>
        context.ExecutionConfiguration.GetInputFlow(nodeId) is { } flow
            ? InputFlowTracking.Wrap(input, flow)
            : input;

    private async Task<IDataStream> GetNodeInputAsync(string nodeId, PipelineGraph graph, ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataStream?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances, IReadOnlyDictionary<string, NodeDefinition> nodeDefinitions, CancellationToken cancellationToken)
    {
        var inputEdges = inputLookup[nodeId].ToList();

        if (inputEdges.Count == 0)
            throw new InvalidOperationException(ErrorMessages.NodeMissingInputConnection(nodeId, "unknown", "unknown"));

        var inputPipes = new List<IDataStream>(inputEdges.Count);
        foreach (var edge in inputEdges)
            inputPipes.Add(ResolveEdgeInput(edge, nodeOutputs, nodeId));

        var nodeDef = nodeDefinitions[nodeId];

        if (nodeDef.Kind != NodeKind.Join)
            ValidateRuntimeInputContract(graph, nodeId, inputPipes);

        if (inputPipes.Count == 1)
            return inputPipes[0];

        var targetNode = nodeInstances[nodeId];
        return await pipeMergeService
            .MergeAsync(nodeDef, targetNode, inputPipes, ExecutionAnnotationsService.GetMergeCapacity(graph, nodeId), cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    ///     Resolves the input a specific incoming edge delivers, handing a Route node's output the branch this edge
    ///     represents instead of the next unclaimed subscriber channel.
    /// </summary>
    private static IDataStream ResolveEdgeInput(Edge edge, IDictionary<string, IDataStream?> nodeOutputs, string nodeId)
    {
        if (!nodeOutputs.TryGetValue(edge.SourceNodeId, out var upstream) || upstream is null)
            throw new InvalidOperationException(ErrorMessages.OutputNotFoundForSourceNode(edge.SourceNodeId) + $" when processing node '{nodeId}'.");

        return upstream is IEdgeRoutedDataStream routed ? routed.GetEdgeView(edge) : upstream;
    }

    private static void ValidateRuntimeInputContract(PipelineGraph graph, string nodeId, IReadOnlyList<IDataStream> inputPipes)
    {
        if (graph.ExecutionOptions.NodeExecutionAnnotations?.TryGetValue(
                ExecutionAnnotationKeys.RuntimeStreamContractForNode(nodeId),
                out var contractObj) != true ||
            contractObj is not RuntimeNodeStreamContract { EffectiveInputItemType: { } expectedType })
            return;

        foreach (var pipe in inputPipes)
        {
            var actualType = pipe.GetDataType();

            // Allow interface/base-type compatibility (e.g., IReadOnlyCollection<T> for IEnumerable<T> inputs).
            if (!expectedType.IsAssignableFrom(actualType))
                throw new InvalidOperationException(ErrorMessages.InputStreamContractMismatch(nodeId, expectedType, actualType));
        }
    }

    private static IDataStream AdaptOutput(NodeExecutionPlan plan, IDataStream output, Type expectedType, string streamName)
    {
        if (plan.AdaptOutput is null)
        {
            throw new InvalidOperationException(
                ErrorMessages.OutputAdaptationUnavailable(plan.NodeId, expectedType, output.GetDataType()));
        }

        return plan.AdaptOutput(output, streamName);
    }

    private static StatsCounter GetOrCreateCounter(PipelineContext context)
    {
        context.Observability.ProcessedItemsCounter ??= new StatsCounter();
        return context.Observability.ProcessedItemsCounter;
    }
}
