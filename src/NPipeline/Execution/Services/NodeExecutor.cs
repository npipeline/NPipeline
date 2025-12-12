using System.Reflection;
using System.Runtime.CompilerServices;
using NPipeline.Attributes.Lineage;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
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
    DataPipeWrapperService dataPipeWrapperService)
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
        IDictionary<string, IDataPipe?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IReadOnlyDictionary<string, NodeDefinition> nodeDefinitionMap)
    {
        var nodeDef = nodeDefinitionMap[plan.NodeId];
        var instance = nodeInstances[plan.NodeId];

        return plan.Kind switch
        {
            NodeKind.Source when plan.ExecuteSource is not null =>
                ExecuteSourcePlanAsync(plan, graph, context, nodeOutputs, nodeDef),
            NodeKind.Transform when plan.ExecuteTransform is not null =>
                ExecuteTransformPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),
            NodeKind.Join when plan.ExecuteJoin is not null =>
                ExecuteJoinPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),
            NodeKind.Aggregate when plan.ExecuteAggregate is not null =>
                ExecuteAggregatePlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),
            NodeKind.Sink when plan.ExecuteSink is not null =>
                ExecuteSinkPlanAsync(plan, graph, context, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, nodeDef, instance),
            _ => throw new NotSupportedException(ErrorMessages.NodeKindNotSupported(plan.Kind.ToString())),
        };
    }

    private async Task ExecuteSourcePlanAsync(NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        IDictionary<string, IDataPipe?> nodeOutputs,
        NodeDefinition nodeDef)
    {
        var output = await plan.ExecuteSource!(context, context.CancellationToken);

        if (graph.Lineage.ItemLevelLineageEnabled)
            output = lineageService.WrapSourceStream(output, plan.NodeId, graph.Lineage.LineageOptions);

        var counter = GetOrCreateCounter(context);
        output = dataPipeWrapperService.WrapWithCountingAndBranching(output, counter, context, graph, plan.NodeId);
        context.RegisterForDisposal(output);
        nodeOutputs[plan.NodeId] = output;
    }

    private async Task ExecuteTransformPlanAsync(NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataPipe?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IReadOnlyDictionary<string, NodeDefinition> nodeDefinitionMap,
        NodeDefinition nodeDef,
        INode instance)
    {
        var input = await GetNodeInputAsync(plan.NodeId, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, context.CancellationToken);
        IDataPipe transformed;

        if (graph.Lineage.ItemLevelLineageEnabled)
        {
            var adapter = nodeDef.LineageAdapter ?? throw new InvalidOperationException(ErrorMessages.LineageAdapterMissing(plan.NodeId));

            var (unwrapped, rewrap) = adapter(input, plan.NodeId, nodeDef.DeclaredCardinality ?? TransformCardinality.OneToOne, graph.Lineage.LineageOptions,
                context.CancellationToken);

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
        transformed = dataPipeWrapperService.WrapWithCountingAndBranching(transformed, counter, context, graph, plan.NodeId);
        var disposable = transformed as IAsyncDisposable;
        context.RegisterForDisposal(disposable);

        nodeOutputs[plan.NodeId] = transformed;
    }

    private async Task ExecuteJoinPlanAsync(NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataPipe?> nodeOutputs,
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
        IDataPipe output;

        if (graph.Lineage.ItemLevelLineageEnabled)
        {
            var unwrapped = lineageService.UnwrapLineageStream(merged.ToAsyncEnumerable(context.CancellationToken), context.CancellationToken);
#pragma warning disable CA2000 // Temp pipe registered for disposal immediately
            var tempPipe = new StreamingDataPipe<object>(unwrapped, $"JoinInput_{plan.NodeId}");
#pragma warning restore CA2000
            context.RegisterForDisposal(tempPipe);
            var rawOutput = await plan.ExecuteJoin!([tempPipe], context, context.CancellationToken);
            var expectedOut = nodeDef.OutputType ?? rawOutput.GetDataType();

            if (rawOutput.GetDataType() != expectedOut)
            {
                var adaptMethod = typeof(NodeExecutor).GetMethod(nameof(AdaptJoinOutput), BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(expectedOut);

                rawOutput = (IDataPipe)adaptMethod.Invoke(null, [rawOutput, $"JoinResult_{plan.NodeId}"])!;
            }

            output = lineageService.WrapNodeOutput(rawOutput, plan.NodeId, graph.Lineage.LineageOptions, HopDecisionFlags.Joined, context.CancellationToken);
        }
        else
        {
            output = await plan.ExecuteJoin!([merged], context, context.CancellationToken);

            // Ensure typed output if delegate returned an untyped/object pipe
            if (nodeDef.OutputType is not null && output.GetDataType() != nodeDef.OutputType)
            {
                var adaptMethod = typeof(NodeExecutor).GetMethod(nameof(AdaptJoinOutput), BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(nodeDef.OutputType);

                output = (IDataPipe)adaptMethod.Invoke(null, [output, $"JoinResult_{plan.NodeId}"])!;

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
        output = dataPipeWrapperService.WrapWithCountingAndBranching(output, counter, context, graph, plan.NodeId);
        var disposable = output as IAsyncDisposable;
        context.RegisterForDisposal(disposable);

        nodeOutputs[plan.NodeId] = output;
    }

    private async Task ExecuteAggregatePlanAsync(NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataPipe?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IReadOnlyDictionary<string, NodeDefinition> nodeDefinitionMap,
        NodeDefinition nodeDef,
        INode instance)
    {
        var input = await GetNodeInputAsync(plan.NodeId, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, context.CancellationToken);
        IDataPipe output;

        if (graph.Lineage.ItemLevelLineageEnabled)
        {
            var unwrapped = lineageService.UnwrapLineageStream(input.ToAsyncEnumerable(), context.CancellationToken);
#pragma warning disable CA2000 // Temp pipe registered for disposal immediately
            var tempPipe = new StreamingDataPipe<object?>(unwrapped, $"AggInput_{plan.NodeId}");
#pragma warning restore CA2000
            context.RegisterForDisposal(tempPipe);
            output = await plan.ExecuteAggregate!(tempPipe, context, context.CancellationToken);

            // Adapt aggregate output to declared OutputType prior to lineage wrapping so sinks get strongly typed pipes.
            if (nodeDef.OutputType is not null && output.GetDataType() != nodeDef.OutputType)
            {
                var adaptMethod = typeof(NodeExecutor).GetMethod(nameof(AdaptJoinOutput), BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(nodeDef.OutputType);

                output = (IDataPipe)adaptMethod.Invoke(null, [output, $"AggregateResult_{plan.NodeId}"])!;
            }

            output = lineageService.WrapNodeOutput(output, plan.NodeId, graph.Lineage.LineageOptions, HopDecisionFlags.Aggregated, context.CancellationToken);
        }
        else
        {
            output = await plan.ExecuteAggregate!(input, context, context.CancellationToken);

            // Ensure output pipe matches declared result type for downstream strict casting (e.g., SinkNode<T>).
            if (nodeDef.OutputType is not null && output.GetDataType() != nodeDef.OutputType)
            {
                var adaptMethod = typeof(NodeExecutor).GetMethod(nameof(AdaptJoinOutput), BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(nodeDef.OutputType);

                output = (IDataPipe)adaptMethod.Invoke(null, [output, $"AggregateResult_{plan.NodeId}"])!;

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
        output = dataPipeWrapperService.WrapWithCountingAndBranching(output, counter, context, graph, plan.NodeId);
        context.RegisterForDisposal(output as IAsyncDisposable ?? input);
        nodeOutputs[plan.NodeId] = output;
    }

    private async Task ExecuteSinkPlanAsync(NodeExecutionPlan plan,
        PipelineGraph graph,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataPipe?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances,
        IReadOnlyDictionary<string, NodeDefinition> nodeDefinitionMap,
        NodeDefinition nodeDef,
        INode instance)
    {
        var input = await GetNodeInputAsync(plan.NodeId, inputLookup, nodeOutputs, nodeInstances, nodeDefinitionMap, context.CancellationToken);
        var effectiveInput = input;

        if (graph.Lineage.ItemLevelLineageEnabled)
        {
            var lineageUnwrap = nodeDef.SinkLineageUnwrap ??
                                throw new InvalidOperationException(ErrorMessages.SinkNodeLineageUnwrapMissing(plan.NodeId));

            effectiveInput = lineageUnwrap(input, context.Items.TryGetValue(PipelineContextKeys.LineageSink, out var ls)
                ? (ILineageSink)ls
                : null, plan.NodeId, graph.Lineage.LineageOptions, context.CancellationToken);
        }

        await plan.ExecuteSink!(effectiveInput, context, context.CancellationToken).ConfigureAwait(false);
        nodeOutputs[plan.NodeId] = null; // sinks produce no downstream pipe
    }

    private async Task<IDataPipe> GetNodeInputAsync(string nodeId, ILookup<string, Edge> inputLookup, IDictionary<string, IDataPipe?> nodeOutputs,
        IReadOnlyDictionary<string, INode> nodeInstances, IReadOnlyDictionary<string, NodeDefinition> nodeDefinitions, CancellationToken cancellationToken)
    {
        var inputEdges = inputLookup[nodeId].ToList();

        if (inputEdges.Count == 0)
            throw new InvalidOperationException(ErrorMessages.NodeMissingInputConnection(nodeId, "unknown", "unknown"));

        var inputPipes = inputEdges.Select(edge =>
        {
            if (!nodeOutputs.TryGetValue(edge.SourceNodeId, out var inputData) || inputData is null)
                throw new InvalidOperationException(ErrorMessages.OutputNotFoundForSourceNode(edge.SourceNodeId) + $" when processing node '{nodeId}'.");

            return inputData;
        }).ToList();

        if (inputPipes.Count == 1)
            return inputPipes[0];

        var nodeDef = nodeDefinitions[nodeId];
        var targetNode = nodeInstances[nodeId];
        return await pipeMergeService.MergeAsync(nodeDef, targetNode, inputPipes, cancellationToken);
    }

    private static IDataPipe CreateTypedJoinPipe(NodeDefinition def, IAsyncEnumerable<object?> source, string streamName)
    {
        var outType = def.OutputType ?? typeof(object);

        var method =
            typeof(NodeExecutor).GetMethod(nameof(CreateTypedJoinPipeGeneric), BindingFlags.NonPublic | BindingFlags.Static)!.MakeGenericMethod(outType);

        return (IDataPipe)method.Invoke(null, [source, streamName])!;
    }

    private static StreamingDataPipe<TOut> CreateTypedJoinPipeGeneric<TOut>(IAsyncEnumerable<object?> source, string streamName)
    {
        async IAsyncEnumerable<TOut> CastStream([EnumeratorCancellation] CancellationToken ct = default)
        {
            await foreach (var item in source.WithCancellation(ct))

                // Consistent null handling: always yield a value for null items
                // For reference types and nullable value types, yield null
                // For non-nullable value types, yield default
            {
                if (item is null)
                    yield return default!;
                else
                {
                    if (item is TOut typed)
                        yield return typed;
                    else

                        // Attempt direct cast (will throw immediately, surfacing misconfiguration early)
                        yield return (TOut)item;
                }
            }
        }

        return new StreamingDataPipe<TOut>(CastStream(), streamName);
    }

    private static StreamingDataPipe<TOut> AdaptJoinOutput<TOut>(IDataPipe untyped, string streamName)
    {
        // If already correct type just return
        if (untyped is IDataPipe<TOut> typedExisting)
        {
            // If already correct type but not a StreamingDataPipe<TOut>, adapt by wrapping enumeration.
            if (typedExisting is StreamingDataPipe<TOut> streaming)
                return streaming;

            async IAsyncEnumerable<TOut> Passthrough([EnumeratorCancellation] CancellationToken ct = default)
            {
                await foreach (var obj in typedExisting.ToAsyncEnumerable(ct).WithCancellation(ct))
                {
                    if (obj is TOut t)
                        yield return t;
                    else
                        yield return (TOut)obj!;
                }
            }

            return new StreamingDataPipe<TOut>(Passthrough(), streamName);
        }

        async IAsyncEnumerable<TOut> Cast([EnumeratorCancellation] CancellationToken ct = default)
        {
            await foreach (var obj in untyped.ToAsyncEnumerable(ct).WithCancellation(ct))

                // Consistent null handling: always yield a value for null items
                // For reference types and nullable value types, yield null
                // For non-nullable value types, yield default
            {
                if (obj is null)
                    yield return default!;
                else
                {
                    if (obj is TOut t)
                        yield return t;
                    else
                        yield return (TOut)obj!;
                }
            }
        }

        return new StreamingDataPipe<TOut>(Cast(), streamName);
    }

    private static StatsCounter GetOrCreateCounter(PipelineContext context)
    {
        if (!context.Items.TryGetValue(PipelineContextKeys.TotalProcessedItems, out var statsObj) || statsObj is not StatsCounter counter)
        {
            counter = new StatsCounter();
            context.Items[PipelineContextKeys.TotalProcessedItems] = counter;
        }

        return counter;
    }
}
