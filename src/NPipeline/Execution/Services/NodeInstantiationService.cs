using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution.Plans;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Services;

/// <summary>
///     Service responsible for instantiating pipeline nodes and registering stateful nodes.
/// </summary>
public sealed class NodeInstantiationService : INodeInstantiationService
{
    /// <inheritdoc />
    public Dictionary<string, INode> InstantiateNodes(PipelineGraph graph, INodeFactory nodeFactory)
    {
        return graph.Nodes.ToDictionary(
            def => def.Id,
            def => nodeFactory.Create(def, graph)
        );
    }

    /// <inheritdoc />
    public void RegisterStatefulNodes(Dictionary<string, INode> nodeInstances, PipelineContext context)
    {
        var registry = context.StatefulRegistry;

        if (registry is null)
            return;

        foreach (var (nodeId, nodeInstance) in nodeInstances)
        {
            var instType = nodeInstance.GetType();

            // Detect IStatefulNode (generic or non-generic) by interface name to avoid direct reference
            var isStateful = instType.GetInterfaces().Any(i => i.Name.StartsWith("IStatefulNode", StringComparison.Ordinal));

            if (isStateful)
            {
                try
                {
                    registry.Register(nodeId, nodeInstance);
                }
                catch
                {
                    // Swallow registration failures - non-fatal
                }
            }
        }
    }

    /// <summary>
    ///     Builds per-node execution plans binding generic strategies to non-generic delegates.
    /// </summary>
    public Dictionary<string, NodeExecutionPlan> BuildPlans(PipelineGraph graph, IReadOnlyDictionary<string, INode> nodeInstances)
    {
        var plans = new Dictionary<string, NodeExecutionPlan>(graph.NodeDefinitionMap.Count);

        foreach (var (nodeId, def) in graph.NodeDefinitionMap)
        {
            var instance = nodeInstances[def.Id];

            switch (def.Kind)
            {
                case NodeKind.Source:
                    plans[def.Id] = new NodeExecutionPlan(
                        def.Id,
                        def.Kind,
                        def.InputType,
                        def.OutputType,
                        BuildSourceDelegate(def, instance));

                    break;
                case NodeKind.Transform when instance is ITransformNode transformNode:
                    plans[def.Id] = new NodeExecutionPlan(
                        def.Id,
                        def.Kind,
                        def.InputType,
                        def.OutputType,
                        ExecuteTransform: BuildTransformDelegate(def, transformNode));

                    break;
                case NodeKind.Join when instance is IJoinNode joinNode:
                    plans[def.Id] = new NodeExecutionPlan(
                        def.Id,
                        def.Kind,
                        def.InputType,
                        def.OutputType,
                        ExecuteJoin: BuildJoinDelegate(def, joinNode));

                    break;
                case NodeKind.Aggregate when instance is IAggregateNode aggregateNode:
                    plans[def.Id] = new NodeExecutionPlan(
                        def.Id,
                        def.Kind,
                        def.InputType,
                        def.OutputType,
                        ExecuteAggregate: BuildAggregateDelegate(def, aggregateNode));

                    break;
                case NodeKind.Sink:
                    plans[def.Id] = new NodeExecutionPlan(
                        def.Id,
                        def.Kind,
                        def.InputType,
                        def.OutputType,
                        ExecuteSink: BuildSinkDelegate(def, instance));

                    break;
                default:
                    // Fallback placeholder to avoid missing dictionary entries; executor will validate
                    plans[def.Id] = new NodeExecutionPlan(def.Id, def.Kind, def.InputType, def.OutputType);
                    break;
            }
        }

        return plans;
    }

    private static Func<IDataPipe, PipelineContext, CancellationToken, Task<IDataPipe>> BuildTransformDelegate(
        NodeDefinition def,
        ITransformNode transformNode)
    {
        var inType = def.InputType ?? throw new InvalidOperationException($"Missing InputType for transform node '{def.Id}'.");
        var outType = def.OutputType ?? throw new InvalidOperationException($"Missing OutputType for transform node '{def.Id}'.");

        var strategy = def.ExecutionStrategy ?? transformNode.ExecutionStrategy;
        var execMethod = typeof(IExecutionStrategy).GetMethod(nameof(IExecutionStrategy.ExecuteAsync))!;
        var closedExec = execMethod.MakeGenericMethod(inType, outType);

        // Parameters for delegate
        var pipeParam = Expression.Parameter(typeof(IDataPipe), "pipe");
        var ctxParam = Expression.Parameter(typeof(PipelineContext), "ctx");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        // Casts
        var typedInputInterface = typeof(IDataPipe<>).MakeGenericType(inType);
        var castInput = Expression.Convert(pipeParam, typedInputInterface);
        var typedNodeInterface = typeof(ITransformNode<,>).MakeGenericType(inType, outType);
        var nodeConst = Expression.Constant(transformNode);
        var castNode = Expression.Convert(nodeConst, typedNodeInterface);
        var strategyConst = Expression.Constant(strategy);

        var call = Expression.Call(strategyConst, closedExec, castInput, castNode, ctxParam, ctParam); // Task<IDataPipe<TOut>>

        var upcastMethod = typeof(NodeInstantiationService)
            .GetMethod("UpcastTask", BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(outType);

        var upcastCall = Expression.Call(upcastMethod, call); // Task<IDataPipe>

        var lambda = Expression.Lambda<Func<IDataPipe, PipelineContext, CancellationToken, Task<IDataPipe>>>(upcastCall, pipeParam, ctxParam,
            ctParam);

        return lambda.Compile();
    }

    // Helper used by expression tree to upcast Task<IDataPipe<T>> to Task<IDataPipe>
    private static async Task<IDataPipe> UpcastTask<T>(Task<IDataPipe<T>> task)
    {
        var result = await task.ConfigureAwait(false); // eliminate Task.Result (CA1849)
        return result;
    }

    private static Func<PipelineContext, CancellationToken, Task<IDataPipe>> BuildSourceDelegate(
        NodeDefinition def,
        INode instance)
    {
        var outputType = def.OutputType ?? throw new InvalidOperationException($"Missing OutputType for source node '{def.Id}'.");

        // Get the ISourceNode<TOut> interface for this output type
        var sourceInterface = typeof(ISourceNode<>).MakeGenericType(outputType);

        if (!sourceInterface.IsAssignableFrom(instance.GetType()))
        {
            throw new InvalidOperationException(
                $"Source node '{def.Id}' does not implement {sourceInterface.Name}.");
        }

        // Get the ExecuteAsync method
        var executeMethod = sourceInterface.GetMethod(
            nameof(ISourceNode<int>.Execute),
            BindingFlags.Public | BindingFlags.Instance,
            null,
            [typeof(PipelineContext), typeof(CancellationToken)],
            null) ?? throw new InvalidOperationException(
            $"Could not find ExecuteAsync method on {sourceInterface.Name}.");

        // Build a delegate that calls the method and upcasts the result
        var ctxParam = Expression.Parameter(typeof(PipelineContext), "ctx");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        var instanceExpr = Expression.Constant(instance);
        var typedInstanceExpr = Expression.Convert(instanceExpr, sourceInterface);

        var callExpr = Expression.Call(typedInstanceExpr, executeMethod, ctxParam, ctParam); // IDataPipe<TOut>

        // Upcast to non-generic IDataPipe so we can wrap in Task.FromResult
        var castExpr = Expression.Convert(callExpr, typeof(IDataPipe));

        var fromResultMethod = typeof(Task)
            .GetMethod(nameof(Task.FromResult), BindingFlags.Public | BindingFlags.Static)!
            .MakeGenericMethod(typeof(IDataPipe));

        var wrappedCall = Expression.Call(fromResultMethod, castExpr); // Task<IDataPipe>

        var lambda = Expression.Lambda<Func<PipelineContext, CancellationToken, Task<IDataPipe>>>(
            wrappedCall, ctxParam, ctParam);

        return lambda.Compile();
    }

    private static Func<IDataPipe, PipelineContext, CancellationToken, Task> BuildSinkDelegate(
        NodeDefinition def,
        INode instance)
    {
        var inputType = def.InputType ?? throw new InvalidOperationException($"Missing InputType for sink node '{def.Id}'.");

        // Get the ISinkNode<TIn> interface for this input type
        var sinkInterface = typeof(ISinkNode<>).MakeGenericType(inputType);

        if (!sinkInterface.IsAssignableFrom(instance.GetType()))
        {
            throw new InvalidOperationException(
                $"Sink node '{def.Id}' does not implement {sinkInterface.Name}.");
        }

        // Get the ExecuteAsync method
        var executeMethod = sinkInterface.GetMethod(
            nameof(ISinkNode<int>.ExecuteAsync),
            BindingFlags.Public | BindingFlags.Instance,
            null,
            [typeof(IDataPipe<>).MakeGenericType(inputType), typeof(PipelineContext), typeof(CancellationToken)],
            null) ?? throw new InvalidOperationException(
            $"Could not find ExecuteAsync method on {sinkInterface.Name}.");

        // Build a delegate that casts input and calls the method
        var inputParam = Expression.Parameter(typeof(IDataPipe), "input");
        var ctxParam = Expression.Parameter(typeof(PipelineContext), "ctx");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        var typedInputInterface = typeof(IDataPipe<>).MakeGenericType(inputType);
        var castInputExpr = Expression.Convert(inputParam, typedInputInterface);

        var instanceExpr = Expression.Constant(instance);
        var typedInstanceExpr = Expression.Convert(instanceExpr, sinkInterface);

        var callExpr = Expression.Call(typedInstanceExpr, executeMethod, castInputExpr, ctxParam, ctParam);

        var lambda = Expression.Lambda<Func<IDataPipe, PipelineContext, CancellationToken, Task>>(
            callExpr, inputParam, ctxParam, ctParam);

        return lambda.Compile();
    }

    private static Func<IEnumerable<IDataPipe>, PipelineContext, CancellationToken, Task<IDataPipe>> BuildJoinDelegate(
        NodeDefinition def,
        IJoinNode joinNode)
    {
        return async (inputs, ctx, ct) =>
        {
            var merged = inputs.First(); // merge already performed upstream
            var stream = merged.ToAsyncEnumerable(ct);
            var joined = await joinNode.ExecuteAsync(stream, ctx, ct);
            var outType = def.OutputType ?? typeof(object);

            var method = typeof(NodeInstantiationService)
                .GetMethod(nameof(CreateTypedJoinPipe), BindingFlags.NonPublic | BindingFlags.Static)!
                .MakeGenericMethod(outType);

            return (IDataPipe)method.Invoke(null, [joined, $"JoinResult_{def.Id}"])!;
        };
    }

    private static Func<IDataPipe, PipelineContext, CancellationToken, Task<IDataPipe>> BuildAggregateDelegate(
        NodeDefinition def,
        IAggregateNode aggregateNode)
    {
        return async (input, ctx, ct) =>
        {
            var stream = input.ToAsyncEnumerable(ct);
            var result = await aggregateNode.ExecuteAsync(stream, ct);

            if (result is IAsyncEnumerable<object?> asyncEnum)
                return new StreamingDataPipe<object?>(asyncEnum, $"AggregateResult_{def.Id}");

            var list = result is not null
                ? new List<object?> { result }
                : new List<object?>();

            return new InMemoryDataPipe<object?>(list, $"AggregateResult_{def.Id}");
        };
    }

    // Helper for typed join pipe creation
    private static StreamingDataPipe<TOut> CreateTypedJoinPipe<TOut>(IAsyncEnumerable<object?> joined, string streamName)
    {
        async IAsyncEnumerable<TOut> Cast([EnumeratorCancellation] CancellationToken ct = default)
        {
            await foreach (var item in joined.WithCancellation(ct))
            {
                if (item is null)
                {
                    if (default(TOut) is null)
                        yield return default!; // allow null for reference/nullable types

                    continue;
                }

                if (item is TOut t)
                    yield return t;
                else
                    yield return (TOut)item;
            }
        }

        return new StreamingDataPipe<TOut>(Cast(), streamName);
    }
}
