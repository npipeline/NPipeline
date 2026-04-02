using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution.Plans;
using NPipeline.Execution.Pooling;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Services;

/// <summary>
///     Service responsible for instantiating pipeline nodes and registering stateful nodes.
/// </summary>
public sealed class NodeInstantiationService : INodeInstantiationService
{
    private static readonly MethodInfo AdaptOutputPipeGenericMethod = typeof(NodeInstantiationService)
        .GetMethod(nameof(AdaptOutputPipe), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo UpcastTaskGenericMethod = typeof(NodeInstantiationService)
        .GetMethod(nameof(UpcastTask), BindingFlags.NonPublic | BindingFlags.Static)!;

    /// <inheritdoc />
    public Dictionary<string, INode> InstantiateNodes(PipelineGraph graph, INodeFactory nodeFactory)
    {
        var nodeInstances = PipelineObjectPool.RentNodeDictionary(graph.Nodes.Length);

        try
        {
            foreach (var def in graph.Nodes)
            {
                nodeInstances.Add(def.Id, nodeFactory.Create(def, graph));
            }

            return nodeInstances;
        }
        catch
        {
            PipelineObjectPool.Return(nodeInstances);
            throw;
        }
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

            plans[nodeId] = def.Kind switch
            {
                NodeKind.Source => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    BuildSourceDelegate(def, instance)),

                NodeKind.Transform when instance is ITransformNode transformNode => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    ExecuteTransform: BuildTransformDelegate(def, transformNode)),

                NodeKind.Transform when instance is IStreamTransformNode streamTransformNode => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    ExecuteTransform: BuildStreamTransformDelegate(def, streamTransformNode)),

                NodeKind.Tap when instance is ITransformNode tapNode => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    ExecuteTransform: BuildTransformDelegate(def, tapNode)),

                NodeKind.Branch when instance is ITransformNode branchNode => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    ExecuteTransform: BuildTransformDelegate(def, branchNode)),

                NodeKind.Lookup when instance is ITransformNode lookupNode => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    ExecuteTransform: BuildTransformDelegate(def, lookupNode)),

                NodeKind.Composite when instance is ITransformNode compositeNode => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    ExecuteTransform: BuildTransformDelegate(def, compositeNode)),

                NodeKind.StreamTransform when instance is IStreamTransformNode streamTransformNode => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    ExecuteTransform: BuildStreamTransformDelegate(def, streamTransformNode)),

                NodeKind.Batch when instance is IStreamTransformNode batchNode => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    ExecuteTransform: BuildStreamTransformDelegate(def, batchNode)),

                NodeKind.Join when instance is IJoinNode joinNode => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    ExecuteJoin: BuildJoinDelegate(def, joinNode),
                    AdaptOutput: BuildOutputAdapter(def.OutputType)),

                NodeKind.Aggregate when instance is IAggregateNode aggregateNode => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    ExecuteAggregate: BuildAggregateDelegate(def, aggregateNode),
                    AdaptOutput: BuildOutputAdapter(def.OutputType)),

                NodeKind.Sink => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    ExecuteSink: BuildSinkDelegate(def, instance)),

                NodeKind.CompositeInput => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    BuildSourceDelegate(def, instance)),

                NodeKind.CompositeOutput => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    ExecuteSink: BuildSinkDelegate(def, instance)),

                _ => new NodeExecutionPlan(def.Id, def.Kind, def.InputType, def.OutputType),
            };
        }

        return plans;
    }

    private static Func<IDataStream, PipelineContext, CancellationToken, Task<IDataStream>> BuildTransformDelegate(
        NodeDefinition def,
        ITransformNode transformNode)
    {
        var inType = def.InputType ?? throw new InvalidOperationException($"Missing InputType for transform node '{def.Id}'.");
        var outType = def.OutputType ?? throw new InvalidOperationException($"Missing OutputType for transform node '{def.Id}'.");
        var strategy = def.ExecutionStrategy ?? transformNode.ExecutionStrategy;

        return BuildStrategyDelegate(
            inType,
            outType,
            strategy,
            typeof(IExecutionStrategy),
            nameof(IExecutionStrategy.ExecuteAsync),
            transformNode,
            typeof(ITransformNode<,>));
    }

    private static Func<IDataStream, PipelineContext, CancellationToken, Task<IDataStream>> BuildStreamTransformDelegate(
        NodeDefinition def,
        IStreamTransformNode streamTransformNode)
    {
        var inType = def.InputType ?? throw new InvalidOperationException($"Missing InputType for stream transform node '{def.Id}'.");
        var outType = def.OutputType ?? throw new InvalidOperationException($"Missing OutputType for stream transform node '{def.Id}'.");

        // Check if the execution strategy implements IStreamExecutionStrategy
        var streamStrategy = def.ExecutionStrategy as IStreamExecutionStrategy ?? streamTransformNode.ExecutionStrategy as IStreamExecutionStrategy;

        if (streamStrategy is null)
        {
            // Use the original IExecutionStrategy for ITransformNode compatibility
            var strategy = def.ExecutionStrategy ?? streamTransformNode.ExecutionStrategy;

            return BuildStrategyDelegate(
                inType,
                outType,
                strategy,
                typeof(IExecutionStrategy),
                nameof(IExecutionStrategy.ExecuteAsync),
                streamTransformNode,
                typeof(ITransformNode<,>));
        }

        // Use the stream execution strategy when supported.
        return BuildStrategyDelegate(
            inType,
            outType,
            streamStrategy,
            typeof(IStreamExecutionStrategy),
            nameof(IStreamExecutionStrategy.ExecuteAsync),
            streamTransformNode,
            typeof(IStreamTransformNode<,>));
    }

    private static Func<IDataStream, PipelineContext, CancellationToken, Task<IDataStream>> BuildStrategyDelegate(
        Type inType,
        Type outType,
        object strategy,
        Type strategyInterface,
        string executeMethodName,
        INode nodeInstance,
        Type nodeInterfaceDefinition)
    {
        var execMethod = strategyInterface.GetMethod(executeMethodName) ??
                         throw new InvalidOperationException($"Could not find '{executeMethodName}' on {strategyInterface.Name}.");

        var closedExec = execMethod.MakeGenericMethod(inType, outType);

        var pipeParam = Expression.Parameter(typeof(IDataStream), "pipe");
        var ctxParam = Expression.Parameter(typeof(PipelineContext), "ctx");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        var typedInputInterface = typeof(IDataStream<>).MakeGenericType(inType);
        var castInput = Expression.Convert(pipeParam, typedInputInterface);
        var typedNodeInterface = nodeInterfaceDefinition.MakeGenericType(inType, outType);
        var castNode = Expression.Convert(Expression.Constant(nodeInstance), typedNodeInterface);
        var strategyConst = Expression.Constant(strategy);

        var call = Expression.Call(strategyConst, closedExec, castInput, castNode, ctxParam, ctParam);
        var upcastCall = Expression.Call(UpcastTaskGenericMethod.MakeGenericMethod(outType), call);

        return Expression.Lambda<Func<IDataStream, PipelineContext, CancellationToken, Task<IDataStream>>>(
            upcastCall,
            pipeParam,
            ctxParam,
            ctParam).Compile();
    }

    // Helper used by expression tree to upcast Task<IDataStream<T>> to Task<IDataStream>
    private static async Task<IDataStream> UpcastTask<T>(Task<IDataStream<T>> task)
    {
        var result = await task.ConfigureAwait(false); // eliminate Task.Result (CA1849)
        return result;
    }

    private static Func<PipelineContext, CancellationToken, Task<IDataStream>> BuildSourceDelegate(
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

        // Get the OpenStream method
        var executeMethod = sourceInterface.GetMethod(
            nameof(ISourceNode<int>.OpenStream),
            BindingFlags.Public | BindingFlags.Instance,
            null,
            [typeof(PipelineContext), typeof(CancellationToken)],
            null) ?? throw new InvalidOperationException(
            $"Could not find OpenStream method on {sourceInterface.Name}.");

        // Build a delegate that calls the method and upcasts the result
        var ctxParam = Expression.Parameter(typeof(PipelineContext), "ctx");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        var instanceExpr = Expression.Constant(instance);
        var typedInstanceExpr = Expression.Convert(instanceExpr, sourceInterface);

        var callExpr = Expression.Call(typedInstanceExpr, executeMethod, ctxParam, ctParam); // IDataStream<TOut>

        // Upcast to non-generic IDataStream so we can wrap in Task.FromResult
        var castExpr = Expression.Convert(callExpr, typeof(IDataStream));

        var fromResultMethod = typeof(Task)
            .GetMethod(nameof(Task.FromResult), BindingFlags.Public | BindingFlags.Static)!
            .MakeGenericMethod(typeof(IDataStream));

        var wrappedCall = Expression.Call(fromResultMethod, castExpr); // Task<IDataStream>

        var lambda = Expression.Lambda<Func<PipelineContext, CancellationToken, Task<IDataStream>>>(
            wrappedCall, ctxParam, ctParam);

        return lambda.Compile();
    }

    private static Func<IDataStream, PipelineContext, CancellationToken, Task> BuildSinkDelegate(
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

        // Get the ConsumeAsync method
        var executeMethod = sinkInterface.GetMethod(
            nameof(ISinkNode<int>.ConsumeAsync),
            BindingFlags.Public | BindingFlags.Instance,
            null,
            [typeof(IDataStream<>).MakeGenericType(inputType), typeof(PipelineContext), typeof(CancellationToken)],
            null) ?? throw new InvalidOperationException(
            $"Could not find ConsumeAsync method on {sinkInterface.Name}.");

        // Build a delegate that casts input and calls the method
        var inputParam = Expression.Parameter(typeof(IDataStream), "input");
        var ctxParam = Expression.Parameter(typeof(PipelineContext), "ctx");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        var typedInputInterface = typeof(IDataStream<>).MakeGenericType(inputType);
        var castInputExpr = Expression.Convert(inputParam, typedInputInterface);

        var instanceExpr = Expression.Constant(instance);
        var typedInstanceExpr = Expression.Convert(instanceExpr, sinkInterface);

        var callExpr = Expression.Call(typedInstanceExpr, executeMethod, castInputExpr, ctxParam, ctParam);

        var lambda = Expression.Lambda<Func<IDataStream, PipelineContext, CancellationToken, Task>>(
            callExpr, inputParam, ctxParam, ctParam);

        return lambda.Compile();
    }

    private static Func<IEnumerable<IDataStream>, PipelineContext, CancellationToken, Task<IDataStream>> BuildJoinDelegate(
        NodeDefinition def,
        IJoinNode joinNode)
    {
        return async (inputs, ctx, ct) =>
        {
            var merged = inputs.First(); // merge already performed upstream
            var stream = merged.ToAsyncEnumerable(ct);
            var joined = await joinNode.ExecuteAsync(stream, ctx, ct).ConfigureAwait(false);
            return new DataStream<object?>(joined, $"JoinResult_{def.Id}");
        };
    }

    private static Func<IDataStream, PipelineContext, CancellationToken, Task<IDataStream>> BuildAggregateDelegate(
        NodeDefinition def,
        IAggregateNode aggregateNode)
    {
        return async (input, ctx, ct) =>
        {
            var stream = input.ToAsyncEnumerable(ct);
            var result = await aggregateNode.ExecuteAsync(stream, ct);

            if (result is IAsyncEnumerable<object?> asyncEnum)
                return new DataStream<object?>(asyncEnum, $"AggregateResult_{def.Id}");

            List<object?> list;

            if (result is not null)
                list = [result];
            else
                list = [];

            return new DataStream<object?>(list.ToAsyncEnumerable(), $"AggregateResult_{def.Id}");
        };
    }

    internal static Func<IDataStream, string, IDataStream>? BuildOutputAdapter(Type? outputType)
    {
        if (outputType is null)
            return null;

        var pipeParam = Expression.Parameter(typeof(IDataStream), "pipe");
        var streamNameParam = Expression.Parameter(typeof(string), "streamName");

        var closedMethod = AdaptOutputPipeGenericMethod.MakeGenericMethod(outputType);
        var call = Expression.Call(closedMethod, pipeParam, streamNameParam);
        var castToIDataStream = Expression.Convert(call, typeof(IDataStream));

        return Expression.Lambda<Func<IDataStream, string, IDataStream>>(castToIDataStream, pipeParam, streamNameParam).Compile();
    }

    private static DataStream<TOut> AdaptOutputPipe<TOut>(IDataStream untyped, string streamName)
    {
        if (untyped is IDataStream<TOut> typedExisting)
        {
            if (typedExisting is DataStream<TOut> streaming)
                return streaming;

            async IAsyncEnumerable<TOut> Passthrough([EnumeratorCancellation] CancellationToken ct = default)
            {
                await foreach (var obj in typedExisting.ToAsyncEnumerable(ct).WithCancellation(ct).ConfigureAwait(false))
                {
                    yield return obj is TOut t
                        ? t
                        : (TOut)obj!;
                }
            }

            return new DataStream<TOut>(Passthrough(), streamName);
        }

        async IAsyncEnumerable<TOut> Cast([EnumeratorCancellation] CancellationToken ct = default)
        {
            await foreach (var item in untyped.ToAsyncEnumerable(ct).WithCancellation(ct).ConfigureAwait(false))
            {
                yield return item is null
                    ? default!
                    : item is TOut t
                        ? t
                        : (TOut)item!;
            }
        }

        return new DataStream<TOut>(Cast(), streamName);
    }
}
