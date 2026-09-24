using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution.Plans;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.State;

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

    private static readonly MethodInfo CoerceExecutionStrategyMethod = typeof(NodeInstantiationService)
        .GetMethod(nameof(CoerceExecutionStrategy), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo CoerceStreamExecutionStrategyMethod = typeof(NodeInstantiationService)
        .GetMethod(nameof(CoerceStreamExecutionStrategy), BindingFlags.NonPublic | BindingFlags.Static)!;

    /// <inheritdoc />
    public Dictionary<string, INode> InstantiateNodes(PipelineGraph graph, INodeFactory nodeFactory)
    {
        var nodeInstances = new Dictionary<string, INode>(graph.Nodes.Length);

        foreach (var def in graph.Nodes)
        {
            nodeInstances.Add(def.Id, nodeFactory.Create(def, graph));
        }

        return nodeInstances;
    }

    /// <inheritdoc />
    public void RegisterStatefulNodes(Dictionary<string, INode> nodeInstances, PipelineContext context)
    {
        var registry = context.StatefulRegistry;

        if (registry is null)
            return;

        foreach (var (nodeId, nodeInstance) in nodeInstances)
        {
            if (nodeInstance is IStatefulNode)
                registry.Register(nodeId, nodeInstance);
        }
    }

    /// <summary>
    ///     Builds per-node execution plans binding generic strategies to non-generic delegates.
    /// </summary>
    /// <remarks>
    ///     The resulting plans are instance-independent: node instances are passed to the compiled delegates at
    ///     invocation time rather than captured, so a plan may be cached and reused by later runs that create their
    ///     own node instances. <paramref name="nodeInstances" /> is used only to select and validate the delegate
    ///     shape for each node; no instance is retained by the returned plans.
    /// </remarks>
    public Dictionary<string, NodeExecutionPlan> BuildPlans(PipelineGraph graph, IReadOnlyDictionary<string, INode> nodeInstances)
    {
        var plans = new Dictionary<string, NodeExecutionPlan>(graph.NodeDefinitionMap.Count);

        foreach (var (nodeId, def) in graph.NodeDefinitionMap)
        {
            var instance = nodeInstances[def.Id];

            plans[nodeId] = def.Kind switch
            {
                NodeKind.Source or NodeKind.CompositeInput => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    BuildSourceDelegate(def, instance)),

                NodeKind.Transform or NodeKind.Tap or NodeKind.Branch or NodeKind.Route or NodeKind.Lookup or NodeKind.Composite
                    when instance is ITransformNode => new NodeExecutionPlan(
                        def.Id,
                        def.Kind,
                        def.InputType,
                        def.OutputType,
                        ExecuteTransform: BuildTransformDelegate(def)),

                NodeKind.Transform or NodeKind.Route or NodeKind.StreamTransform or NodeKind.Batch
                    when instance is IStreamTransformNode streamTransformNode => new NodeExecutionPlan(
                        def.Id,
                        def.Kind,
                        def.InputType,
                        def.OutputType,
                        ExecuteTransform: BuildStreamTransformDelegate(def, streamTransformNode)),

                NodeKind.Join when instance is IJoinNode => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    ExecuteJoin: BuildJoinDelegate(def),
                    AdaptOutput: BuildOutputAdapter(def.OutputType)),

                NodeKind.Aggregate when instance is IAggregateNode => new NodeExecutionPlan(
                    def.Id,
                    def.Kind,
                    def.InputType,
                    def.OutputType,
                    ExecuteAggregate: BuildAggregateDelegate(def),
                    AdaptOutput: BuildOutputAdapter(def.OutputType)),

                NodeKind.Sink or NodeKind.CompositeOutput => new NodeExecutionPlan(
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

    private static Func<INode, IExecutionStrategy, IDataStream, PipelineContext, CancellationToken, Task<IDataStream>> BuildTransformDelegate(
        NodeDefinition def)
    {
        var inType = def.InputType ?? throw new InvalidOperationException($"Missing InputType for transform node '{def.Id}'.");
        var outType = def.OutputType ?? throw new InvalidOperationException($"Missing OutputType for transform node '{def.Id}'.");

        return BuildStrategyDelegate(
            def.Id,
            inType,
            outType,
            typeof(IExecutionStrategy),
            nameof(IExecutionStrategy.ExecuteAsync),
            CoerceExecutionStrategyMethod,
            typeof(ITransformNode<,>));
    }

    private static Func<INode, IExecutionStrategy, IDataStream, PipelineContext, CancellationToken, Task<IDataStream>> BuildStreamTransformDelegate(
        NodeDefinition def,
        IStreamTransformNode streamTransformNode)
    {
        var inType = def.InputType ?? throw new InvalidOperationException($"Missing InputType for stream transform node '{def.Id}'.");
        var outType = def.OutputType ?? throw new InvalidOperationException($"Missing OutputType for stream transform node '{def.Id}'.");

        // Validate eagerly so a misconfigured strategy is reported at build time with the node type in hand,
        // rather than on first enumeration. The strategy is still resolved per-run from the current definition.
        _ = NodeExecutionStrategyResolver.ResolveStream(def, streamTransformNode);

        return BuildStrategyDelegate(
            def.Id,
            inType,
            outType,
            typeof(IStreamExecutionStrategy),
            nameof(IStreamExecutionStrategy.ExecuteAsync),
            CoerceStreamExecutionStrategyMethod,
            typeof(IStreamTransformNode<,>));
    }

    /// <summary>
    ///     Compiles a delegate that invokes the supplied execution strategy against the supplied node instance.
    /// </summary>
    /// <remarks>
    ///     Neither the node nor its strategy is captured: both arrive as parameters on each call, which is what makes
    ///     the compiled delegate safe to cache across runs.
    /// </remarks>
    private static Func<INode, IExecutionStrategy, IDataStream, PipelineContext, CancellationToken, Task<IDataStream>> BuildStrategyDelegate(
        string nodeId,
        Type inType,
        Type outType,
        Type strategyInterface,
        string executeMethodName,
        MethodInfo strategyCoercion,
        Type nodeInterfaceDefinition)
    {
        var execMethod = strategyInterface.GetMethod(executeMethodName) ??
                         throw new InvalidOperationException($"Could not find '{executeMethodName}' on {strategyInterface.Name}.");

        var closedExec = execMethod.MakeGenericMethod(inType, outType);

        var nodeParam = Expression.Parameter(typeof(INode), "node");
        var strategyParam = Expression.Parameter(typeof(IExecutionStrategy), "strategy");
        var pipeParam = Expression.Parameter(typeof(IDataStream), "pipe");
        var ctxParam = Expression.Parameter(typeof(PipelineContext), "ctx");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        var typedInputInterface = typeof(IDataStream<>).MakeGenericType(inType);
        var castInput = Expression.Convert(pipeParam, typedInputInterface);
        var typedNodeInterface = nodeInterfaceDefinition.MakeGenericType(inType, outType);
        var castNode = Expression.Convert(nodeParam, typedNodeInterface);
        var strategyExpr = Expression.Call(strategyCoercion, strategyParam, nodeParam, Expression.Constant(nodeId));

        var call = Expression.Call(strategyExpr, closedExec, castInput, castNode, ctxParam, Expression.Constant(nodeId), ctParam);
        var upcastCall = Expression.Call(UpcastTaskGenericMethod.MakeGenericMethod(outType), call);

        return Expression.Lambda<Func<INode, IExecutionStrategy, IDataStream, PipelineContext, CancellationToken, Task<IDataStream>>>(
            upcastCall,
            nodeParam,
            strategyParam,
            pipeParam,
            ctxParam,
            ctParam).Compile();
    }

    private static IExecutionStrategy CoerceExecutionStrategy(IExecutionStrategy strategy, INode node, string nodeId)
    {
        if (node is ITransformNode)
            return strategy;

        throw new InvalidOperationException(ErrorMessages.NodeCannotSupplyExecutionStrategy(
            nodeId,
            node.GetType().FullName ?? node.GetType().Name,
            nameof(ITransformNode)));
    }

    private static IStreamExecutionStrategy CoerceStreamExecutionStrategy(IExecutionStrategy strategy, INode node, string nodeId)
    {
        if (node is not (IStreamTransformNode or ITransformNode))
        {
            throw new InvalidOperationException(ErrorMessages.NodeCannotSupplyExecutionStrategy(
                nodeId,
                node.GetType().FullName ?? node.GetType().Name,
                nameof(IStreamTransformNode)));
        }

        if (strategy is IStreamExecutionStrategy streamStrategy)
            return streamStrategy;

        throw new InvalidOperationException(ErrorMessages.StreamTransformNodeRequiresStreamStrategy(
            nodeId,
            node.GetType().FullName ?? node.GetType().Name,
            strategy.GetType().FullName ?? strategy.GetType().Name));
    }

    // Helper used by expression tree to upcast Task<IDataStream<T>> to Task<IDataStream>
    private static async Task<IDataStream> UpcastTask<T>(Task<IDataStream<T>> task)
    {
        var result = await task.ConfigureAwait(false); // eliminate Task.Result (CA1849)
        return result;
    }

    private static Func<INode, PipelineContext, CancellationToken, Task<IDataStream>> BuildSourceDelegate(
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

        // Build a delegate that calls the method on the supplied node and upcasts the result
        var nodeParam = Expression.Parameter(typeof(INode), "node");
        var ctxParam = Expression.Parameter(typeof(PipelineContext), "ctx");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        var typedInstanceExpr = Expression.Convert(nodeParam, sourceInterface);

        var callExpr = Expression.Call(typedInstanceExpr, executeMethod, ctxParam, ctParam); // IDataStream<TOut>

        // Upcast to non-generic IDataStream so we can wrap in Task.FromResult
        var castExpr = Expression.Convert(callExpr, typeof(IDataStream));

        var fromResultMethod = typeof(Task)
            .GetMethod(nameof(Task.FromResult), BindingFlags.Public | BindingFlags.Static)!
            .MakeGenericMethod(typeof(IDataStream));

        var wrappedCall = Expression.Call(fromResultMethod, castExpr); // Task<IDataStream>

        var lambda = Expression.Lambda<Func<INode, PipelineContext, CancellationToken, Task<IDataStream>>>(
            wrappedCall, nodeParam, ctxParam, ctParam);

        return lambda.Compile();
    }

    private static Func<INode, IDataStream, PipelineContext, CancellationToken, Task> BuildSinkDelegate(
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

        // Build a delegate that casts input and calls the method on the supplied node
        var nodeParam = Expression.Parameter(typeof(INode), "node");
        var inputParam = Expression.Parameter(typeof(IDataStream), "input");
        var ctxParam = Expression.Parameter(typeof(PipelineContext), "ctx");
        var ctParam = Expression.Parameter(typeof(CancellationToken), "ct");

        var typedInputInterface = typeof(IDataStream<>).MakeGenericType(inputType);
        var castInputExpr = Expression.Convert(inputParam, typedInputInterface);

        var typedInstanceExpr = Expression.Convert(nodeParam, sinkInterface);

        var callExpr = Expression.Call(typedInstanceExpr, executeMethod, castInputExpr, ctxParam, ctParam);

        var lambda = Expression.Lambda<Func<INode, IDataStream, PipelineContext, CancellationToken, Task>>(
            callExpr, nodeParam, inputParam, ctxParam, ctParam);

        return lambda.Compile();
    }

    private static Func<INode, IEnumerable<IDataStream>, PipelineContext, CancellationToken, Task<IDataStream>> BuildJoinDelegate(
        NodeDefinition def)
    {
        var streamName = $"JoinResult_{def.Id}";
        var nodeId = def.Id;

        return async (node, inputs, ctx, ct) =>
        {
            var joinNode = node as IJoinNode
                           ?? throw new InvalidOperationException(
                               $"Join node '{nodeId}' of type '{node.GetType().FullName}' does not implement {nameof(IJoinNode)}.");

            var merged = inputs.First(); // merge already performed upstream
            var stream = merged.ToAsyncEnumerable(ct);
            var joined = await joinNode.ExecuteAsync(stream, ctx, ct).ConfigureAwait(false);
            return new DataStream<object?>(joined, streamName);
        };
    }

    private static Func<INode, IDataStream, PipelineContext, CancellationToken, Task<IDataStream>> BuildAggregateDelegate(
        NodeDefinition def)
    {
        var streamName = $"AggregateResult_{def.Id}";
        var nodeId = def.Id;

        return async (node, input, ctx, ct) =>
        {
            var aggregateNode = node as IAggregateNode
                                ?? throw new InvalidOperationException(
                                    $"Aggregate node '{nodeId}' of type '{node.GetType().FullName}' does not implement {nameof(IAggregateNode)}.");

            var stream = input.ToAsyncEnumerable(ct);
            var result = await aggregateNode.ExecuteAsync(stream, ct).ConfigureAwait(false);

            if (result is IAsyncEnumerable<object?> asyncEnum)
                return new DataStream<object?>(asyncEnum, streamName);

            List<object?> list = result is not null
                ? [result]
                : [];

            return new DataStream<object?>(list.ToAsyncEnumerable(), streamName);
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
