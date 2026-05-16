using System.Collections.Concurrent;
using System.Reflection;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.DataStreams;
using NPipeline.DataFlow.Routing;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;
using NPipeline.Lineage;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Services;

/// <summary>
///     Provides wrapping that combines counting and multicasting in a single operation.
///     This eliminates one layer of DataStream wrapping compared to wrapping counting and multicasting separately.
/// </summary>
public sealed class DataStreamWrapperService
{
    // Cache of per-T multicast wrappers (single reflection on miss, zero reflection on hot path).
    private static readonly ConcurrentDictionary<Type, IOptimizedWrapper> WrapperCache = new();

    // Cache of route-options adapters for lineage-wrapped stream compatibility.
    private static readonly ConcurrentDictionary<(Type StreamType, Type RouteOptionsType), Func<object, object>?> RouteOptionsAdapterCache = new();

    /// <summary>
    ///     Wraps a data pipe with counting and optional multicasting in a single optimized layer.
    /// </summary>
    /// <param name="pipe">The source data pipe to wrap.</param>
    /// <param name="counter">The stats counter for item counting.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="graph">The pipeline graph.</param>
    /// <param name="nodeId">The ID of the current node.</param>
    /// <returns>An optimized data pipe with counting and optional multicasting.</returns>
    public IDataStream WrapWithCountingAndBranching(
        IDataStream pipe,
        StatsCounter counter,
        PipelineContext context,
        PipelineGraph graph,
        string nodeId)
    {
        var outgoingEdges = graph.Edges.Where(e => e.SourceNodeId == nodeId).ToArray();
        var branchCount = outgoingEdges.Length;
        var routeOptions = GetRouteOptions(graph, nodeId);
        var isRouteNode = graph.NodeDefinitionMap.TryGetValue(nodeId, out var nodeDef) && nodeDef.Kind == NodeKind.Route;
        var useConditionalRouting = isRouteNode && routeOptions is not null;
        var dataType = pipe.GetDataType();

        var wrapper = WrapperCache.GetOrAdd(dataType, static t => IOptimizedWrapper.Create(t));

        if (!useConditionalRouting && branchCount <= 1)
        {
            // No branching needed - use simple counting passthrough
            return wrapper.WrapPassthrough(pipe, counter, context);
        }

        // Branching or conditional routing needed - use combined counting + multicast wrappers
        var options = GetBranchOptions(graph, nodeId);
        var metrics = new BranchMetrics();
        context.NodeExecutionScopeRegistry.SetRuntimeAnnotation(ExecutionAnnotationKeys.BranchMetricsForNode(nodeId), metrics);

        if (useConditionalRouting)
            return wrapper.WrapConditionalMulticast(pipe, counter, outgoingEdges, options, routeOptions!, metrics);

        return wrapper.WrapMulticast(pipe, counter, branchCount, options, metrics);
    }

    private static BranchOptions? GetBranchOptions(PipelineGraph graph, string nodeId)
    {
        return graph.ExecutionOptions.NodeExecutionAnnotations?.TryGetValue(ExecutionAnnotationKeys.BranchOptionsForNode(nodeId), out var fo) == true &&
               fo is BranchOptions f
            ? f
            : graph.ExecutionOptions.NodeExecutionAnnotations?.TryGetValue(ExecutionAnnotationKeys.GlobalBranchingCapacity, out var gcap) == true &&
              gcap is int gc and > 0
                ? new BranchOptions(gc)
                : null;
    }

    private static object? GetRouteOptions(PipelineGraph graph, string nodeId)
    {
        return graph.ExecutionOptions.NodeExecutionAnnotations?.TryGetValue(ExecutionAnnotationKeys.RouteOptionsForNode(nodeId), out var routeOptions) == true
            ? routeOptions
            : null;
    }

    private static string GetAssemblyQualifiedTypeName(Type type)
    {
        return type.AssemblyQualifiedName ?? type.FullName ?? type.Name;
    }

    private static Func<object, object>? CreateRouteOptionsAdapter(Type streamType, Type routeOptionsType)
    {
        if (!streamType.IsGenericType || streamType.GetGenericTypeDefinition() != typeof(LineagePacket<>))
            return null;

        var payloadType = streamType.GetGenericArguments()[0];
        var payloadRouteOptionsType = typeof(RouteOptions<>).MakeGenericType(payloadType);

        if (routeOptionsType != payloadRouteOptionsType)
            return null;

        var method = typeof(DataStreamWrapperService)
            .GetMethod(nameof(AdaptLineageRouteOptions), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(payloadType);

        return method.CreateDelegate<Func<object, object>>();
    }

    private static object AdaptLineageRouteOptions<TPayload>(object routeOptions)
    {
        var payloadRouteOptions = (RouteOptions<TPayload>)routeOptions;
        var adapted = new RouteOptions<LineagePacket<TPayload>>()
            .WithMatchMode(payloadRouteOptions.MatchMode)
            .WithNoMatchBehavior(payloadRouteOptions.NoMatchBehavior);

        if (payloadRouteOptions.OtherwiseOutputName is { } otherwiseOutputName)
            adapted.Otherwise(otherwiseOutputName);

        foreach (var rule in payloadRouteOptions.Rules)
        {
            adapted.When(rule.OutputName, packet => rule.Predicate(packet.Data));
        }

        return adapted;
    }

    // Internal wrapper abstraction avoids per-call reflection.
    private interface IOptimizedWrapper
    {
        IDataStream WrapPassthrough(IDataStream pipe, StatsCounter counter, PipelineContext? context);
        IDataStream WrapMulticast(IDataStream pipe, StatsCounter counter, int subscribers, BranchOptions? options, BranchMetrics metrics);
        IDataStream WrapConditionalMulticast(
            IDataStream pipe,
            StatsCounter counter,
            IReadOnlyList<Edge> outgoingEdges,
            BranchOptions? options,
            object routeOptions,
            BranchMetrics metrics);

        static IOptimizedWrapper Create(Type t)
        {
            var wrapperType = typeof(OptimizedWrapper<>).MakeGenericType(t);
            return (IOptimizedWrapper)Activator.CreateInstance(wrapperType)!;
        }
    }

    private sealed class OptimizedWrapper<T> : IOptimizedWrapper
    {
        public IDataStream WrapPassthrough(IDataStream pipe, StatsCounter counter, PipelineContext? context)
        {
            var typed = (IDataStream<T>)pipe;
            return new CountingPassthroughDataStream<T>(typed, counter, context);
        }

        public IDataStream WrapMulticast(IDataStream pipe, StatsCounter counter, int subscribers, BranchOptions? options, BranchMetrics metrics)
        {
            var typed = (IDataStream<T>)pipe;
            return new CountingMulticastDataStream<T>(typed, counter, subscribers, options?.PerSubscriberBufferCapacity, metrics);
        }

        public IDataStream WrapConditionalMulticast(
            IDataStream pipe,
            StatsCounter counter,
            IReadOnlyList<Edge> outgoingEdges,
            BranchOptions? options,
            object routeOptions,
            BranchMetrics metrics)
        {
            var typed = (IDataStream<T>)pipe;

            var typedRouteOptions = routeOptions as RouteOptions<T>;

            if (typedRouteOptions is null)
            {
                var adapter = RouteOptionsAdapterCache.GetOrAdd(
                    (typeof(T), routeOptions.GetType()),
                    static key => CreateRouteOptionsAdapter(key.StreamType, key.RouteOptionsType));

                if (adapter is not null)
                    typedRouteOptions = (RouteOptions<T>)adapter(routeOptions);
            }

            if (typedRouteOptions is null)
            {
                throw new InvalidOperationException(
                    $"Route options type mismatch for routed stream '{typed.StreamName}'. " +
                    $"Expected {GetAssemblyQualifiedTypeName(typeof(RouteOptions<T>))} but got {GetAssemblyQualifiedTypeName(routeOptions.GetType())}.");
            }

            return new CountingConditionalMulticastDataStream<T>(
                typed,
                counter,
                outgoingEdges,
                options?.PerSubscriberBufferCapacity,
                typedRouteOptions,
                metrics);
        }
    }
}
