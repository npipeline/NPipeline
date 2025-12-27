using System.Collections.Concurrent;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Services;

/// <summary>
///     Provides wrapping that combines counting and multicasting in a single operation.
///     This eliminates one layer of DataPipe wrapping compared to wrapping counting and multicasting separately.
/// </summary>
public sealed class DataPipeWrapperService
{
    // Cache of per-T multicast wrappers (single reflection on miss, zero reflection on hot path).
    private static readonly ConcurrentDictionary<Type, IOptimizedWrapper> WrapperCache = new();

    /// <summary>
    ///     Wraps a data pipe with counting and optional multicasting in a single optimized layer.
    /// </summary>
    /// <param name="pipe">The source data pipe to wrap.</param>
    /// <param name="counter">The stats counter for item counting.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="graph">The pipeline graph.</param>
    /// <param name="nodeId">The ID of the current node.</param>
    /// <returns>An optimized data pipe with counting and optional multicasting.</returns>
    public IDataPipe WrapWithCountingAndBranching(
        IDataPipe pipe,
        StatsCounter counter,
        PipelineContext context,
        PipelineGraph graph,
        string nodeId)
    {
        var branchCount = graph.Edges.Count(e => e.SourceNodeId == nodeId);
        var dataType = pipe.GetDataType();

        var wrapper = WrapperCache.GetOrAdd(dataType, static t => IOptimizedWrapper.Create(t));

        if (branchCount <= 1)
        {
            // No branching needed - use simple counting passthrough
            return wrapper.WrapPassthrough(pipe, counter, context);
        }

        // Branching needed - use combined counting + multicast
        var options = GetBranchOptions(graph, nodeId);
        var metrics = new BranchMetrics();
        context.Items[ExecutionAnnotationKeys.BranchMetricsForNode(nodeId)] = metrics;

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

    // Internal wrapper abstraction avoids per-call reflection.
    private interface IOptimizedWrapper
    {
        IDataPipe WrapPassthrough(IDataPipe pipe, StatsCounter counter, PipelineContext? context);
        IDataPipe WrapMulticast(IDataPipe pipe, StatsCounter counter, int subscribers, BranchOptions? options, BranchMetrics metrics);

        static IOptimizedWrapper Create(Type t)
        {
            var wrapperType = typeof(OptimizedWrapper<>).MakeGenericType(t);
            return (IOptimizedWrapper)Activator.CreateInstance(wrapperType)!;
        }
    }

    private sealed class OptimizedWrapper<T> : IOptimizedWrapper
    {
        public IDataPipe WrapPassthrough(IDataPipe pipe, StatsCounter counter, PipelineContext? context)
        {
            var typed = (IDataPipe<T>)pipe;
            return new CountingPassthroughDataPipe<T>(typed, counter, context);
        }

        public IDataPipe WrapMulticast(IDataPipe pipe, StatsCounter counter, int subscribers, BranchOptions? options, BranchMetrics metrics)
        {
            var typed = (IDataPipe<T>)pipe;
            return new CountingMulticastDataPipe<T>(typed, counter, subscribers, options?.PerSubscriberBufferCapacity, metrics);
        }
    }
}
