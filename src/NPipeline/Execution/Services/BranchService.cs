using System.Collections.Concurrent;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Branching;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Services;

/// <summary>
///     Provides services for branching (fanning out) data from a single pipe to multiple downstream pipes.
/// </summary>
public sealed class BranchService : IBranchService
{
    // Cache of per-T multicast wrappers (single reflection on miss, zero reflection on hot path).
    private static readonly ConcurrentDictionary<Type, IMulticastWrapper> WrapperCache = new();

    /// <inheritdoc />
    public IDataPipe MaybeMulticast(IDataPipe pipe, PipelineGraph graph, string nodeId, PipelineContext context)
    {
        var branchCount = graph.Edges.Count(e => e.SourceNodeId == nodeId);

        if (branchCount <= 1)
            return pipe;

        var dataType = pipe.GetDataType();
        var options = GetBranchOptions(graph, nodeId);
        var metrics = new BranchMetrics();
        context.Items[ExecutionAnnotationKeys.BranchMetricsForNode(nodeId)] = metrics;

        var wrapper = WrapperCache.GetOrAdd(dataType, static t => IMulticastWrapper.Create(t));
        return wrapper.Wrap(pipe, branchCount, options, metrics);
    }

    private static BranchOptions? GetBranchOptions(PipelineGraph graph, string nodeId)
    {
        if (graph.ExecutionOptions.NodeExecutionAnnotations is not null &&
            graph.ExecutionOptions.NodeExecutionAnnotations.TryGetValue(ExecutionAnnotationKeys.BranchOptionsForNode(nodeId), out var fo) &&
            fo is BranchOptions f)
            return f;

        if (graph.ExecutionOptions.NodeExecutionAnnotations is not null &&
            graph.ExecutionOptions.NodeExecutionAnnotations.TryGetValue(ExecutionAnnotationKeys.GlobalBranchingCapacity, out var gcap) &&
            gcap is int gc and > 0)
            return new BranchOptions(gc);

        return null;
    }

    // Internal wrapper abstraction avoids per-call reflection.
    private interface IMulticastWrapper
    {
        IDataPipe Wrap(IDataPipe pipe, int subscribers, BranchOptions? options, BranchMetrics metrics);

        static IMulticastWrapper Create(Type t)
        {
            var wrapperType = typeof(MulticastWrapper<>).MakeGenericType(t);
            return (IMulticastWrapper)Activator.CreateInstance(wrapperType)!;
        }
    }

    private sealed class MulticastWrapper<T> : IMulticastWrapper
    {
        public IDataPipe Wrap(IDataPipe pipe, int subscribers, BranchOptions? options, BranchMetrics metrics)
        {
            var typed = (IDataPipe<T>)pipe;
            return MulticastDataPipe<T>.Create(typed, subscribers, options?.PerSubscriberBufferCapacity, $"Multicast_{pipe.StreamName}", metrics);
        }
    }
}
