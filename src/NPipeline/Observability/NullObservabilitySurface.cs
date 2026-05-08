using System.Diagnostics;
using NPipeline.Execution;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Observability;

/// <summary>
///     A no-op implementation of <see cref="IObservabilitySurface" /> that performs no logging, tracing, or metrics collection.
/// </summary>
public sealed class NullObservabilitySurface : IObservabilitySurface
{
    /// <summary>
    ///     Gets the shared singleton instance.
    /// </summary>
    public static readonly NullObservabilitySurface Instance = new();

    /// <inheritdoc />
    public IPipelineActivity BeginPipeline<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new()
        => NullPipelineActivity.Instance;

    /// <inheritdoc />
    public Task CompletePipeline<TDefinition>(PipelineContext context, PipelineGraph graph, IPipelineActivity pipelineActivity)
        where TDefinition : IPipelineDefinition, new()
        => Task.CompletedTask;

    /// <inheritdoc />
    public Task FailPipeline<TDefinition>(PipelineContext context, Exception ex, IPipelineActivity pipelineActivity)
        where TDefinition : IPipelineDefinition, new()
        => Task.CompletedTask;

    /// <inheritdoc />
    public IPipelineActivity BeginPipeline(Type definitionType, PipelineContext context)
        => NullPipelineActivity.Instance;

    /// <inheritdoc />
    public Task CompletePipeline(Type definitionType, PipelineContext context, PipelineGraph graph, IPipelineActivity pipelineActivity)
        => Task.CompletedTask;

    /// <inheritdoc />
    public Task FailPipeline(Type definitionType, PipelineContext context, Exception ex, IPipelineActivity pipelineActivity)
        => Task.CompletedTask;

    /// <inheritdoc />
    public NodeObservationScope BeginNode(PipelineContext context, PipelineGraph graph, NodeDefinition nodeDef, INode nodeInstance)
    {
        var startTs = DateTimeOffset.UtcNow;
        var startTimestamp = Stopwatch.GetTimestamp();
        return new NodeObservationScope(nodeDef.Id, nodeInstance.GetType().Name, startTs, startTimestamp,
            NullPipelineActivity.Instance, context.PipelineId, context.PipelineName, null);
    }

    /// <inheritdoc />
    public NodeExecutionCompleted CompleteNodeSuccess(PipelineContext context, NodeObservationScope scope)
    {
        var duration = Stopwatch.GetElapsedTime(scope.StartTimestamp);
        return new NodeExecutionCompleted(scope.NodeId, scope.NodeType, duration, true, null, scope.PipelineId, scope.PipelineName);
    }

    /// <inheritdoc />
    public NodeExecutionCompleted CompleteNodeFailure(PipelineContext context, NodeObservationScope scope, Exception ex)
    {
        var duration = Stopwatch.GetElapsedTime(scope.StartTimestamp);
        return new NodeExecutionCompleted(scope.NodeId, scope.NodeType, duration, false, ex, scope.PipelineId, scope.PipelineName);
    }
}
