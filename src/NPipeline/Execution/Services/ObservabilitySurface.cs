using NPipeline.DataFlow.Branching;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Logging;
using NPipeline.Observability.Tracing;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Services;

/// <summary>
///     Default implementation of <see cref="IObservabilitySurface" /> centralizing logging, tracing and observer events.
/// </summary>
public sealed class ObservabilitySurface : IObservabilitySurface
{
    public IPipelineActivity BeginPipeline<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new()
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(ObservabilitySurface));
        var activity = context.Tracer.StartActivity($"Pipeline.Run: {typeof(TDefinition).Name}");
        logger.Log(LogLevel.Information, "Starting pipeline run for {PipelineName}", typeof(TDefinition).Name);
        return activity;
    }

    public void CompletePipeline<TDefinition>(PipelineContext context, PipelineGraph graph, IPipelineActivity pipelineActivity)
        where TDefinition : IPipelineDefinition, new()
    {
        // Emit branch metrics as tracing tags
        foreach (var kv in context.Items.Where(kv => kv.Key.StartsWith(ExecutionAnnotationKeys.BranchMetricsPrefix, StringComparison.Ordinal)))
        {
            if (kv.Value is BranchMetrics fm)
            {
                pipelineActivity.SetTag($"{kv.Key}.subscribers", fm.SubscriberCount);
                pipelineActivity.SetTag($"{kv.Key}.capacity", fm.PerSubscriberCapacity ?? -1);
                pipelineActivity.SetTag($"{kv.Key}.maxAggregateBacklog", fm.MaxAggregateBacklog);
                pipelineActivity.SetTag($"{kv.Key}.completed", fm.SubscribersCompleted);
                pipelineActivity.SetTag($"{kv.Key}.faulted", fm.Faulted);
            }
        }

        var logger = context.LoggerFactory.CreateLogger(nameof(ObservabilitySurface));
        logger.Log(LogLevel.Information, "Finished pipeline run for {PipelineName}", typeof(TDefinition).Name);
    }

    public void FailPipeline<TDefinition>(PipelineContext context, Exception ex, IPipelineActivity pipelineActivity)
        where TDefinition : IPipelineDefinition, new()
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(ObservabilitySurface));
        logger.Log(LogLevel.Error, ex, "Pipeline run for {PipelineName} failed", typeof(TDefinition).Name);
        pipelineActivity.RecordException(ex);
    }

    public NodeObservationScope BeginNode(PipelineContext context, NodeDefinition nodeDef, INode nodeInstance)
    {
        var tracer = context.Tracer;
        var logger = context.LoggerFactory.CreateLogger(nameof(ObservabilitySurface));
        var activity = tracer.StartActivity($"Node.Execute: {nodeDef.Id}");
        activity.SetTag("node.id", nodeDef.Id);
        activity.SetTag("node.type", nodeInstance.GetType().Name);
        logger.Log(LogLevel.Information, "Executing node {NodeId} of type {NodeType}", nodeDef.Id, nodeInstance.GetType().Name);

        var observer = context.ExecutionObserver;

        var startTs = DateTimeOffset.UtcNow;
        observer.OnNodeStarted(new NodeExecutionStarted(nodeDef.Id, nodeInstance.GetType().Name, startTs));
        return new NodeObservationScope(nodeDef.Id, nodeInstance.GetType().Name, startTs, activity);
    }

    public NodeExecutionCompleted CompleteNodeSuccess(PipelineContext context, NodeObservationScope scope)
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(ObservabilitySurface));
        logger.Log(LogLevel.Information, "Finished executing node {NodeId}", scope.NodeId);
        var duration = DateTimeOffset.UtcNow - scope.StartTime;
        var completed = new NodeExecutionCompleted(scope.NodeId, scope.NodeType, duration, true, null);

        context.ExecutionObserver.OnNodeCompleted(completed);
        return completed;
    }

    public NodeExecutionCompleted CompleteNodeFailure(PipelineContext context, NodeObservationScope scope, Exception ex)
    {
        scope.Activity.RecordException(ex);
        var duration = DateTimeOffset.UtcNow - scope.StartTime;
        var failed = new NodeExecutionCompleted(scope.NodeId, scope.NodeType, duration, false, ex);
        context.ExecutionObserver.OnNodeCompleted(failed);
        return failed;
    }
}
