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
    /// <summary>
    ///     Begins a pipeline run and returns the created activity.
    /// </summary>
    /// <typeparam name="TDefinition">The type of pipeline definition.</typeparam>
    /// <param name="context">The pipeline context.</param>
    /// <returns>The created pipeline activity.</returns>
    public IPipelineActivity BeginPipeline<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new()
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(ObservabilitySurface));
        var activity = context.Tracer.StartActivity($"Pipeline.Run: {typeof(TDefinition).Name}");
        logger.Log(LogLevel.Information, "Starting pipeline run for {PipelineName}", typeof(TDefinition).Name);
        return activity;
    }

    /// <summary>
    ///     Records successful pipeline completion (branch metrics, final log) and disposes nothing (caller disposes activity scope).
    /// </summary>
    /// <typeparam name="TDefinition">The type of pipeline definition.</typeparam>
    /// <param name="context">The pipeline context.</param>
    /// <param name="graph">The pipeline graph.</param>
    /// <param name="pipelineActivity">The pipeline activity.</param>
    public async Task CompletePipeline<TDefinition>(PipelineContext context, PipelineGraph graph, IPipelineActivity pipelineActivity)
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

        try
        {
            await EmitMetricsAsync<TDefinition>(context, true, null).ConfigureAwait(false);
        }
        catch (Exception emitEx)
        {
            logger.Log(LogLevel.Error, emitEx, "Failed to emit observability metrics for pipeline {PipelineName}", typeof(TDefinition).Name);
        }
    }

    /// <summary>
    ///     Records a pipeline failure (logs + activity exception).
    /// </summary>
    /// <typeparam name="TDefinition">The type of pipeline definition.</typeparam>
    /// <param name="context">The pipeline context.</param>
    /// <param name="ex">The exception that caused failure.</param>
    /// <param name="pipelineActivity">The pipeline activity.</param>
    public async Task FailPipeline<TDefinition>(PipelineContext context, Exception ex, IPipelineActivity pipelineActivity)
        where TDefinition : IPipelineDefinition, new()
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(ObservabilitySurface));
        logger.Log(LogLevel.Error, ex, "Pipeline run for {PipelineName} failed", typeof(TDefinition).Name);
        pipelineActivity.RecordException(ex);

        try
        {
            await EmitMetricsAsync<TDefinition>(context, false, ex).ConfigureAwait(false);
        }
        catch (Exception emitEx)
        {
            logger.Log(LogLevel.Error, emitEx, "Failed to emit observability metrics after pipeline failure for {PipelineName}", typeof(TDefinition).Name);
        }
    }

    /// <summary>
    ///     Starts node execution, returning a scope with timing and activity.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="graph">The pipeline graph.</param>
    /// <param name="nodeDef">The node definition.</param>
    /// <param name="nodeInstance">The node instance.</param>
    /// <returns>A node observation scope containing timing and activity information.</returns>
    public NodeObservationScope BeginNode(PipelineContext context, PipelineGraph graph, NodeDefinition nodeDef, INode nodeInstance)
    {
        var tracer = context.Tracer;
        var logger = context.LoggerFactory.CreateLogger(nameof(ObservabilitySurface));
        var activity = tracer.StartActivity($"Node.Execute: {nodeDef.Id}");
        activity.SetTag("node.id", nodeDef.Id);
        activity.SetTag("node.type", nodeInstance.GetType().Name);
        if (logger.IsEnabled(LogLevel.Information))
        {
            logger.Log(LogLevel.Information, "Executing node {NodeId} of type {NodeType}", nodeDef.Id, nodeInstance.GetType().Name);
        }

        var observer = context.ExecutionObserver;

        var startTs = DateTimeOffset.UtcNow;
        observer.OnNodeStarted(new NodeExecutionStarted(nodeDef.Id, nodeInstance.GetType().Name, startTs));

        // Check for per-node observability configuration
        IAutoObservabilityScope? autoObservabilityScope = null;
        var optionsKey = "NPipeline.Observability.Options:" + nodeDef.Id;

        if (graph.ExecutionOptions.NodeExecutionAnnotations != null &&
            graph.ExecutionOptions.NodeExecutionAnnotations.TryGetValue(
                optionsKey,
                out var optionsValue))
        {
            var collector = context.ObservabilityFactory.ResolveObservabilityCollector();

            if (collector != null && optionsValue != null)
            {
                // Create AutoObservabilityScope using reflection to avoid circular dependency
                var scopeType = Type.GetType("NPipeline.Extensions.Observability.AutoObservabilityScope, NPipeline.Extensions.Observability");

                if (scopeType != null)
                {
                    autoObservabilityScope = (IAutoObservabilityScope?)Activator.CreateInstance(
                        scopeType, collector, nodeDef.Id, optionsValue);
                }
            }
        }

        // Store the scope in context so execution strategies can access it to track item counts
        if (autoObservabilityScope != null)
        {
            var contextKey = PipelineContextKeys.NodeObservabilityScope(nodeDef.Id);
            logger.Log(LogLevel.Debug, "Storing AutoObservabilityScope in context with key: {ContextKey}", contextKey);
            context.Items[contextKey] = autoObservabilityScope;
        }

        return new NodeObservationScope(nodeDef.Id, nodeInstance.GetType().Name, startTs, activity, autoObservabilityScope);
    }

    /// <summary>
    ///     Records node success and returns NodeExecutionCompleted event for downstream persistence.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="scope">The node observation scope.</param>
    /// <returns>A NodeExecutionCompleted event with success information.</returns>
    public NodeExecutionCompleted CompleteNodeSuccess(PipelineContext context, NodeObservationScope scope)
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(ObservabilitySurface));
        logger.Log(LogLevel.Information, "Finished executing node {NodeId}", scope.NodeId);
        var duration = DateTimeOffset.UtcNow - scope.StartTime;

        // Check if AutoObservabilityScope recorded a failure
        var failureException = scope.AutoObservabilityScope?.GetFailureException();
        var success = failureException == null;

        var completed = new NodeExecutionCompleted(scope.NodeId, scope.NodeType, duration, success, failureException);

        context.ExecutionObserver.OnNodeCompleted(completed);

        // Don't dispose AutoObservabilityScope here - it will be disposed when data pipe is fully consumed
        // For streaming execution, items are iterated after node "completes"

        return completed;
    }

    /// <summary>
    ///     Records node failure and returns NodeExecutionCompleted event for downstream persistence.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="scope">The node observation scope.</param>
    /// <param name="ex">The exception that caused the failure.</param>
    /// <returns>A NodeExecutionCompleted event with failure information.</returns>
    public NodeExecutionCompleted CompleteNodeFailure(PipelineContext context, NodeObservationScope scope, Exception ex)
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(ObservabilitySurface));
        logger.Log(LogLevel.Error, ex, "Node {NodeId} failed", scope.NodeId);
        var duration = DateTimeOffset.UtcNow - scope.StartTime;
        var completed = new NodeExecutionCompleted(scope.NodeId, scope.NodeType, duration, false, ex);

        context.ExecutionObserver.OnNodeCompleted(completed);

        // Record failure on AutoObservabilityScope if present and dispose it
        if (scope.AutoObservabilityScope is IAutoObservabilityScope autoScope)
        {
            autoScope.RecordFailure(ex);
            autoScope.Dispose(); // Dispose scope to ensure metrics are recorded
        }

        return completed;
    }

    /// <summary>
    ///     Emits metrics to registered sinks if observability is enabled.
    /// </summary>
    /// <typeparam name="TDefinition">The type of pipeline definition.</typeparam>
    /// <param name="context">The pipeline context.</param>
    /// <param name="success">Whether pipeline execution was successful.</param>
    /// <param name="exception">Any exception that occurred during pipeline execution.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private async Task EmitMetricsAsync<TDefinition>(PipelineContext context, bool success, Exception? exception) where TDefinition : IPipelineDefinition, new()
    {
        var collector = context.ObservabilityFactory.ResolveObservabilityCollector();

        if (collector is null)
            return;

        var startTime = context.Items.TryGetValue(PipelineContextKeys.PipelineStartTimeUtc, out var startTimeObj) && startTimeObj is DateTime startTimeDt
            ? startTimeDt
            : DateTime.UtcNow;

        var pipelineRunId = Guid.NewGuid();
        var endTime = DateTime.UtcNow;
        var pipelineName = typeof(TDefinition).Name;

        await collector.EmitMetricsAsync(pipelineName, pipelineRunId, startTime, endTime, success, exception, context.CancellationToken).ConfigureAwait(false);
    }
}
