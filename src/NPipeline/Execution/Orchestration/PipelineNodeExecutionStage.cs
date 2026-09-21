using System.Runtime.ExceptionServices;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using NPipeline.Resilience;

namespace NPipeline.Execution.Orchestration;

internal sealed class PipelineNodeExecutionStage(
    ITopologyService topologyService,
    INodeExecutor nodeExecutor,
    IErrorHandlingService errorHandlingService,
    IPersistenceService persistenceService,
    IObservabilitySurface observabilitySurface)
{
    /// <summary>
    ///     Executes every node in the graph.
    /// </summary>
    /// <remarks>
    ///     <para>
    ///         Only terminal nodes actually pull data: sources, transforms, joins and aggregates all return a lazy stream
    ///         handle and do no work until something enumerates them. Walking the whole graph sequentially is therefore
    ///         correct for a linear pipeline, where the single terminal node drains everything.
    ///     </para>
    ///     <para>
    ///         It is not correct once the graph fans out. A fan-out node feeds its subscribers through one multicast pump
    ///         that advances only when every subscriber accepts the current item, so draining the terminals one after
    ///         another stalls it: with a bounded per-subscriber buffer the pump blocks forever once the first buffer fills
    ///         (a deadlock), and with an unbounded buffer it races ahead and holds the whole stream in memory. Terminal
    ///         nodes below a fan-out are therefore drained concurrently, which restores real backpressure.
    ///     </para>
    /// </remarks>
    public async Task ExecuteAsync(
        PipelineExecutionSetupResult setup,
        PipelineContext context,
        IDictionary<string, IDataStream?> nodeOutputs)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(nodeOutputs);

        var inputLookup = topologyService.BuildInputLookup(setup.Graph);
        var sortedNodes = topologyService.TopologicalSort(setup.Graph);
        var deferTerminals = HasFanOut(setup.Graph);

        List<NodeDefinition>? terminals = null;

        foreach (var nodeDef in sortedNodes.Select(id => setup.NodeDefinitionMap[id]))
        {
            if (deferTerminals && IsTerminal(setup.Graph, nodeDef))
            {
                (terminals ??= []).Add(nodeDef);
                continue;
            }

            context.CancellationToken.ThrowIfCancellationRequested();
            await ExecuteNodeAsync(nodeDef, setup, context, inputLookup, nodeOutputs, null).ConfigureAwait(false);
        }

        if (terminals is not null)
            await DrainTerminalsAsync(terminals, setup, context, inputLookup, nodeOutputs).ConfigureAwait(false);
    }

    /// <summary>
    ///     Drains the deferred terminal nodes together so no subscriber of a shared multicast pump is left unread.
    /// </summary>
    private async Task DrainTerminalsAsync(
        List<NodeDefinition> terminals,
        PipelineExecutionSetupResult setup,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataStream?> nodeOutputs)
    {
        if (terminals.Count == 1)
        {
            context.CancellationToken.ThrowIfCancellationRequested();
            await ExecuteNodeAsync(terminals[0], setup, context, inputLookup, nodeOutputs, null).ConfigureAwait(false);
            return;
        }

        // Terminal nodes run on separate threads from here, so the shared per-run state they touch needs guarding.
        var gate = new object();
        var synchronizedOutputs = new SynchronizedNodeOutputs(nodeOutputs, gate);
        var tasks = new List<Task>(terminals.Count);

        // Freeze CurrentNodeId for the duration: it is one field on the shared context, and these terminals would
        // otherwise interleave their per-item scopes and leave it pointing at whichever finished last.
        context.NodeEnvironment.NodesRunConcurrently = true;

        try
        {
            foreach (var nodeDef in terminals)
            {
                tasks.Add(Task.Run(
                    () => ExecuteNodeAsync(nodeDef, setup, context, inputLookup, synchronizedOutputs, gate),
                    context.CancellationToken));
            }

            // Surface the first failure without waiting on the siblings. A terminal that throws before it starts
            // reading never drains its branch, so the multicast pump blocks on that branch and its siblings stop
            // making progress. Cleanup disposes the streams, which cancels the pump and releases them.
            var pending = new List<Task>(tasks);

            while (pending.Count > 0)
            {
                var finished = await Task.WhenAny(pending).ConfigureAwait(false);
                _ = pending.Remove(finished);

                if (finished.IsCompletedSuccessfully)
                    continue;

                // Cleanup is about to iterate and dispose the node outputs; stragglers must stop touching them.
                synchronizedOutputs.DetachFromInner();
                ObserveInBackground(pending);
                await finished.ConfigureAwait(false); // rethrows with the original stack
            }
        }
        finally
        {
            context.NodeEnvironment.NodesRunConcurrently = false;
        }
    }

    /// <summary>
    ///     Keeps abandoned terminal tasks from surfacing as unobserved exceptions once the run is already failing.
    /// </summary>
    private static void ObserveInBackground(List<Task> pending)
    {
        foreach (var task in pending)
        {
            _ = task.ContinueWith(
                static t => _ = t.Exception,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    private async Task ExecuteNodeAsync(
        NodeDefinition nodeDef,
        PipelineExecutionSetupResult setup,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataStream?> nodeOutputs,
        object? gate)
    {
        context.CancellationToken.ThrowIfCancellationRequested();

        // CurrentNodeId is a single field on the shared context, so it is only meaningful while nodes run one at a
        // time. Concurrent terminals address themselves by node id explicitly instead.
        using var nodeScopeHandle = gate is null
            ? context.ScopedNode(nodeDef.Id)
            : default;

        ApplyPerNodeExecutionAnnotation(setup.Graph, context, nodeDef.Id);

        var nodeInstance = setup.NodeInstances[nodeDef.Id];
        var nodeScope = observabilitySurface.BeginNode(context, setup.Graph, nodeDef, nodeInstance);

        try
        {
            await ExecuteNodeWithRetriesAsync(
                nodeDef,
                nodeInstance,
                setup,
                context,
                nodeScope,
                inputLookup,
                nodeOutputs,
                gate).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            SetNodeFlag(context, gate, $"NodeError_{nodeDef.Id}");
            var failedEvent = observabilitySurface.CompleteNodeFailure(context, nodeScope, ex);
            persistenceService.TryPersistAfterNode(context, failedEvent);
            HandleNodeExecutionException(nodeDef, context, ex);
        }
    }

    /// <summary>
    ///     A node is terminal when nothing downstream consumes it, which makes it safe to defer and drain alongside
    ///     its siblings.
    /// </summary>
    private static bool IsTerminal(PipelineGraph graph, NodeDefinition nodeDef)
    {
        if (nodeDef.Kind is not (NodeKind.Sink or NodeKind.CompositeOutput))
            return false;

        foreach (var edge in graph.Edges)
        {
            if (string.Equals(edge.SourceNodeId, nodeDef.Id, StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    /// <summary>
    ///     Reports whether any node feeds more than one downstream node, which is what puts a multicast pump in play.
    /// </summary>
    private static bool HasFanOut(PipelineGraph graph)
    {
        if (graph.Edges.Length < 2)
            return false;

        HashSet<string>? seen = null;

        foreach (var edge in graph.Edges)
        {
            seen ??= new HashSet<string>(StringComparer.Ordinal);

            if (!seen.Add(edge.SourceNodeId))
                return true;
        }

        return false;
    }

    private static void SetNodeFlag(PipelineContext context, object? gate, string key)
    {
        // Properties is a plain dictionary under the HighThroughput profile, so concurrent terminals must serialize.
        if (gate is null)
        {
            context.Properties[key] = true;
            return;
        }

        lock (gate)
        {
            context.Properties[key] = true;
        }
    }

    private static void ApplyPerNodeExecutionAnnotation(PipelineGraph graph, PipelineContext context, string nodeId)
    {
        if (graph.ExecutionOptions.NodeExecutionAnnotations != null &&
            graph.ExecutionOptions.NodeExecutionAnnotations.TryGetValue(nodeId, out var annotation))
            context.NodeEnvironment.NodeExecutionScopeRegistry.SetNodeExecutionAnnotation(nodeId, annotation);
        else
            _ = context.NodeEnvironment.NodeExecutionScopeRegistry.RemoveNodeExecutionAnnotation(nodeId);

        if (graph.ExecutionOptions.NodeExecutionAnnotations != null &&
            graph.ExecutionOptions.NodeExecutionAnnotations.TryGetValue(ExecutionAnnotationKeys.NodeResiliencePolicyForNode(nodeId), out var policyAnnotation) &&
            policyAnnotation is IResiliencePolicy nodePolicy)
            context.NodeEnvironment.NodeExecutionScopeRegistry.SetRuntimeAnnotation(ExecutionAnnotationKeys.NodeResiliencePolicyForNode(nodeId), nodePolicy);
    }

    private async Task ExecuteNodeWithRetriesAsync(
        NodeDefinition nodeDef,
        INode nodeInstance,
        PipelineExecutionSetupResult setup,
        PipelineContext context,
        NodeObservationScope nodeScope,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataStream?> nodeOutputs,
        object? gate)
    {
        await errorHandlingService.ExecuteWithRetriesAsync(
            nodeDef,
            nodeInstance,
            setup.Graph,
            context,
            async () =>
            {
                var plan = setup.ExecutionPlans[nodeDef.Id];

                await nodeExecutor.ExecuteAsync(
                    plan,
                    setup.Graph,
                    context,
                    inputLookup,
                    nodeOutputs,
                    setup.NodeInstances,
                    setup.NodeDefinitionMap).ConfigureAwait(false);

                var completedEvent = observabilitySurface.CompleteNodeSuccess(context, nodeScope);
                SetNodeFlag(context, gate, $"NodeCompleted_{nodeDef.Id}");
                persistenceService.TryPersistAfterNode(context, completedEvent);
            },
            context.CancellationToken).ConfigureAwait(false);
    }

    private static void HandleNodeExecutionException(NodeDefinition nodeDef, PipelineContext context, Exception ex)
    {
        var logger = context.Observability.LoggerFactory.CreateLogger(nameof(PipelineRunner));
        PipelineRunnerLogMessages.NodeFailed(logger, nodeDef.Id, ex.GetType().Name, ex.Message);

        if (context.ExecutionConfiguration.ResiliencePolicy is not DefaultResiliencePolicy &&
            nodeDef.ExecutionStrategy?.GetType().Name == "ResilientExecutionStrategy")
        {
            var effectiveRetries = RetryOptionsResolver.Resolve(context, nodeDef.Id);

            if (effectiveRetries.MaxNodeRestartAttempts <= 0)
                PipelineRunnerLogMessages.ResilientStrategyWithoutRestartAttempts(logger, nodeDef.Id, effectiveRetries.MaxNodeRestartAttempts);

            if (effectiveRetries.MaxMaterializedItems == null)
                PipelineRunnerLogMessages.ResilientStrategyWithoutMaterializedItems(logger, nodeDef.Id);
        }

        if (context.ExecutionConfiguration.IsParallelExecution)
        {
            PipelineRunnerLogMessages.PreservingExceptionForParallelExecution(logger, ex.GetType().Name, nodeDef.Id);
            ExceptionDispatchInfo.Capture(ex).Throw();
        }

        if (ex is OperationCanceledException)
        {
            PipelineRunnerLogMessages.PreservingCancellationException(logger, nodeDef.Id);
            ExceptionDispatchInfo.Capture(ex).Throw();
        }

        if (ex is not PipelineException)
        {
            PipelineRunnerLogMessages.WrappingException(logger, ex.GetType().Name, nodeDef.Id);
            throw new PipelineExecutionException(ErrorMessages.PipelineExecutionFailedAtNode(nodeDef.Id, ex), ex);
        }

        ExceptionDispatchInfo.Capture(ex).Throw();
    }
}
