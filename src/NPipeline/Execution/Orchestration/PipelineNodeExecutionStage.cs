using System.Runtime.ExceptionServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.Routing;
using NPipeline.ErrorHandling;
using NPipeline.Execution.Annotations;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Execution.Orchestration;

internal sealed class PipelineNodeExecutionStage(
    ITopologyService topologyService,
    INodeExecutor nodeExecutor,
    IErrorHandlingService errorHandlingService,
    IPersistenceService persistenceService,
    IObservabilitySurface observabilitySurface)
{
    /// <summary>
    ///     How long sibling terminals are given to observe cancellation before their outputs are detached from cleanup.
    /// </summary>
    private static readonly TimeSpan TerminalShutdownGrace = TimeSpan.FromSeconds(5);

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

        var topology = GraphTopology.For(setup.Graph);
        var inputLookup = topologyService.BuildInputLookup(setup.Graph);
        var sortedNodes = topologyService.TopologicalSort(setup.Graph);
        var deferTerminals = HasFanOut(setup.Graph);

        List<NodeDefinition>? terminals = null;

        foreach (var nodeDef in sortedNodes.Select(id => setup.NodeDefinitionMap[id]))
        {
            if (deferTerminals && IsTerminal(topology, nodeDef))
            {
                (terminals ??= []).Add(nodeDef);
                continue;
            }

            context.CancellationToken.ThrowIfCancellationRequested();
            await ExecuteNodeAsync(nodeDef, setup, context, inputLookup, nodeOutputs).ConfigureAwait(false);
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
            var releaser = new RoutedEdgeReleaser(terminals, inputLookup, nodeOutputs);
            await ExecuteTerminalAsync(terminals[0], setup, context, inputLookup, nodeOutputs, releaser).ConfigureAwait(false);
            return;
        }

        // Terminal nodes run on separate threads from here, so the node output bag they share needs guarding.
        var gate = new object();
        var synchronizedOutputs = new SynchronizedNodeOutputs(nodeOutputs, gate);
        var edgeReleaser = new RoutedEdgeReleaser(terminals, inputLookup, synchronizedOutputs);
        var tasks = new List<Task>(terminals.Count);

        foreach (var nodeDef in terminals)
        {
            tasks.Add(Task.Run(
                () => ExecuteTerminalAsync(nodeDef, setup, context, inputLookup, synchronizedOutputs, edgeReleaser),
                context.CancellationToken));
        }

        // Surface the first failure without waiting on the siblings. A terminal that throws before it starts
        // reading never drains its branch, so the multicast pump blocks on that branch and its siblings stop
        // making progress.
        var pending = new List<Task>(tasks);

        while (pending.Count > 0)
        {
            var finished = await Task.WhenAny(pending).ConfigureAwait(false);
            _ = pending.Remove(finished);

            if (finished.IsCompletedSuccessfully)
                continue;

            // Stop the siblings before cleanup tears down their inputs and instances. A sibling that ignores
            // cancellation is given a grace period, then its outputs are detached so cleanup can dispose them.
            context.CancelRun();
            var rest = Task.WhenAll(pending);

            if (await Task.WhenAny(rest, Task.Delay(TerminalShutdownGrace, CancellationToken.None)).ConfigureAwait(false) != rest)
                synchronizedOutputs.DetachFromInner();

            ObserveInBackground(pending);
            await finished.ConfigureAwait(false); // rethrows with the original stack
        }
    }

    /// <summary>
    ///     Executes a terminal node, including its retries, then releases the routed edges it no longer needs.
    /// </summary>
    private async Task ExecuteTerminalAsync(
        NodeDefinition nodeDef,
        PipelineExecutionSetupResult setup,
        PipelineContext context,
        ILookup<string, Edge> inputLookup,
        IDictionary<string, IDataStream?> nodeOutputs,
        RoutedEdgeReleaser edgeReleaser)
    {
        try
        {
            await ExecuteNodeAsync(nodeDef, setup, context, inputLookup, nodeOutputs).ConfigureAwait(false);
        }
        finally
        {
            edgeReleaser.TerminalFinished(nodeDef.Id);
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
        IDictionary<string, IDataStream?> nodeOutputs)
    {
        context.CancellationToken.ThrowIfCancellationRequested();

        ApplyPerNodeExecutionAnnotation(setup.Graph, context, nodeDef.Id);

        var nodeInstance = setup.NodeInstances[nodeDef.Id];
        var nodeScope = observabilitySurface.BeginNode(context, setup.Graph, nodeDef, nodeInstance);

        // A sink drains its input inside the node's execution, so each L3 retry re-executes the sink and re-begins a
        // node scope. This handle spans every retry: an attempt releasing its own handle must not unregister the
        // scope the next attempt records to, and the last attempt's release ends the node's observability at the
        // node's end, not at the pipeline's.
        using var sinkRetryScope = nodeDef.Kind is NodeKind.Sink or NodeKind.CompositeOutput
            ? context.NodeEnvironment.NodeExecutionScopeRegistry.BeginNodeScope(nodeDef.Id)
            : null;

        try
        {
            await ExecuteNodeWithRetriesAsync(
                nodeDef,
                nodeInstance,
                setup,
                context,
                nodeScope,
                inputLookup,
                nodeOutputs).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            context.NodeEnvironment.SetNodeStatus(nodeDef.Id, NodeExecutionStatus.Failed);
            var failedEvent = observabilitySurface.CompleteNodeFailure(context, nodeScope, ex);
            await persistenceService.TryPersistAfterNode(context, failedEvent).ConfigureAwait(false);
            HandleNodeExecutionException(nodeDef, context, ex);
        }
    }

    /// <summary>
    ///     A node is terminal when nothing downstream consumes it, which makes it safe to defer and drain alongside
    ///     its siblings.
    /// </summary>
    private static bool IsTerminal(GraphTopology topology, NodeDefinition nodeDef) =>
        nodeDef.Kind is NodeKind.Sink or NodeKind.CompositeOutput
        && !topology.OutgoingEdges.ContainsKey(nodeDef.Id);

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

    private static void ApplyPerNodeExecutionAnnotation(PipelineGraph graph, PipelineContext context, string nodeId)
    {
        if (graph.ExecutionOptions.NodeExecutionAnnotations != null &&
            graph.ExecutionOptions.NodeExecutionAnnotations.TryGetValue(nodeId, out var annotation))
            context.NodeEnvironment.NodeExecutionScopeRegistry.SetNodeExecutionAnnotation(nodeId, annotation);
        else
            _ = context.NodeEnvironment.NodeExecutionScopeRegistry.RemoveNodeExecutionAnnotation(nodeId);

        if (graph.ExecutionOptions.NodeExecutionAnnotations != null &&
            graph.ExecutionOptions.NodeExecutionAnnotations.TryGetValue(ExecutionAnnotationKeys.NodeResiliencePolicyForNode(nodeId),
                out var policyAnnotation) &&
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
        IDictionary<string, IDataStream?> nodeOutputs)
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
                context.NodeEnvironment.SetNodeStatus(nodeDef.Id, NodeExecutionStatus.Completed);
                await persistenceService.TryPersistAfterNode(context, completedEvent).ConfigureAwait(false);
            },
            context.CancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Releases each routed edge once every terminal below the node consuming it has finished.
    /// </summary>
    /// <remarks>
    ///     A fan-out pump feeds its edges in lockstep, so an edge nobody reads any more must be released or it stalls its
    ///     siblings. The consuming node cannot release it itself: transform, join and aggregate outputs are lazy and
    ///     are read later by the terminals below them, and node retry reads the same edge again. Once every terminal
    ///     reachable from the node has finished, including its retries, nothing can read the node's input edges again.
    /// </remarks>
    private sealed class RoutedEdgeReleaser
    {
        private readonly object _gate = new();
        private readonly ILookup<string, Edge> _inputLookup;
        private readonly Dictionary<string, List<string>> _nodesByTerminal = new(StringComparer.Ordinal);
        private readonly IDictionary<string, IDataStream?> _nodeOutputs;
        private readonly Dictionary<string, int> _pendingTerminals = new(StringComparer.Ordinal);

        public RoutedEdgeReleaser(
            IReadOnlyList<NodeDefinition> terminals,
            ILookup<string, Edge> inputLookup,
            IDictionary<string, IDataStream?> nodeOutputs)
        {
            _inputLookup = inputLookup;
            _nodeOutputs = nodeOutputs;

            foreach (var terminal in terminals)
            {
                // The terminal itself and every node upstream of it.
                var reached = new List<string>();
                var seen = new HashSet<string>(StringComparer.Ordinal) { terminal.Id };
                var stack = new Stack<string>();
                stack.Push(terminal.Id);

                while (stack.Count > 0)
                {
                    var nodeId = stack.Pop();
                    reached.Add(nodeId);
                    _pendingTerminals[nodeId] = _pendingTerminals.GetValueOrDefault(nodeId) + 1;

                    foreach (var edge in inputLookup[nodeId])
                    {
                        if (seen.Add(edge.SourceNodeId))
                            stack.Push(edge.SourceNodeId);
                    }
                }

                _nodesByTerminal[terminal.Id] = reached;
            }
        }

        public void TerminalFinished(string terminalId)
        {
            if (!_nodesByTerminal.TryGetValue(terminalId, out var reached))
                return;

            List<string>? released = null;

            lock (_gate)
            {
                foreach (var nodeId in reached)
                {
                    if (--_pendingTerminals[nodeId] == 0)
                        (released ??= []).Add(nodeId);
                }
            }

            if (released is null)
                return;

            foreach (var nodeId in released)
            {
                foreach (var edge in _inputLookup[nodeId])
                {
                    if (_nodeOutputs.TryGetValue(edge.SourceNodeId, out var upstream) && upstream is IEdgeRoutedDataStream routed)
                        routed.ReleaseEdge(edge);
                }
            }
        }
    }

    private static void HandleNodeExecutionException(NodeDefinition nodeDef, PipelineContext context, Exception ex)
    {
        var logger = context.Observability.LoggerFactory.CreateLogger(nameof(PipelineRunner));
        PipelineRunnerLogMessages.NodeFailed(logger, nodeDef.Id, ex.GetType().Name, ex.Message);

        // A cancellation of this run is preserved raw. A foreign OperationCanceledException, such as a client
        // timeout, is wrapped like any other failure so it names its node.
        if (ex is OperationCanceledException && context.CancellationToken.IsCancellationRequested)
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
