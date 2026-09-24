using NPipeline.ErrorHandling;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Caching;
using NPipeline.Execution.Plans;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
using NPipeline.Reliability;
using NPipeline.State;

namespace NPipeline.Execution.Orchestration;

internal sealed class PipelineExecutionSetupStage(
    INodeFactory nodeFactory,
    INodeInstantiationService nodeInstantiationService,
    IPipelineExecutionPlanCache executionPlanCache,
    IRuntimePipelineBinder runtimePipelineBinder)
{
    public async Task<PipelineExecutionSetupResult> PrepareAsync(
        Type definitionType,
        PipelineGraph graph,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definitionType);
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(context);

        var runtimeBinding = await runtimePipelineBinder.BindAsync(graph, context).ConfigureAwait(false);
        graph = runtimeBinding.Graph;
        ApplyRuntimeBindings(context, runtimeBinding);

        await VisualizeIfConfiguredAsync(graph, cancellationToken).ConfigureAwait(false);
        ApplyResilienceOptions(graph, context);
        EnsureDeadLetterSinkIfNeeded(graph, context);

        var nodeInstances = nodeInstantiationService.InstantiateNodes(graph, nodeFactory);
        context.NodeEnvironment.RegisterNodes(nodeInstances);
        ApplyGlobalServices(graph, context);

        graph = graph.EnsureNodeDefinitionMapInitialized();
        var nodeDefinitionMap = graph.NodeDefinitionMap;
        var executionPlans = BuildExecutionPlans(definitionType, graph, nodeInstances);

        nodeInstantiationService.RegisterStatefulNodes(nodeInstances, context);

        return new PipelineExecutionSetupResult(
            graph,
            nodeInstances,
            nodeDefinitionMap,
            executionPlans,
            runtimeBinding.PipelineLineageSink);
    }

    private static async Task VisualizeIfConfiguredAsync(PipelineGraph graph, CancellationToken cancellationToken)
    {
        if (graph.ExecutionOptions.Visualizer is not null)
            await graph.ExecutionOptions.Visualizer.VisualizeAsync(graph, cancellationToken).ConfigureAwait(false);
    }

    private static void ApplyResilienceOptions(PipelineGraph graph, PipelineContext context)
    {
        var execution = context.ExecutionConfiguration;
        execution.ResetResilienceOptions();
        execution.Resilience = graph.ErrorHandling.Resilience ?? PipelineResilienceOptions.None;

        if (graph.ErrorHandling.NodeResilience is { Count: > 0 } nodeResilience)
        {
            foreach (var (nodeId, options) in nodeResilience)
            {
                execution.SetNodeResilienceOptions(nodeId, options);
            }
        }

        PipelineRunnerLogMessages.StoringResilienceOptions(context.Observability.LoggerFactory.CreateLogger(nameof(PipelineRunner)),
            execution.Resilience.ItemRetry.MaxRetries);
    }

    /// <summary>
    ///     Fails the run before any node starts when a transform would dead-letter with nowhere to send the item.
    /// </summary>
    /// <remarks>
    ///     This runs after the runtime bindings, because the dead-letter sink can come from the context or from DI as
    ///     well as from the builder.
    /// </remarks>
    private static void EnsureDeadLetterSinkIfNeeded(PipelineGraph graph, PipelineContext context)
    {
        if (context.DeadLetterSink is not null)
            return;

        foreach (var node in graph.Nodes)
        {
            if (node.Kind == NodeKind.Transform &&
                context.ExecutionConfiguration.GetResilienceOptions(node.Id).OnItemFailure == ItemFailureAction.DeadLetter)
                throw new DeadLetterSinkNotConfiguredException(node.Id);
        }
    }

    private static void ApplyRuntimeBindings(PipelineContext context, RuntimePipelineBindingResult runtimeBinding)
    {
        var lineage = context.Lineage;

        lineage.LineageSink = runtimeBinding.LineageSink;
        lineage.PipelineLineageSink = runtimeBinding.PipelineLineageSink;
        lineage.LineageCollector = runtimeBinding.LineageCollector;
        context.ExecutionConfiguration.ResiliencePolicy = runtimeBinding.ResiliencePolicy;

        if (runtimeBinding.DeadLetterSink is not null)
            context.DeadLetterSink = runtimeBinding.DeadLetterSink;
    }

    /// <summary>
    ///     Copies the services supplied as global annotations onto the typed members of the context that own them.
    /// </summary>
    /// <remarks>
    ///     The annotations bag is the builder's, so it is read here and never mirrored into
    ///     <see cref="PipelineContext.Properties" />, which belongs to the user.
    /// </remarks>
    private static void ApplyGlobalServices(PipelineGraph graph, PipelineContext context)
    {
        var annotations = graph.ExecutionOptions.NodeExecutionAnnotations;

        if (annotations is not { Count: > 0 })
            return;

        if (annotations.TryGetValue(ExecutionAnnotationKeys.GlobalExecutionObserver, out var observer) &&
            observer is IExecutionObserver executionObserver)
            context.Observability.ExecutionObserver = executionObserver;

        if (annotations.TryGetValue(ExecutionAnnotationKeys.GlobalStateManager, out var stateManager) &&
            stateManager is IPipelineStateManager pipelineStateManager)
            context.StateManager = pipelineStateManager;

        if (annotations.TryGetValue(ExecutionAnnotationKeys.GlobalStatefulRegistry, out var registry) &&
            registry is IStatefulRegistry statefulRegistry)
            context.StatefulRegistry = statefulRegistry;
    }

    private Dictionary<string, NodeExecutionPlan> BuildExecutionPlans(
        Type definitionType,
        PipelineGraph graph,
        Dictionary<string, INode> nodeInstances) =>
        ShouldUseCache(graph)
            ? BuildPlansWithCache(definitionType, graph, nodeInstances)
            : nodeInstantiationService.BuildPlans(graph, nodeInstances);

    private Dictionary<string, NodeExecutionPlan> BuildPlansWithCache(
        Type pipelineDefinitionType,
        PipelineGraph graph,
        IReadOnlyDictionary<string, INode> nodeInstances)
    {
        ArgumentNullException.ThrowIfNull(pipelineDefinitionType);
        ArgumentNullException.ThrowIfNull(graph);
        ArgumentNullException.ThrowIfNull(nodeInstances);

        if (executionPlanCache.TryGetCachedPlans(pipelineDefinitionType, graph, out var cachedPlans) && cachedPlans is not null)
            return cachedPlans;

        var plans = nodeInstantiationService.BuildPlans(graph, nodeInstances);
        executionPlanCache.CachePlans(pipelineDefinitionType, graph, plans);

        return plans;
    }

    private bool ShouldUseCache(PipelineGraph graph)
    {
        if (executionPlanCache is NullPipelineExecutionPlanCache)
            return false;

        if (graph.PreconfiguredNodeInstances.Count > 0)
            return false;

        return true;
    }
}
