using System.Collections.Immutable;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Caching;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Execution.Plans;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Observability.Logging;
using NPipeline.Pipeline;
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
        ApplyRetryOptions(graph, context);
        ConfigureCircuitBreaker(graph, context);

        var nodeInstances = nodeInstantiationService.InstantiateNodes(graph, nodeFactory);
        ApplyNodeExecutionStrategies(graph, nodeInstances);
        ApplyGlobalExecutionAnnotations(graph, context);
        ApplyGlobalServicesFromProperties(context);

        graph = graph.EnsureNodeDefinitionMapInitialized();
        var nodeDefinitionMap = graph.NodeDefinitionMap;
        var executionPlans = BuildExecutionPlans(definitionType, graph, nodeInstances);

        ApplyStatefulRegistryFromProperties(context);
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

    private static void ApplyRetryOptions(PipelineGraph graph, PipelineContext context)
    {
        var logger = context.LoggerFactory.CreateLogger(nameof(PipelineRunner));
        var execution = context.ExecutionConfiguration;

        if (graph.ErrorHandling.RetryOptions is not null)
        {
            PipelineRunnerLogMessages.StoringRetryOptions(logger, graph.ErrorHandling.RetryOptions.MaxItemRetries);
            execution.GlobalRetryOptions = graph.ErrorHandling.RetryOptions;
        }
        else
        {
            PipelineRunnerLogMessages.RetryOptionsNull(logger);
            execution.GlobalRetryOptions = execution.RetryOptions;
        }

        if (graph.ErrorHandling.NodeRetryOverrides is not { Count: > 0 })
            return;

        foreach (var kvp in graph.ErrorHandling.NodeRetryOverrides)
        {
            execution.NodeRetryOverrides[kvp.Key] = kvp.Value;
        }
    }

    private static void ConfigureCircuitBreaker(PipelineGraph graph, PipelineContext context)
    {
        var execution = context.ExecutionConfiguration;

        if (graph.ErrorHandling.CircuitBreakerOptions is null)
        {
            execution.CircuitBreakerOptions = null;
            execution.CircuitBreakerManager = null;
            execution.CircuitBreakerMemoryOptions = null;
            return;
        }

        execution.CircuitBreakerOptions = graph.ErrorHandling.CircuitBreakerOptions;
        var memoryOptions = graph.ErrorHandling.CircuitBreakerMemoryOptions;
        execution.CircuitBreakerMemoryOptions = memoryOptions;

        if (!graph.ErrorHandling.CircuitBreakerOptions.Enabled)
        {
            execution.CircuitBreakerManager = null;
            execution.CircuitBreakerMemoryOptions = null;
            return;
        }

        var managerLogger = context.LoggerFactory.CreateLogger(nameof(CircuitBreakerManager));
        var circuitBreakerManager = context.CreateAndRegister(new CircuitBreakerManager(managerLogger, memoryOptions));
        execution.CircuitBreakerManager = circuitBreakerManager;
        PipelineRunnerLogMessages.CircuitBreakerManagerCreated(managerLogger);
    }

    private static void ApplyRuntimeBindings(PipelineContext context, RuntimePipelineBindingResult runtimeBinding)
    {
        var lineage = context.Lineage;

        lineage.LineageSink = runtimeBinding.LineageSink;
        lineage.PipelineLineageSink = runtimeBinding.PipelineLineageSink;

        if (runtimeBinding.PipelineErrorHandler is not null)
            context.PipelineErrorHandler = runtimeBinding.PipelineErrorHandler;

        if (runtimeBinding.DeadLetterSink is not null)
            context.DeadLetterSink = runtimeBinding.DeadLetterSink;
    }

    private static void ApplyNodeExecutionStrategies(PipelineGraph graph, IReadOnlyDictionary<string, INode> nodeInstances)
    {
        foreach (var def in graph.Nodes)
        {
            if (def.ExecutionStrategy is not null && nodeInstances.TryGetValue(def.Id, out var inst) && inst is ITransformNode transformNode)
                transformNode.ExecutionStrategy = def.ExecutionStrategy;
        }
    }

    private static void ApplyGlobalExecutionAnnotations(PipelineGraph graph, PipelineContext context)
    {
        foreach (var kv in graph.ExecutionOptions.NodeExecutionAnnotations ?? ImmutableDictionary<string, object>.Empty)
        {
            if (!kv.Key.StartsWith(ExecutionAnnotationKeys.GlobalAnnotationPrefix, StringComparison.Ordinal))
                continue;

            var trimmed = kv.Key.Substring(ExecutionAnnotationKeys.GlobalAnnotationPrefix.Length);
            var newKey = $"{ExecutionAnnotationKeys.GlobalPropertyPrefix}{trimmed}";
            context.Properties[newKey] = kv.Value;
        }
    }

    private static void ApplyGlobalServicesFromProperties(PipelineContext context)
    {
        if (context.Properties.TryGetValue(ExecutionAnnotationKeys.GlobalPropertyPrefix + "NPipeline.StateManager", out var sm))
            context.StateManager = sm as IPipelineStateManager;

        if (context.Properties.TryGetValue(ExecutionAnnotationKeys.ExecutionObserverProperty, out var eo) && eo is IExecutionObserver execObs)
            context.ExecutionObserver = execObs;
    }

    private Dictionary<string, NodeExecutionPlan> BuildExecutionPlans(
        Type definitionType,
        PipelineGraph graph,
        Dictionary<string, INode> nodeInstances)
    {
        return ShouldUseCache(graph)
            ? BuildPlansWithCache(definitionType, graph, nodeInstances)
            : nodeInstantiationService.BuildPlans(graph, nodeInstances);
    }

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

    private static void ApplyStatefulRegistryFromProperties(PipelineContext context)
    {
        if (context.Properties.TryGetValue("NPipeline.Global.NPipeline.State.StatefulRegistry", out var regObj))
            context.StatefulRegistry = regObj as IStatefulRegistry;
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
