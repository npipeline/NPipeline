using System.Collections.Immutable;
using System.ComponentModel;
using System.Reflection;
using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Graph;
using NPipeline.Graph.PipelineDelegates;
using NPipeline.Graph.Validation;
using NPipeline.Execution.Annotations;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Visualization;
using NPipeline.Reliability;

namespace NPipeline.Pipeline;

/// <summary>
///     Configuration, validation, and lineage methods for PipelineBuilder.
///     Also includes internal lineage adapter construction.
/// </summary>
public sealed partial class PipelineBuilder
{
    /// <summary>
    ///     Method for setting execution strategies on nodes. Use fluent extension methods on node handles instead.
    /// </summary>
    /// <remarks>
    ///     This method is public to support fluent extensions in separate assemblies (e.g., parallelism extensions),
    ///     but is hidden from IntelliSense to discourage direct use. Always use the fluent extension methods on node handles.
    /// </remarks>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public PipelineBuilder WithExecutionStrategy(NodeHandle handle, IExecutionStrategy strategy)
    {
        ArgumentNullException.ThrowIfNull(handle);
        ArgumentNullException.ThrowIfNull(strategy);

        if (!NodeState.Nodes.TryGetValue(handle.Id, out var nodeDef))
            throw new InvalidOperationException(ErrorMessages.NodeNotFoundInBuilder(handle.Id, "WithExecutionStrategy"));

        if (!typeof(ITransformNode).IsAssignableFrom(nodeDef.NodeType) && !typeof(IStreamTransformNode).IsAssignableFrom(nodeDef.NodeType))
            throw new InvalidOperationException(ErrorMessages.ExecutionStrategyCannotBeSetForNonTransformNode(nodeDef.Name, nodeDef.Kind.ToString()));

        NodeState.Nodes[handle.Id] = nodeDef.WithExecutionStrategy(strategy);
        return this;
    }

    /// <summary>
    ///     Wraps each transform whose resilience options allow restarts in the node restart strategy.
    /// </summary>
    /// <remarks>
    ///     A transform whose configured strategy cannot resume is left as it is; <c>ResilienceOptionsRule</c> reports it
    ///     as a build error. The builder's own node definitions are not changed, so a build that fails validation can be
    ///     corrected and built again.
    /// </remarks>
    private static ImmutableArray<NodeDefinition> WithNodeRestart(IEnumerable<NodeDefinition> nodes, ErrorHandlingConfiguration errorHandling)
    {
        var pipelineOptions = errorHandling.Resilience ?? PipelineResilienceOptions.None;
        var result = ImmutableArray.CreateBuilder<NodeDefinition>();

        foreach (var node in nodes)
        {
            var options = errorHandling.NodeResilience?.GetValueOrDefault(node.Id) ?? pipelineOptions;

            var restartable = options.NodeRestart.MaxRestarts > 0
                              && typeof(ITransformNode).IsAssignableFrom(node.NodeType)
                              && node.ExecutionStrategy is null or IResumableExecutionStrategy;

            result.Add(restartable
                ? node.WithExecutionStrategy(new ResilientExecutionStrategy(node.ExecutionStrategy))
                : node);
        }

        return result.ToImmutable();
    }

    /// <summary>
    ///     Adds a unified resilience policy to coordinate retry, error routing, circuit breaking, and dead-letter decisions.
    /// </summary>
    /// <param name="resiliencePolicy">The resilience policy instance to use during execution.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddResiliencePolicy(IResiliencePolicy resiliencePolicy)
    {
        ArgumentNullException.ThrowIfNull(resiliencePolicy);
        ConfigurationState.ResiliencePolicy = resiliencePolicy;
        ConfigurationState.ResiliencePolicyType = null;
        return this;
    }

    /// <summary>
    ///     Adds a unified resilience policy type to coordinate retry, error routing, circuit breaking, and dead-letter decisions.
    /// </summary>
    /// <typeparam name="T">The resilience policy type that implements <see cref="IResiliencePolicy" />.</typeparam>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddResiliencePolicy<T>() where T : IResiliencePolicy
    {
        ConfigurationState.ResiliencePolicyType = typeof(T);
        ConfigurationState.ResiliencePolicy = null;
        return this;
    }

    /// <summary>
    ///     Adds a resilience policy that decides for one node in place of the pipeline's policy.
    /// </summary>
    /// <param name="handle">The node.</param>
    /// <param name="resiliencePolicy">The policy that decides the node's item, restart, and node failures.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddResiliencePolicy(NodeHandle handle, IResiliencePolicy resiliencePolicy)
    {
        ArgumentNullException.ThrowIfNull(handle);
        ArgumentNullException.ThrowIfNull(resiliencePolicy);

        if (!NodeState.Nodes.ContainsKey(handle.Id))
            throw new InvalidOperationException(ErrorMessages.NodeNotFoundInBuilder(handle.Id, "AddResiliencePolicy"));

        NodeState.ExecutionAnnotations[ExecutionAnnotationKeys.NodeResiliencePolicyForNode(handle.Id)] = resiliencePolicy;
        return this;
    }

    /// <summary>
    ///     Adds a dead letter sink to handle messages that cannot be processed after retry attempts.
    /// </summary>
    /// <param name="deadLetterSink">The dead letter sink instance to use for failed messages.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddDeadLetterSink(IDeadLetterSink deadLetterSink)
    {
        ConfigurationState.DeadLetterSink = deadLetterSink;
        ConfigurationState.DeadLetterSinkType = null;
        return this;
    }

    /// <summary>
    ///     Adds a dead letter sink of type T to handle messages that cannot be processed after retry attempts.
    /// </summary>
    /// <typeparam name="T">The type of the dead letter sink that implements IDeadLetterSink.</typeparam>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddDeadLetterSink<T>() where T : IDeadLetterSink
    {
        ConfigurationState.DeadLetterSinkType = typeof(T);
        ConfigurationState.DeadLetterSink = null;
        return this;
    }

    /// <summary>
    ///     Sets the optimization profile for the pipeline, controlling whether sensible defaults
    ///     are applied (<see cref="PipelineOptimizationProfile.Default" />) or strict zero-allocation
    ///     configuration is required (<see cref="PipelineOptimizationProfile.HighThroughput" />).
    /// </summary>
    /// <param name="profile">The optimization profile to use. Default is <see cref="PipelineOptimizationProfile.Default" />.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    /// <remarks>
    ///     <para>
    ///         The profile sets the resilience options that
    ///         <see cref="WithResilience(Func{PipelineResilienceOptions, PipelineResilienceOptions})" /> starts from.
    ///         Under <see cref="PipelineOptimizationProfile.Default" /> (the default), transient item failures are
    ///         retried three times with exponential backoff; permanent failures fail at once.
    ///     </para>
    ///     <para>
    ///         Under <see cref="PipelineOptimizationProfile.HighThroughput" />, nothing is retried
    ///         (<see cref="PipelineResilienceOptions.None" />) and all performance analyzers are active at build time.
    ///     </para>
    /// </remarks>
    public PipelineBuilder WithOptimizationProfile(PipelineOptimizationProfile profile)
    {
        _config = _config with { OptimizationProfile = profile };
        return this;
    }

    /// <summary>
    ///     Configures the pipeline's resilience options: item retry, node restart, node retry, the circuit breaker, and
    ///     what happens to an item that is not retried.
    /// </summary>
    /// <param name="configure">
    ///     Derives the options from the ones passed in, normally with <c>with</c>. The options passed in are the
    ///     optimization profile's defaults (see <see cref="PipelineResilienceOptions.ForProfile" />), with any earlier
    ///     <see cref="WithResilience(Func{PipelineResilienceOptions, PipelineResilienceOptions})" /> call applied.
    /// </param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    /// <remarks>
    ///     The function runs when the pipeline is built, so the call can come before or after
    ///     <see cref="WithOptimizationProfile" />.
    ///     <code>
    ///     builder.WithResilience(o => o with
    ///     {
    ///         ItemRetry = ItemRetryOptions.Default with { MaxRetries = 5 },
    ///         OnItemFailure = ItemFailureAction.DeadLetter,
    ///     });
    ///     </code>
    /// </remarks>
    public PipelineBuilder WithResilience(Func<PipelineResilienceOptions, PipelineResilienceOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        _config = _config with { ConfigureResilience = Compose(_config.ConfigureResilience, configure) };
        return this;
    }

    /// <summary>
    ///     Configures one node's resilience options, derived from the pipeline's.
    /// </summary>
    /// <param name="handle">The node.</param>
    /// <param name="configure">
    ///     Derives the node's options from the ones passed in, normally with <c>with</c>. The options passed in are the
    ///     pipeline's (after <see cref="WithResilience(Func{PipelineResilienceOptions, PipelineResilienceOptions})" />),
    ///     with any earlier call for the same node applied.
    /// </param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    /// <remarks>
    ///     <para>
    ///         Item retry and node restart apply only to transform nodes. Changing them from the pipeline's values
    ///         for a source, sink, or aggregate is a build error rather than a setting that silently does nothing.
    ///     </para>
    ///     <code>
    ///     builder.WithResilience(enrich, o => o with { ItemRetry = o.ItemRetry with { MaxRetries = 10 } });
    ///     </code>
    /// </remarks>
    public PipelineBuilder WithResilience(NodeHandle handle, Func<PipelineResilienceOptions, PipelineResilienceOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(handle);
        ArgumentNullException.ThrowIfNull(configure);

        if (!NodeState.Nodes.ContainsKey(handle.Id))
            throw new InvalidOperationException(ErrorMessages.NodeNotFoundInBuilder(handle.Id, "WithResilience"));

        NodeState.ResilienceOverrides[handle.Id] = Compose(NodeState.ResilienceOverrides.GetValueOrDefault(handle.Id), configure);
        return this;
    }

    private static Func<PipelineResilienceOptions, PipelineResilienceOptions> Compose(
        Func<PipelineResilienceOptions, PipelineResilienceOptions>? first,
        Func<PipelineResilienceOptions, PipelineResilienceOptions> second)
    {
        return first is null
            ? second
            : options => second(first(options));
    }

    /// <summary>
    ///     Enables item-level lineage tracking with default options.
    /// </summary>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder EnableItemLevelLineage()
    {
        _config = _config with
        {
            ItemLevelLineageEnabled = true,
            LineageOptions = _config.LineageOptions ?? LineageOptions.CompleteLineage,
        };

        return this;
    }

    /// <summary>
    ///     Enables item-level lineage tracking with custom immutable option transformation.
    /// </summary>
    /// <param name="configure">A function that transforms baseline lineage options.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder EnableItemLevelLineage(Func<LineageOptions, LineageOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var baseline = _config.LineageOptions ?? LineageOptions.CompleteLineage;
        var opts = configure(baseline);
        ArgumentNullException.ThrowIfNull(opts);
        _config = _config with { ItemLevelLineageEnabled = true, LineageOptions = opts };
        return this;
    }

    /// <summary>
    ///     Adds a lineage sink to record item-level lineage information.
    /// </summary>
    /// <param name="lineageSink">The lineage sink instance to use for recording lineage data.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddLineageSink(ILineageSink lineageSink)
    {
        ConfigurationState.LineageSink = lineageSink;
        ConfigurationState.LineageSinkType = null;
        return this;
    }

    /// <summary>
    ///     Adds a lineage sink of type T to record item-level lineage information.
    /// </summary>
    /// <typeparam name="T">The type of the lineage sink that implements ILineageSink.</typeparam>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddLineageSink<T>() where T : ILineageSink
    {
        ConfigurationState.LineageSinkType = typeof(T);
        ConfigurationState.LineageSink = null;
        return this;
    }

    /// <summary>
    ///     Adds a pipeline lineage sink to record pipeline-level lineage information.
    /// </summary>
    /// <param name="pipelineLineageSink">The pipeline lineage sink instance to use for recording pipeline lineage data.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddPipelineLineageSink(IPipelineLineageSink pipelineLineageSink)
    {
        ConfigurationState.PipelineLineageSink = pipelineLineageSink;
        ConfigurationState.PipelineLineageSinkType = null;
        return this;
    }

    /// <summary>
    ///     Adds a pipeline lineage sink of type T to record pipeline-level lineage information.
    /// </summary>
    /// <typeparam name="T">The type of the pipeline lineage sink that implements IPipelineLineageSink.</typeparam>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddPipelineLineageSink<T>() where T : IPipelineLineageSink
    {
        ConfigurationState.PipelineLineageSinkType = typeof(T);
        ConfigurationState.PipelineLineageSink = null;
        return this;
    }

    /// <summary>
    ///     Sets an execution option for a specific node identified by its ID.
    /// </summary>
    /// <param name="nodeId">The ID of the node to set the execution option for.</param>
    /// <param name="option">The execution option to set for the node.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder SetNodeExecutionOption(string nodeId, object option)
    {
        NodeState.ExecutionAnnotations[nodeId] = option;
        return this;
    }

    /// <summary>
    ///     Sets a global execution observer that will monitor execution across all nodes.
    /// </summary>
    /// <param name="observer">The observer instance to use for global execution monitoring.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder SetGlobalExecutionObserver(object observer)
    {
        ConfigurationState.GlobalExecutionObserver = observer;
        return this;
    }

    /// <summary>
    ///     Sets a global annotation with the specified key and value.
    /// </summary>
    /// <param name="key">The key for the global annotation.</param>
    /// <param name="value">The value for the global annotation.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder SetGlobalAnnotation(string key, object value)
    {
        NodeState.ExecutionAnnotations[$"global::{key}"] = value;
        return this;
    }

    /// <summary>
    ///     Adds a visualizer to generate visual representations of the pipeline.
    /// </summary>
    /// <param name="visualizer">The visualizer instance to use for pipeline visualization.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddVisualizer(IPipelineVisualizer visualizer)
    {
        ConfigurationState.Visualizer = visualizer;
        return this;
    }

    #region Validation Configuration

    /// <summary>
    ///     Sets the validation mode for the pipeline graph.
    /// </summary>
    /// <remarks>
    ///     Determines whether validation issues cause exceptions (Error), warnings (Warn), or are ignored (Off).
    /// </remarks>
    public PipelineBuilder WithValidationMode(GraphValidationMode mode)
    {
        _config = _config with { GraphValidationMode = mode };
        return this;
    }

    /// <summary>
    ///     Adds a custom validation rule to the pipeline graph.
    /// </summary>
    /// <remarks>
    ///     Custom rules are evaluated when building the pipeline unless validation is disabled.
    /// </remarks>
    public PipelineBuilder WithValidationRule(IGraphRule rule)
    {
        _customValidationRules.Add(rule);
        return this;
    }

    /// <summary>
    ///     Disables extended validation rules (enabled by default).
    /// </summary>
    /// <remarks>
    ///     Extended validation includes additional checks for best practices (resilience configuration,
    ///     parallel execution settings, etc.). It's enabled by default for safety. Disable only if you
    ///     need maximum build performance and are confident in your configuration.
    /// </remarks>
    public PipelineBuilder WithoutExtendedValidation()
    {
        _config = _config with { ExtendedValidation = false };
        return this;
    }

    /// <summary>
    ///     Enables early name validation to catch duplicate node names as they're added.
    /// </summary>
    /// <remarks>
    ///     By default, name validation occurs at build time. This option validates names immediately.
    /// </remarks>
    public PipelineBuilder WithEarlyNameValidation()
    {
        _config = _config with { EarlyNameValidation = true };
        return this;
    }

    /// <summary>
    ///     Disables early name validation, allowing duplicate names to be caught at build time instead.
    /// </summary>
    /// <remarks>
    ///     Early name validation is enabled by default. Use this method to disable it and rely on build-time validation instead.
    /// </remarks>
    public PipelineBuilder WithoutEarlyNameValidation()
    {
        _config = _config with { EarlyNameValidation = false };
        return this;
    }

    #endregion
}
