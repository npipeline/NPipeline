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
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Visualization;

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
    ///     Method for applying resilience wrapping to nodes. Use fluent extension methods on node handles instead.
    /// </summary>
    /// <remarks>
    ///     This method is public to support fluent extensions in separate assemblies,
    ///     but is hidden from IntelliSense to discourage direct use. Always use the fluent extension methods on node handles.
    /// </remarks>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public PipelineBuilder WithResilience(NodeHandle handle)
    {
        if (!NodeState.Nodes.TryGetValue(handle.Id, out var nodeDef))
            throw new InvalidOperationException(ErrorMessages.NodeNotFoundInBuilder(handle.Id, "WithResilience"));

        if (!typeof(ITransformNode).IsAssignableFrom(nodeDef.NodeType))
            throw new InvalidOperationException(ErrorMessages.ResilienceCannotBeAppliedToNonTransformNode(nodeDef.Name, nodeDef.Kind.ToString()));

        var currentStrategy = nodeDef.ExecutionStrategy ?? SequentialExecutionStrategy.Instance;

        if (currentStrategy is not ResilientExecutionStrategy)
            currentStrategy = new ResilientExecutionStrategy(currentStrategy);

        NodeState.Nodes[handle.Id] = nodeDef.WithExecutionStrategy(currentStrategy);
        return this;
    }

    /// <summary>
    ///     Method for setting error handlers on nodes. Use fluent extension methods on node handles instead.
    /// </summary>
    /// <remarks>
    ///     This method is public to support fluent extensions in separate assemblies,
    ///     but is hidden from IntelliSense to discourage direct use. Always use the fluent extension methods on node handles.
    /// </remarks>
    [EditorBrowsable(EditorBrowsableState.Never)]
    public PipelineBuilder WithErrorHandler(NodeHandle handle, Type errorHandlerType)
    {
        ArgumentNullException.ThrowIfNull(handle);
        ArgumentNullException.ThrowIfNull(errorHandlerType);

        if (!NodeState.Nodes.TryGetValue(handle.Id, out var nodeDef))
            throw new InvalidOperationException(ErrorMessages.NodeNotFoundInBuilder(handle.Id, "WithErrorHandler"));

        if (!typeof(INodeErrorHandler).IsAssignableFrom(errorHandlerType))
            throw new ArgumentException(ErrorMessages.InvalidErrorHandlerType(errorHandlerType.Name), nameof(errorHandlerType));

        NodeState.Nodes[handle.Id] = nodeDef.WithErrorHandlerType(errorHandlerType);
        return this;
    }

    /// <summary>
    ///     Adds a pipeline error handler to handle errors that occur during pipeline execution.
    /// </summary>
    /// <param name="errorHandler">The error handler instance to use for pipeline-wide error handling.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddPipelineErrorHandler(IPipelineErrorHandler errorHandler)
    {
        ArgumentNullException.ThrowIfNull(errorHandler);
        ConfigurationState.PipelineErrorHandler = errorHandler;
        ConfigurationState.PipelineErrorHandlerType = null;
        return this;
    }

    /// <summary>
    ///     Adds a pipeline error handler of type T to handle errors that occur during pipeline execution.
    /// </summary>
    /// <typeparam name="T">The type of the error handler that implements IPipelineErrorHandler.</typeparam>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder AddPipelineErrorHandler<T>() where T : IPipelineErrorHandler
    {
        ConfigurationState.PipelineErrorHandlerType = typeof(T);
        ConfigurationState.PipelineErrorHandler = null;
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
    ///     Configures retry options for the pipeline using a configuration function.
    /// </summary>
    /// <param name="configure">A function that takes the current retry options and returns modified options.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder WithRetryOptions(Func<PipelineRetryOptions, PipelineRetryOptions> configure)
    {
        _config = _config with { RetryOptions = configure(_config.RetryOptions) };
        return this;
    }

    /// <summary>
    ///     Configures circuit breaker settings for the pipeline to handle failures gracefully.
    /// </summary>
    /// <param name="failureThreshold">The number of failures before opening the circuit breaker. Default is 5.</param>
    /// <param name="openDuration">The duration to keep the circuit breaker open. Default is 1 minute.</param>
    /// <param name="samplingWindow">The time window to sample for failure rate calculation. Default is 5 minutes.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder WithCircuitBreaker(int failureThreshold = 5, TimeSpan? openDuration = null, TimeSpan? samplingWindow = null)
    {
        _config = _config with
        {
            CircuitBreakerOptions =
            new PipelineCircuitBreakerOptions(failureThreshold, openDuration ?? TimeSpan.FromMinutes(1), samplingWindow ?? TimeSpan.FromMinutes(5))
                .Validate(),
        };

        return this;
    }

    /// <summary>
    ///     Configures memory management options for the circuit breaker.
    /// </summary>
    /// <param name="configure">A function that takes the current memory management options and returns modified options.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder ConfigureCircuitBreakerMemoryManagement(Func<CircuitBreakerMemoryManagementOptions, CircuitBreakerMemoryManagementOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var current = _config.CircuitBreakerMemoryOptions ?? CircuitBreakerMemoryManagementOptions.Default;
        _config = _config with { CircuitBreakerMemoryOptions = configure(current).Validate() };
        return this;
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
    ///     Sets retry options for a specific node identified by its handle.
    /// </summary>
    /// <param name="handle">The handle of the node to configure retry options for.</param>
    /// <param name="options">The retry options to apply to the node.</param>
    /// <returns>The current PipelineBuilder instance for method chaining.</returns>
    public PipelineBuilder WithRetryOptions(NodeHandle handle, PipelineRetryOptions options)
    {
        if (!NodeState.Nodes.ContainsKey(handle.Id))
            throw new InvalidOperationException(ErrorMessages.NodeNotFoundInBuilder(handle.Id, "WithRetryOptions"));

        NodeState.RetryOverrides[handle.Id] = options;
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
