using System.Collections.Frozen;
using System.Collections.Immutable;
using NPipeline.Configuration;
using NPipeline.ErrorHandling;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Visualization;

namespace NPipeline.Graph;

/// <summary>
///     Represents the entire pipeline as a directed graph of nodes and edges.
/// </summary>
public sealed record PipelineGraph
{
    /// <summary>
    ///     Creates a new PipelineGraph with the specified core parameters.
    /// </summary>
    /// <param name="nodes">The collection of all node definitions in the graph.</param>
    /// <param name="edges">The collection of all edges connecting the nodes.</param>
    /// <param name="preconfiguredNodeInstances">A dictionary of node instances that are created and configured at definition time.</param>
    public PipelineGraph(
        ImmutableList<NodeDefinition> nodes,
        ImmutableList<Edge> edges,
        ImmutableDictionary<string, INode> preconfiguredNodeInstances)
    {
        Nodes = nodes;
        Edges = edges;
        PreconfiguredNodeInstances = preconfiguredNodeInstances;
    }

    /// <summary>
    ///     Parameterless constructor for use with the builder pattern.
    /// </summary>
    public PipelineGraph()
    {
    }


    /// <summary>
    ///     The collection of all node definitions in the graph.
    /// </summary>
    public required ImmutableList<NodeDefinition> Nodes { get; init; }

    /// <summary>
    ///     The collection of all edges connecting the nodes.
    /// </summary>
    public required ImmutableList<Edge> Edges { get; init; }

    /// <summary>
    ///     A dictionary of node instances that are created and configured at definition time.
    /// </summary>
    public required ImmutableDictionary<string, INode> PreconfiguredNodeInstances { get; init; }

    /// <summary>
    ///     A cached frozen dictionary mapping node IDs to their definitions for O(1) lookups during execution.
    ///     This eliminates repeated conversions from the immutable list to mutable dictionaries at runtime.
    ///     If not explicitly set, it is automatically computed from the Nodes collection.
    /// </summary>
    public FrozenDictionary<string, NodeDefinition> NodeDefinitionMap { get; init; } = FrozenDictionary<string, NodeDefinition>.Empty;

    /// <summary>
    ///     The error handling configuration.
    /// </summary>
    public ErrorHandlingConfiguration ErrorHandling { get; init; } = ErrorHandlingConfiguration.Default;

    /// <summary>
    ///     The lineage configuration.
    /// </summary>
    public LineageConfiguration Lineage { get; init; } = LineageConfiguration.Default;

    /// <summary>
    ///     The execution options configuration.
    /// </summary>
    public ExecutionOptionsConfiguration ExecutionOptions { get; init; } = ExecutionOptionsConfiguration.Default;

    /// <summary>
    ///     Called after the record is initialized to ensure NodeDefinitionMap is populated from Nodes if needed.
    ///     This method should be called after object initialization, or use the factory method CreateAndInitialize.
    /// </summary>
    /// <returns>A new PipelineGraph with NodeDefinitionMap populated if it was empty.</returns>
    public PipelineGraph EnsureNodeDefinitionMapInitialized()
    {
        // If NodeDefinitionMap is empty but Nodes is not, create it from Nodes
        if (NodeDefinitionMap.Count == 0 && Nodes.Count > 0)
            return this with { NodeDefinitionMap = Nodes.ToFrozenDictionary(n => n.Id) };

        return this;
    }
}

/// <summary>
///     Provides a fluent interface for building PipelineGraph instances.
/// </summary>
public sealed class PipelineGraphBuilder
{
    private CircuitBreakerMemoryManagementOptions? _circuitBreakerMemoryOptions;
    private PipelineCircuitBreakerOptions? _circuitBreakerOptions;
    private IDeadLetterSink? _deadLetterSink;
    private Type? _deadLetterSinkType;
    private ImmutableList<Edge> _edges = ImmutableList<Edge>.Empty;
    private bool _itemLevelLineageEnabled;
    private LineageOptions? _lineageOptions;
    private ILineageSink? _lineageSink;
    private Type? _lineageSinkType;
    private FrozenDictionary<string, NodeDefinition> _nodeDefinitionMap = FrozenDictionary<string, NodeDefinition>.Empty;
    private ImmutableDictionary<string, object> _nodeExecutionAnnotations = ImmutableDictionary<string, object>.Empty;
    private ImmutableDictionary<string, PipelineRetryOptions> _nodeRetryOverrides = ImmutableDictionary<string, PipelineRetryOptions>.Empty;
    private ImmutableList<NodeDefinition> _nodes = ImmutableList<NodeDefinition>.Empty;
    private IPipelineErrorHandler? _pipelineErrorHandler;
    private Type? _pipelineErrorHandlerType;
    private IPipelineLineageSink? _pipelineLineageSink;
    private Type? _pipelineLineageSinkType;
    private ImmutableDictionary<string, INode> _preconfiguredNodeInstances = ImmutableDictionary<string, INode>.Empty;
    private PipelineRetryOptions? _retryOptions;
    private IPipelineVisualizer? _visualizer;

    /// <summary>
    ///     Creates a new PipelineGraphBuilder.
    /// </summary>
    public PipelineGraphBuilder()
    {
    }

    /// <summary>
    ///     Sets the nodes for the pipeline graph.
    /// </summary>
    /// <param name="nodes">The collection of node definitions.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithNodes(ImmutableList<NodeDefinition> nodes)
    {
        _nodes = nodes;
        return this;
    }

    /// <summary>
    ///     Sets the nodes for the pipeline graph.
    /// </summary>
    /// <param name="nodes">The collection of node definitions.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithNodes(IEnumerable<NodeDefinition> nodes)
    {
        _nodes = nodes.ToImmutableList();
        return this;
    }

    /// <summary>
    ///     Sets the edges for the pipeline graph.
    /// </summary>
    /// <param name="edges">The collection of edges.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithEdges(ImmutableList<Edge> edges)
    {
        _edges = edges;
        return this;
    }

    /// <summary>
    ///     Sets the edges for the pipeline graph.
    /// </summary>
    /// <param name="edges">The collection of edges.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithEdges(IEnumerable<Edge> edges)
    {
        _edges = edges.ToImmutableList();
        return this;
    }

    /// <summary>
    ///     Sets the preconfigured node instances for the pipeline graph.
    /// </summary>
    /// <param name="instances">The dictionary of preconfigured node instances.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithPreconfiguredNodeInstances(ImmutableDictionary<string, INode> instances)
    {
        _preconfiguredNodeInstances = instances;
        return this;
    }

    /// <summary>
    ///     Sets the preconfigured node instances for the pipeline graph.
    /// </summary>
    /// <param name="instances">The dictionary of preconfigured node instances.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithPreconfiguredNodeInstances(IDictionary<string, INode> instances)
    {
        _preconfiguredNodeInstances = instances.ToImmutableDictionary();
        return this;
    }

    /// <summary>
    ///     Sets the cached node definition map for O(1) lookups during execution.
    /// </summary>
    /// <param name="nodeDefinitionMap">The frozen dictionary mapping node IDs to their definitions.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithNodeDefinitionMap(FrozenDictionary<string, NodeDefinition> nodeDefinitionMap)
    {
        _nodeDefinitionMap = nodeDefinitionMap;
        return this;
    }

    /// <summary>
    ///     Sets the pipeline error handler.
    /// </summary>
    /// <param name="handler">The pipeline error handler.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithPipelineErrorHandler(IPipelineErrorHandler? handler)
    {
        _pipelineErrorHandler = handler;
        return this;
    }

    /// <summary>
    ///     Sets the pipeline error handler type.
    /// </summary>
    /// <param name="handlerType">The pipeline error handler type.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithPipelineErrorHandlerType(Type? handlerType)
    {
        _pipelineErrorHandlerType = handlerType;
        return this;
    }

    /// <summary>
    ///     Sets the dead letter sink.
    /// </summary>
    /// <param name="sink">The dead letter sink.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithDeadLetterSink(IDeadLetterSink? sink)
    {
        _deadLetterSink = sink;
        return this;
    }

    /// <summary>
    ///     Sets the dead letter sink type.
    /// </summary>
    /// <param name="sinkType">The dead letter sink type.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithDeadLetterSinkType(Type? sinkType)
    {
        _deadLetterSinkType = sinkType;
        return this;
    }

    /// <summary>
    ///     Sets whether item-level lineage is enabled.
    /// </summary>
    /// <param name="enabled">Whether item-level lineage is enabled.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithItemLevelLineageEnabled(bool enabled)
    {
        _itemLevelLineageEnabled = enabled;
        return this;
    }

    /// <summary>
    ///     Sets the lineage sink.
    /// </summary>
    /// <param name="sink">The lineage sink.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithLineageSink(ILineageSink? sink)
    {
        _lineageSink = sink;
        return this;
    }

    /// <summary>
    ///     Sets the lineage sink type.
    /// </summary>
    /// <param name="sinkType">The lineage sink type.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithLineageSinkType(Type? sinkType)
    {
        _lineageSinkType = sinkType;
        return this;
    }

    /// <summary>
    ///     Sets the pipeline lineage sink.
    /// </summary>
    /// <param name="sink">The pipeline lineage sink.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithPipelineLineageSink(IPipelineLineageSink? sink)
    {
        _pipelineLineageSink = sink;
        return this;
    }

    /// <summary>
    ///     Sets the pipeline lineage sink type.
    /// </summary>
    /// <param name="sinkType">The pipeline lineage sink type.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithPipelineLineageSinkType(Type? sinkType)
    {
        _pipelineLineageSinkType = sinkType;
        return this;
    }

    /// <summary>
    ///     Sets the lineage options.
    /// </summary>
    /// <param name="options">The lineage options.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithLineageOptions(LineageOptions? options)
    {
        _lineageOptions = options;
        return this;
    }

    /// <summary>
    ///     Sets the retry options.
    /// </summary>
    /// <param name="options">The retry options.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithRetryOptions(PipelineRetryOptions? options)
    {
        _retryOptions = options;
        return this;
    }

    /// <summary>
    ///     Sets the node retry overrides.
    /// </summary>
    /// <param name="overrides">The node retry overrides.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithNodeRetryOverrides(ImmutableDictionary<string, PipelineRetryOptions>? overrides)
    {
        _nodeRetryOverrides = overrides ?? ImmutableDictionary<string, PipelineRetryOptions>.Empty;
        return this;
    }

    /// <summary>
    ///     Sets the node retry overrides.
    /// </summary>
    /// <param name="overrides">The node retry overrides.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithNodeRetryOverrides(IDictionary<string, PipelineRetryOptions>? overrides)
    {
        _nodeRetryOverrides = overrides?.ToImmutableDictionary() ?? ImmutableDictionary<string, PipelineRetryOptions>.Empty;
        return this;
    }

    /// <summary>
    ///     Sets the node execution annotations.
    /// </summary>
    /// <param name="annotations">The node execution annotations.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithNodeExecutionAnnotations(ImmutableDictionary<string, object>? annotations)
    {
        _nodeExecutionAnnotations = annotations ?? ImmutableDictionary<string, object>.Empty;
        return this;
    }

    /// <summary>
    ///     Sets the node execution annotations.
    /// </summary>
    /// <param name="annotations">The node execution annotations.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithNodeExecutionAnnotations(IDictionary<string, object>? annotations)
    {
        _nodeExecutionAnnotations = annotations?.ToImmutableDictionary() ?? ImmutableDictionary<string, object>.Empty;
        return this;
    }

    /// <summary>
    ///     Sets the circuit breaker options.
    /// </summary>
    /// <param name="options">The circuit breaker options.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithCircuitBreakerOptions(PipelineCircuitBreakerOptions? options)
    {
        _circuitBreakerOptions = options;
        return this;
    }

    /// <summary>
    ///     Sets the circuit breaker memory management options.
    /// </summary>
    /// <param name="options">The circuit breaker memory management options.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithCircuitBreakerMemoryOptions(CircuitBreakerMemoryManagementOptions? options)
    {
        _circuitBreakerMemoryOptions = options;
        return this;
    }

    /// <summary>
    ///     Sets the visualizer.
    /// </summary>
    /// <param name="visualizer">The visualizer.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithVisualizer(IPipelineVisualizer? visualizer)
    {
        _visualizer = visualizer;
        return this;
    }

    /// <summary>
    ///     Sets the error handling configuration directly from a configuration object.
    /// </summary>
    /// <param name="config">The error handling configuration.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithErrorHandlingConfiguration(ErrorHandlingConfiguration config)
    {
        ArgumentNullException.ThrowIfNull(config);

        _pipelineErrorHandler = config.PipelineErrorHandler;
        _pipelineErrorHandlerType = config.PipelineErrorHandlerType;
        _deadLetterSink = config.DeadLetterSink;
        _deadLetterSinkType = config.DeadLetterSinkType;
        _retryOptions = config.RetryOptions;
        _nodeRetryOverrides = config.NodeRetryOverrides ?? ImmutableDictionary<string, PipelineRetryOptions>.Empty;
        _circuitBreakerOptions = config.CircuitBreakerOptions;
        _circuitBreakerMemoryOptions = config.CircuitBreakerMemoryOptions;

        return this;
    }

    /// <summary>
    ///     Sets the lineage configuration directly from a configuration object.
    /// </summary>
    /// <param name="config">The lineage configuration.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithLineageConfiguration(LineageConfiguration config)
    {
        ArgumentNullException.ThrowIfNull(config);

        _itemLevelLineageEnabled = config.ItemLevelLineageEnabled;
        _lineageSink = config.LineageSink;
        _lineageSinkType = config.LineageSinkType;
        _pipelineLineageSink = config.PipelineLineageSink;
        _pipelineLineageSinkType = config.PipelineLineageSinkType;
        _lineageOptions = config.LineageOptions;

        return this;
    }

    /// <summary>
    ///     Sets the execution options configuration directly from a configuration object.
    /// </summary>
    /// <param name="config">The execution options configuration.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineGraphBuilder WithExecutionOptionsConfiguration(ExecutionOptionsConfiguration config)
    {
        ArgumentNullException.ThrowIfNull(config);

        _nodeExecutionAnnotations = config.NodeExecutionAnnotations ?? ImmutableDictionary<string, object>.Empty;
        _visualizer = config.Visualizer;

        return this;
    }

    /// <summary>
    ///     Builds the PipelineGraph instance.
    /// </summary>
    /// <returns>The constructed PipelineGraph.</returns>
    public PipelineGraph Build()
    {
        return new PipelineGraph
        {
            Nodes = _nodes,
            Edges = _edges,
            PreconfiguredNodeInstances = _preconfiguredNodeInstances,
            NodeDefinitionMap = _nodeDefinitionMap,
            ErrorHandling = new ErrorHandlingConfiguration
            {
                PipelineErrorHandler = _pipelineErrorHandler,
                DeadLetterSink = _deadLetterSink,
                PipelineErrorHandlerType = _pipelineErrorHandlerType,
                DeadLetterSinkType = _deadLetterSinkType,
                RetryOptions = _retryOptions,
                NodeRetryOverrides = _nodeRetryOverrides,
                CircuitBreakerOptions = _circuitBreakerOptions,
                CircuitBreakerMemoryOptions = _circuitBreakerMemoryOptions,
            },
            Lineage = new LineageConfiguration
            {
                ItemLevelLineageEnabled = _itemLevelLineageEnabled,
                LineageSink = _lineageSink,
                LineageSinkType = _lineageSinkType,
                PipelineLineageSink = _pipelineLineageSink,
                PipelineLineageSinkType = _pipelineLineageSinkType,
                LineageOptions = _lineageOptions,
            },
            ExecutionOptions = new ExecutionOptionsConfiguration
            {
                NodeExecutionAnnotations = _nodeExecutionAnnotations,
                Visualizer = _visualizer,
            },
        };
    }

    /// <summary>
    ///     Creates a new PipelineGraphBuilder with default values.
    /// </summary>
    /// <returns>A new PipelineGraphBuilder instance.</returns>
    public static PipelineGraphBuilder Create()
    {
        return new PipelineGraphBuilder();
    }
}
