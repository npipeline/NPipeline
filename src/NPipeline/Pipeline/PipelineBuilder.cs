using System.Collections.Immutable;
using System.Reflection;
using NPipeline.Configuration;
using NPipeline.Graph;
using NPipeline.Graph.PipelineDelegates;
using NPipeline.Graph.Validation;
using NPipeline.Nodes;
using NPipeline.Pipeline.Internals;

namespace NPipeline.Pipeline;

/// <summary>
///     A builder for creating complex pipeline graphs with compile-time type safety.
/// </summary>
/// <remarks>
///     This class is organized across three partial files for maintainability:
///     - <c>PipelineBuilder.cs</c>: Core state, node registration, graph connections, and naming helpers
///     - <c>PipelineBuilder.Configuration.cs</c>: Configuration methods (With*, Add*), validation settings, and internal lineage construction
///     - <c>PipelineBuilder.Build.cs</c>: Pipeline building and validation (Build, TryBuild)
/// </remarks>
public sealed partial class PipelineBuilder
{
    // Track disposables created during builder configuration so they can be transferred to the PipelineContext at execution time.
    private readonly List<IAsyncDisposable> _builderDisposables = [];
    private readonly List<IGraphRule> _customValidationRules = [];

    // Flag to prevent builder reuse after Build() has been called
    private bool _built;

    // State objects encapsulating related fields by concern

    private BuilderConfig _config = BuilderConfig.Default;
    internal IReadOnlyList<IAsyncDisposable> BuilderDisposables => _builderDisposables;

    // Internal properties for testing access to state objects
    internal BuilderNodeState NodeState { get; } = new();

    internal BuilderConnectionState ConnectionState { get; } = new();

    internal BuilderConfigurationState ConfigurationState { get; } = new();

    internal void RegisterBuilderDisposable(object instance)
    {
        switch (instance)
        {
            case IAsyncDisposable asyncDisp:
                _builderDisposables.Add(asyncDisp);
                break;
            case IDisposable disp:
                _builderDisposables.Add(new BuilderDisposableWrapper(disp));
                break;
        }
    }

    private sealed class BuilderDisposableWrapper(IDisposable inner) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            try
            {
                inner.Dispose();
            }
            catch
            {
                /* swallow builder-phase disposal errors */
            }

            return ValueTask.CompletedTask;
        }
    }

    internal sealed record BuilderConfig(
        bool ExtendedValidation,
        bool EarlyNameValidation,
        bool ItemLevelLineageEnabled,
        GraphValidationMode GraphValidationMode,
        PipelineCircuitBreakerOptions? CircuitBreakerOptions,
        CircuitBreakerMemoryManagementOptions? CircuitBreakerMemoryOptions,
        LineageOptions? LineageOptions,
        PipelineRetryOptions RetryOptions)
    {
        /// <summary>
        ///     Creates a new BuilderConfig with default lineage values.
        /// </summary>
        /// <remarks>
        ///     This record groups all configuration state for building pipelines,
        ///     including error handling, lineage, and execution options.
        ///     Extended validation is enabled by default for safety.
        /// </remarks>
        public static BuilderConfig Default => new(
            true,
            true,
            false,
            GraphValidationMode.Error,
            null,
            null,
            null,
            PipelineRetryOptions.Default);
    }

    #region Node Registration

    /// <summary>
    ///     Adds a source node to the pipeline using a compile-time node type.
    /// </summary>
    /// <param name="name">Optional node name. If not provided, an auto-generated name will be used.</param>
    public SourceNodeHandle<TOut> AddSource<TNode, TOut>(string? name = null) where TNode : ISourceNode<TOut>
    {
        name ??= GenerateUniqueNodeName(typeof(TNode).Name);
        return RegisterNode(name, NodeKind.Source, typeof(TNode), null, typeof(TOut), static (id, def) => new SourceNodeHandle<TOut>(id));
    }

    /// <summary>
    ///     Adds a source node to the pipeline using a runtime node type. This is intended for scenarios
    ///     where the node instance has already been created and the concrete type is only known at runtime.
    /// </summary>
    /// <param name="nodeType">Concrete source node type.</param>
    /// <param name="name">Optional node name. If not provided, an auto-generated name will be used.</param>
    /// <exception cref="ArgumentException">Thrown if <paramref name="nodeType" /> does not implement <see cref="ISourceNode{TOut}" />.</exception>
    public SourceNodeHandle<TOut> AddSource<TOut>(Type nodeType, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(nodeType);

        if (!typeof(ISourceNode<TOut>).IsAssignableFrom(nodeType))
            throw new ArgumentException($"Type {nodeType.FullName} must implement ISourceNode<{typeof(TOut).Name}>", nameof(nodeType));

        name ??= GenerateUniqueNodeName(nodeType.Name);
        return RegisterNode(name, NodeKind.Source, nodeType, null, typeof(TOut), static (id, def) => new SourceNodeHandle<TOut>(id));
    }

    /// <summary>
    ///     Adds a transform node to the pipeline.
    /// </summary>
    /// <param name="name">Optional node name. If not provided, an auto-generated name will be used.</param>
    public TransformNodeHandle<TIn, TOut> AddTransform<TNode, TIn, TOut>(string? name = null) where TNode : ITransformNode<TIn, TOut>
    {
        name ??= GenerateUniqueNodeName(typeof(TNode).Name);
        return RegisterNode(name, NodeKind.Transform, typeof(TNode), typeof(TIn), typeof(TOut), static (id, def) => new TransformNodeHandle<TIn, TOut>(id));
    }

    /// <summary>
    ///     Adds a stream transform node to the pipeline.
    /// </summary>
    /// <param name="name">Optional node name. If not provided, an auto-generated name will be used.</param>
    public TransformNodeHandle<TIn, TOut> AddStreamTransform<TNode, TIn, TOut>(string? name = null) where TNode : IStreamTransformNode<TIn, TOut>
    {
        name ??= GenerateUniqueNodeName(typeof(TNode).Name);
        return RegisterNode(name, NodeKind.StreamTransform, typeof(TNode), typeof(TIn), typeof(TOut), static (id, def) => new TransformNodeHandle<TIn, TOut>(id));
    }

    /// <summary>
    ///     Adds a transform node with a specific <see cref="NodeKind" /> override.
    ///     Used by extension methods (Tap, Branch, Lookup, Composite) that are semantically distinct from generic transforms.
    /// </summary>
    internal TransformNodeHandle<TIn, TOut> AddTransformWithKind<TNode, TIn, TOut>(NodeKind kind, string? name = null) where TNode : ITransformNode<TIn, TOut>
    {
        name ??= GenerateUniqueNodeName(typeof(TNode).Name);
        return RegisterNode(name, kind, typeof(TNode), typeof(TIn), typeof(TOut), static (id, def) => new TransformNodeHandle<TIn, TOut>(id));
    }

    /// <summary>
    ///     Adds a source node with a specific <see cref="NodeKind" /> override.
    ///     Used by extension methods (CompositeInput) that are semantically distinct from generic sources.
    /// </summary>
    internal SourceNodeHandle<TOut> AddSourceWithKind<TNode, TOut>(NodeKind kind, string? name = null) where TNode : ISourceNode<TOut>
    {
        name ??= GenerateUniqueNodeName(typeof(TNode).Name);
        return RegisterNode(name, kind, typeof(TNode), null, typeof(TOut), static (id, def) => new SourceNodeHandle<TOut>(id));
    }

    /// <summary>
    ///     Adds a sink node with a specific <see cref="NodeKind" /> override.
    ///     Used by extension methods (CompositeOutput) that are semantically distinct from generic sinks.
    /// </summary>
    internal SinkNodeHandle<TIn> AddSinkWithKind<TNode, TIn>(NodeKind kind, string? name = null) where TNode : ISinkNode<TIn>
    {
        name ??= GenerateUniqueNodeName(typeof(TNode).Name);
        return RegisterNode(name, kind, typeof(TNode), typeof(TIn), null, static (id, def) => new SinkNodeHandle<TIn>(id));
    }

    /// <summary>
    ///     Adds a stream transform node with a specific <see cref="NodeKind" /> override.
    ///     Used by extension methods (Batcher, Unbatcher) that are semantically distinct from generic stream transforms.
    /// </summary>
    internal TransformNodeHandle<TIn, TOut> AddStreamTransformWithKind<TNode, TIn, TOut>(NodeKind kind, string? name = null) where TNode : IStreamTransformNode<TIn, TOut>
    {
        name ??= GenerateUniqueNodeName(typeof(TNode).Name);
        return RegisterNode(name, kind, typeof(TNode), typeof(TIn), typeof(TOut), static (id, def) => new TransformNodeHandle<TIn, TOut>(id));
    }

    /// <summary>
    ///     Adds a sink node to the pipeline using a compile-time node type.
    /// </summary>
    /// <param name="name">Optional node name. If not provided, an auto-generated name will be used.</param>
    public SinkNodeHandle<TIn> AddSink<TNode, TIn>(string? name = null) where TNode : ISinkNode<TIn>
    {
        name ??= GenerateUniqueNodeName(typeof(TNode).Name);
        return RegisterNode(name, NodeKind.Sink, typeof(TNode), typeof(TIn), null, static (id, def) => new SinkNodeHandle<TIn>(id));
    }

    /// <summary>
    ///     Adds a sink node to the pipeline using a runtime node type. This is intended for scenarios
    ///     where the node instance has already been created and the concrete type is only known at runtime.
    /// </summary>
    /// <param name="nodeType">Concrete sink node type.</param>
    /// <param name="name">Optional node name. If not provided, an auto-generated name will be used.</param>
    /// <exception cref="ArgumentException">Thrown if <paramref name="nodeType" /> does not implement <see cref="ISinkNode{TIn}" />.</exception>
    public SinkNodeHandle<TIn> AddSink<TIn>(Type nodeType, string? name = null)
    {
        ArgumentNullException.ThrowIfNull(nodeType);

        if (!typeof(ISinkNode<TIn>).IsAssignableFrom(nodeType))
            throw new ArgumentException($"Type {nodeType.FullName} must implement ISinkNode<{typeof(TIn).Name}>", nameof(nodeType));

        name ??= GenerateUniqueNodeName(nodeType.Name);
        return RegisterNode(name, NodeKind.Sink, nodeType, typeof(TIn), null, static (id, def) => new SinkNodeHandle<TIn>(id));
    }

    /// <summary>
    ///     Adds a join node to the pipeline.
    /// </summary>
    /// <param name="name">Optional node name. If not provided, an auto-generated name will be used.</param>
    public JoinNodeHandle<TIn1, TIn2, TOut> AddJoin<TNode, TIn1, TIn2, TOut>(string? name = null) where TNode : IJoinNode
    {
        name ??= GenerateUniqueNodeName(typeof(TNode).Name);
        return RegisterNode(name, NodeKind.Join, typeof(TNode), typeof(TIn1), typeof(TOut), static (id, def) => new JoinNodeHandle<TIn1, TIn2, TOut>(id));
    }

    /// <summary>
    ///     Adds an aggregate node to the pipeline.
    /// </summary>
    /// <param name="name">Optional node name. If not provided, an auto-generated name will be used.</param>
    public AggregateNodeHandle<TIn, TResult> AddAggregate<TNode, TIn, TKey, TAccumulate, TResult>(string? name = null)
        where TNode : IAggregateNode where TKey : notnull
    {
        name ??= GenerateUniqueNodeName(typeof(TNode).Name);

        return RegisterNode(name, NodeKind.Aggregate, typeof(TNode), typeof(TIn), typeof(TResult),
            static (id, def) => new AggregateNodeHandle<TIn, TResult>(id));
    }

    /// <summary>
    ///     Adds a simplified aggregate node to the pipeline where the accumulator and result types are the same.
    /// </summary>
    /// <param name="name">Optional node name. If not provided, an auto-generated name will be used.</param>
    public AggregateNodeHandle<TIn, TResult> AddAggregate<TNode, TIn, TKey, TResult>(string? name = null)
        where TNode : IAggregateNode where TKey : notnull
    {
        name ??= GenerateUniqueNodeName(typeof(TNode).Name);

        return RegisterNode(name, NodeKind.Aggregate, typeof(TNode), typeof(TIn), typeof(TResult),
            static (id, def) => new AggregateNodeHandle<TIn, TResult>(id));
    }

    /// <summary>
    ///     Registers a pre-configured node instance to be used at execution time.
    /// </summary>
    public PipelineBuilder AddPreconfiguredNodeInstance(string nodeId, INode instance)
    {
        if (!NodeState.Nodes.ContainsKey(nodeId))
            throw new InvalidOperationException(ErrorMessages.PreConfiguredInstanceNodeNotFound(nodeId));

        if (NodeState.PreconfiguredNodeInstances.TryGetValue(nodeId, out var existing))
        {
            if (!ReferenceEquals(existing, instance))
                throw new InvalidOperationException(ErrorMessages.PreConfiguredInstanceAlreadyAdded(nodeId));

            return this;
        }

        NodeState.PreconfiguredNodeInstances[nodeId] = instance;
        return this;
    }

    /// <summary>
    ///     Sets or replaces a pre-configured node instance for the specified node.
    ///     Unlike <see cref="AddPreconfiguredNodeInstance" />, this method allows replacing
    ///     an existing instance, which is useful for runtime orchestration scenarios
    ///     (e.g., injecting run-scoped composite node instances).
    /// </summary>
    /// <param name="nodeId">The ID of the node to set the instance for.</param>
    /// <param name="instance">The node instance to use at execution time.</param>
    /// <param name="replaceExisting">
    ///     If true (default), replaces any existing preconfigured instance.
    ///     If false, behaves like <see cref="AddPreconfiguredNodeInstance" /> and throws on duplicate.
    /// </param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <exception cref="InvalidOperationException">
    ///     Thrown if the node has not been added to the builder, or if <paramref name="replaceExisting" />
    ///     is false and a different instance is already registered.
    /// </exception>
    public PipelineBuilder SetPreconfiguredNodeInstance(string nodeId, INode instance, bool replaceExisting = true)
    {
        ArgumentNullException.ThrowIfNull(nodeId);
        ArgumentNullException.ThrowIfNull(instance);

        if (!NodeState.Nodes.ContainsKey(nodeId))
            throw new InvalidOperationException(ErrorMessages.PreConfiguredInstanceNodeNotFound(nodeId));

        if (!replaceExisting && NodeState.PreconfiguredNodeInstances.TryGetValue(nodeId, out var existing) &&
            !ReferenceEquals(existing, instance))
        {
            throw new InvalidOperationException(ErrorMessages.PreConfiguredInstanceAlreadyAdded(nodeId));
        }

        NodeState.PreconfiguredNodeInstances[nodeId] = instance;
        return this;
    }

    /// <summary>
    ///     Sets the child pipeline definition type on a composite node.
    /// </summary>
    /// <param name="nodeId">The ID of the composite node.</param>
    /// <param name="childDefinitionType">The <see cref="IPipelineDefinition" /> type of the child sub-pipeline.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the node does not exist.</exception>
    public PipelineBuilder SetNodeChildDefinitionType(string nodeId, Type childDefinitionType)
    {
        ArgumentNullException.ThrowIfNull(childDefinitionType);

        if (!typeof(IPipelineDefinition).IsAssignableFrom(childDefinitionType))
        {
            throw new ArgumentException(
                $"Type '{childDefinitionType.FullName}' must implement {nameof(IPipelineDefinition)}.",
                nameof(childDefinitionType));
        }

        if (!NodeState.Nodes.TryGetValue(nodeId, out var existing))
            throw new InvalidOperationException($"Node '{nodeId}' not found. SetNodeChildDefinitionType must be called after adding the node.");

        NodeState.Nodes[nodeId] = existing with { ChildDefinitionType = childDefinitionType };
        return this;
    }

    /// <summary>
    ///     Sets a metadata value on a node definition.
    /// </summary>
    /// <param name="nodeId">The ID of the node.</param>
    /// <param name="key">The metadata key.</param>
    /// <param name="value">The metadata value.</param>
    /// <returns>The builder instance for method chaining.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the node does not exist.</exception>
    public PipelineBuilder SetNodeMetadata(string nodeId, string key, object value)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);

        if (!NodeState.Nodes.TryGetValue(nodeId, out var existing))
            throw new InvalidOperationException($"Node '{nodeId}' not found. SetNodeMetadata must be called after adding the node.");

        var metadata = existing.Metadata ?? ImmutableDictionary<string, object>.Empty;
        metadata = metadata.SetItem(key, value);
        NodeState.Nodes[nodeId] = existing with { Metadata = metadata };
        return this;
    }

    /// <summary>
    ///     Core node registration logic that handles all node types.
    /// </summary>
    private THandle RegisterNode<THandle>(string name, NodeKind kind, Type nodeType, Type? inType, Type? outType,
        Func<string, NodeDefinition, THandle> handleFactory)
    {
        if (_config.EarlyNameValidation)
            EnsureUniqueName(name);

        var meta = NodeMetadataCache.Get(nodeType);
        var id = GenerateIdFromName(name);
        LineageAdapterDelegate? lineageAdapter = null;
        SinkLineageUnwrapDelegate? sinkUnwrap = null;
        CustomMergeDelegate? customMerge = null;
        var isJoin = false;

        switch (kind)
        {
            case NodeKind.Transform:
            case NodeKind.StreamTransform:
            case NodeKind.Tap:
            case NodeKind.Branch:
            case NodeKind.Lookup:
            case NodeKind.Batch:
                lineageAdapter = BuildLineageAdapterReflection(inType, outType, meta.LineageMapperType);

                if (meta.HasCustomMerge)
                    customMerge = BuildCustomMergeDelegate(nodeType);

                break;
            case NodeKind.Sink:
                sinkUnwrap = BuildSinkLineageUnwrapReflection(inType);

                if (meta.HasCustomMerge)
                    customMerge = BuildCustomMergeDelegate(nodeType);

                break;
            case NodeKind.Join:
                lineageAdapter = BuildLineageAdapterReflection(inType, outType, meta.LineageMapperType);

                // Pre-compile join key selectors during builder phase for zero-overhead runtime access
                // Extract join key type and input types from the join node's generic hierarchy
                var joinInputTypes = ExtractJoinInputTypes(nodeType);

                if (joinInputTypes.HasValue)
                {
                    var keyType = ExtractJoinKeyType(nodeType);

                    if (keyType is not null)
                    {
                        var (in1Type, in2Type) = joinInputTypes.Value;
                        var (sel1, sel2) = JoinKeySelectorRegistry.Compile(nodeType, keyType, in1Type, in2Type);

                        // Register pre-compiled selectors in the runtime registry
                        if (sel1 is not null && sel2 is not null)
                            JoinKeySelectorRegistry.Register(nodeType, sel1, sel2);
                    }
                }

                if (meta.HasCustomMerge)
                    customMerge = BuildCustomMergeDelegate(nodeType);

                isJoin = true;
                break;
            case NodeKind.Aggregate:
            case NodeKind.Source:
            case NodeKind.CompositeInput:
                // nothing extra
                if (meta.HasCustomMerge)
                    customMerge = BuildCustomMergeDelegate(nodeType);

                break;
            case NodeKind.Composite:
                lineageAdapter = BuildLineageAdapterReflection(inType, outType, meta.LineageMapperType);

                if (meta.HasCustomMerge)
                    customMerge = BuildCustomMergeDelegate(nodeType);

                break;
            case NodeKind.CompositeOutput:
                sinkUnwrap = BuildSinkLineageUnwrapReflection(inType);

                if (meta.HasCustomMerge)
                    customMerge = BuildCustomMergeDelegate(nodeType);

                break;
        }

        var def = new NodeDefinition(
            id,
            name,
            nodeType,
            kind,
            inType,
            outType,
            DeclaredCardinality: meta.DeclaredCardinality,
            MergeStrategy: meta.MergeStrategy,
            HasCustomMerge: meta.HasCustomMerge,
            IsJoin: isJoin,
            CustomMerge: customMerge,
            LineageAdapter: lineageAdapter,
            LineageMapperType: meta.LineageMapperType,
            SinkLineageUnwrap: sinkUnwrap);

        if (!NodeState.Nodes.TryAdd(id, def))
            throw new ArgumentException(ErrorMessages.NodeAlreadyAdded(id), id);

        return handleFactory(id, def);
    }

    private static LineageAdapterDelegate BuildLineageAdapterReflection(Type? inType, Type? outType, Type? lineageMapperType)
    {
        if (inType is null || outType is null)
            throw new InvalidOperationException("Lineage adapter creation requires non-null input and output types.");

        var method = typeof(PipelineBuilder).GetMethod("BuildLineageAdapter", BindingFlags.NonPublic | BindingFlags.Static)
                     ?? throw new InvalidOperationException("Internal method 'BuildLineageAdapter' was not found on PipelineBuilder.");

        try
        {
            var result = method.MakeGenericMethod(inType, outType).Invoke(null, [lineageMapperType]);

            return result as LineageAdapterDelegate
                   ?? throw new InvalidOperationException("Internal method 'BuildLineageAdapter' returned an unexpected delegate type.");
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            throw new InvalidOperationException(
                $"Failed to create lineage adapter for input '{inType}' and output '{outType}'.",
                ex.InnerException);
        }
    }

    private static SinkLineageUnwrapDelegate BuildSinkLineageUnwrapReflection(Type? inType)
    {
        if (inType is null)
            throw new InvalidOperationException("Sink lineage unwrap creation requires a non-null input type.");

        var method = typeof(PipelineBuilder).GetMethod("BuildSinkLineageUnwrapDelegate", BindingFlags.NonPublic | BindingFlags.Static)
                     ?? throw new InvalidOperationException("Internal method 'BuildSinkLineageUnwrapDelegate' was not found on PipelineBuilder.");

        try
        {
            var result = method.MakeGenericMethod(inType).Invoke(null, null);

            return result as SinkLineageUnwrapDelegate
                   ?? throw new InvalidOperationException("Internal method 'BuildSinkLineageUnwrapDelegate' returned an unexpected delegate type.");
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            throw new InvalidOperationException(
                $"Failed to create sink lineage unwrap delegate for input '{inType}'.",
                ex.InnerException);
        }
    }

    /// <summary>
    ///     Extracts join input types from a join node's generic hierarchy.
    /// </summary>
    private static (Type, Type)? ExtractJoinInputTypes(Type joinNodeType)
    {
        // Join nodes derive from BaseJoinNode<TKey, TIn1, TIn2, TOut>
        var baseType = joinNodeType.BaseType;

        while (baseType is not null)
        {
            if (baseType.IsGenericType)
            {
                var genericDef = baseType.GetGenericTypeDefinition();

                // Check if this is BaseJoinNode<TKey, TIn1, TIn2, TOut>
                if (genericDef.Name == "BaseJoinNode`4")
                {
                    var args = baseType.GetGenericArguments();

                    // args[0] = TKey, args[1] = TIn1, args[2] = TIn2, args[3] = TOut
                    return (args[1], args[2]);
                }
            }

            baseType = baseType.BaseType;
        }

        return null;
    }

    /// <summary>
    ///     Extracts the key type from a join node's generic hierarchy.
    /// </summary>
    private static Type? ExtractJoinKeyType(Type joinNodeType)
    {
        // Join nodes derive from BaseJoinNode<TKey, TIn1, TIn2, TOut>
        var baseType = joinNodeType.BaseType;

        while (baseType is not null)
        {
            if (baseType.IsGenericType)
            {
                var genericDef = baseType.GetGenericTypeDefinition();

                // Check if this is BaseJoinNode<TKey, TIn1, TIn2, TOut>
                if (genericDef.Name == "BaseJoinNode`4")
                {
                    var args = baseType.GetGenericArguments();

                    // args[0] = TKey, args[1] = TIn1, args[2] = TIn2, args[3] = TOut
                    return args[0];
                }
            }

            baseType = baseType.BaseType;
        }

        return null;
    }

    #endregion

    #region Graph Connections

    /// <summary>
    ///     Connects an output node to an input node with matching data types.
    /// </summary>
    /// <typeparam name="TData">The data type flowing between the connected nodes.</typeparam>
    /// <param name="source">The output node handle (source of data).</param>
    /// <param name="target">The input node handle (sink of data).</param>
    /// <param name="sourceOutputName">Optional name of the source node's output channel.</param>
    /// <param name="targetInputName">Optional name of the target node's input channel.</param>
    /// <returns>The builder instance for method chaining.</returns>
    public PipelineBuilder Connect<TData>(
        IOutputNodeHandle<TData> source,
        IInputNodeHandle<TData> target,
        string? sourceOutputName = null,
        string? targetInputName = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(target);
        ConnectionState.Edges.Add(new Edge(source.Id, target.Id, sourceOutputName, targetInputName));
        return this;
    }

    /// <summary>
    ///     Connects a source/transform/aggregate node to a join node's first input.
    ///     The join node type parameter order is JoinNodeHandle&lt;TIn1, TIn2, TOut&gt;.
    /// </summary>
    /// <remarks>
    ///     This overload is used when connecting to a join where the source data type matches TIn1 (first input).
    ///     The type parameters ensure compile-time verification that the output type matches the join's input type.
    ///     Note: If a join has identical input types (TIn1 == TIn2), overload resolution will be ambiguous
    ///     and the compiler will report an error. This is by design - joins with identical input types should
    ///     differentiate their inputs through separate source node instances with distinct identities (e.g., s1, s2).
    /// </remarks>
    public PipelineBuilder Connect<TData, TIn2, TOut>(
        IOutputNodeHandle<TData> source,
        JoinNodeHandle<TData, TIn2, TOut> target,
        string? sourceOutputName = null,
        string? targetInputName = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(target);
        ConnectionState.Edges.Add(new Edge(source.Id, target.Id, sourceOutputName, targetInputName));
        return this;
    }

    /// <summary>
    ///     Connects a source/transform/aggregate node to a join node's second input.
    ///     The join node type parameter order is JoinNodeHandle&lt;TIn1, TIn2, TOut&gt;.
    /// </summary>
    /// <remarks>
    ///     This overload is used when connecting to a join where the source data type matches TIn2 (second input).
    ///     The type parameters ensure compile-time verification that the output type matches the join's input type.
    ///     Note: If a join has identical input types (TIn1 == TIn2), overload resolution will be ambiguous
    ///     and the compiler will report an error. This is by design - joins with identical input types should
    ///     differentiate their inputs through separate source node instances with distinct identities (e.g., s1, s2).
    /// </remarks>
    public PipelineBuilder Connect<TIn1, TData, TOut>(
        IOutputNodeHandle<TData> source,
        JoinNodeHandle<TIn1, TData, TOut> target,
        string? sourceOutputName = null,
        string? targetInputName = null)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(target);
        ConnectionState.Edges.Add(new Edge(source.Id, target.Id, sourceOutputName, targetInputName));
        return this;
    }

    #endregion

    #region Private Naming Helpers

    /// <summary>
    ///     Generates a unique ID from a node name, ensuring uniqueness in the current builder state.
    /// </summary>
    private string GenerateIdFromName(string name)
    {
        return NodeNameGenerator.GenerateIdFromName(name, NodeState.Nodes);
    }

    /// <summary>
    ///     Generates a unique node name by appending a suffix if necessary to avoid conflicts.
    /// </summary>
    private string GenerateUniqueNodeName(string baseName)
    {
        return NodeNameGenerator.GenerateUniqueNodeName(baseName, NodeState.Nodes.Values);
    }

    /// <summary>
    ///     Validates that a node name is unique; throws if a duplicate is found.
    /// </summary>
    private void EnsureUniqueName(string name)
    {
        NodeNameGenerator.EnsureUniqueName(name, NodeState.Nodes.Values);
    }

    /// <summary>
    ///     Builds custom merge delegates for nodes implementing either <see cref="ICustomMergeNodeUntyped" /> or
    ///     <c>ICustomMergeNode&lt;TIn&gt;</c>. Reflection is performed once per merge node at build time.
    /// </summary>
    private static CustomMergeDelegate BuildCustomMergeDelegate(Type nodeType)
    {
        if (typeof(ICustomMergeNodeUntyped).IsAssignableFrom(nodeType))
            return async (node, pipes, ct) => await ((ICustomMergeNodeUntyped)node).MergeAsyncUntyped(pipes, ct).ConfigureAwait(false);

        var genericIface = nodeType.GetInterfaces()
                               .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ICustomMergeNode<>))
                           ?? throw new InvalidOperationException($"Custom merge node '{nodeType.Name}' does not implement expected generic interface.");

        var inType = genericIface.GetGenericArguments()[0];

        var helper = typeof(PipelineBuilder).GetMethod(nameof(BuildStronglyTypedCustomMergeDelegate), BindingFlags.NonPublic | BindingFlags.Static)!
            .MakeGenericMethod(inType);

        return (CustomMergeDelegate)helper.Invoke(null, null)!;
    }

    private static CustomMergeDelegate BuildStronglyTypedCustomMergeDelegate<TIn>()
    {
        return async (node, pipes, ct) => await ((ICustomMergeNode<TIn>)node).MergeAsync(pipes, ct).ConfigureAwait(false);
    }

    #endregion
}
