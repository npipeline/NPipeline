using System.Reflection;
using NPipeline.Configuration;
using NPipeline.Execution.Lineage;
using NPipeline.Graph;
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
    private readonly List<IAsyncDisposable> _builderDisposables = new();
    private readonly List<IGraphRule> _customValidationRules = [];

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
        /// </remarks>
        public static BuilderConfig Default => new(
            false,
            false,
            false,
            GraphValidationMode.Error,
            null,
            null,
            null,
            PipelineRetryOptions.Default);
    }

    #region Node Registration

    /// <summary>
    ///     Adds a source node to the pipeline.
    /// </summary>
    /// <param name="name">Optional node name. If not provided, an auto-generated name will be used.</param>
    public SourceNodeHandle<TOut> AddSource<TNode, TOut>(string? name = null) where TNode : ISourceNode<TOut>
    {
        name ??= GenerateUniqueNodeName(typeof(TNode).Name);
        return RegisterNode(name, NodeKind.Source, typeof(TNode), null, typeof(TOut), static (id, def) => new SourceNodeHandle<TOut>(id));
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
    ///     Adds a sink node to the pipeline.
    /// </summary>
    /// <param name="name">Optional node name. If not provided, an auto-generated name will be used.</param>
    public SinkNodeHandle<TIn> AddSink<TNode, TIn>(string? name = null) where TNode : ISinkNode<TIn>
    {
        name ??= GenerateUniqueNodeName(typeof(TNode).Name);
        return RegisterNode(name, NodeKind.Sink, typeof(TNode), typeof(TIn), null, static (id, def) => new SinkNodeHandle<TIn>(id));
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
                lineageAdapter = (LineageAdapterDelegate)typeof(PipelineBuilder).GetMethod("BuildLineageAdapter", BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(inType!, outType!).Invoke(null, [meta.LineageMapperType])!;

                if (meta.HasCustomMerge)
                    customMerge = CustomMergeDelegateFactory.Build(nodeType);

                break;
            case NodeKind.Sink:
                sinkUnwrap = (SinkLineageUnwrapDelegate)typeof(PipelineBuilder).GetMethod("BuildSinkLineageUnwrapDelegate",
                        BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(inType!).Invoke(null, null)!;

                if (meta.HasCustomMerge)
                    customMerge = CustomMergeDelegateFactory.Build(nodeType);

                break;
            case NodeKind.Join:
                lineageAdapter = (LineageAdapterDelegate)typeof(PipelineBuilder).GetMethod("BuildLineageAdapter", BindingFlags.NonPublic | BindingFlags.Static)!
                    .MakeGenericMethod(inType!, outType!).Invoke(null, [meta.LineageMapperType])!;

                // Pre-compile join key selectors during builder phase for zero-overhead runtime access
                // Extract join key type and input types from the join node's generic hierarchy
                var joinInputTypes = ExtractJoinInputTypes(nodeType);

                if (joinInputTypes.HasValue)
                {
                    var keyType = ExtractJoinKeyType(nodeType);

                    if (keyType is not null)
                    {
                        var (in1Type, in2Type) = joinInputTypes.Value;
                        var (sel1, sel2) = JoinKeySelectorCompiler.Compile(nodeType, keyType, in1Type, in2Type);

                        // Register pre-compiled selectors in the runtime registry
                        if (sel1 is not null && sel2 is not null)
                            JoinKeySelectorRegistry.Register(nodeType, sel1, sel2);
                    }
                }

                if (meta.HasCustomMerge)
                    customMerge = CustomMergeDelegateFactory.Build(nodeType);

                isJoin = true;
                break;
            case NodeKind.Aggregate:
            case NodeKind.Source:
                // nothing extra
                if (meta.HasCustomMerge)
                    customMerge = CustomMergeDelegateFactory.Build(nodeType);

                break;
        }

        var def = new NodeDefinition(id, name, nodeType, kind, InputType: inType, OutputType: outType,
            MergeStrategy: meta.MergeStrategy, HasCustomMerge: meta.HasCustomMerge, DeclaredCardinality: meta.DeclaredCardinality,
            LineageAdapter: lineageAdapter, SinkLineageUnwrap: sinkUnwrap, LineageMapperType: meta.LineageMapperType, IsJoin: isJoin, CustomMerge: customMerge);

        if (!NodeState.Nodes.TryAdd(id, def))
            throw new ArgumentException(ErrorMessages.NodeAlreadyAdded(id), id);

        return handleFactory(id, def);
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
    ///     This method uses marker interfaces to ensure compile-time type safety while reducing API surface.
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

    #endregion
}
