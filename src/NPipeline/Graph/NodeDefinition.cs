using NPipeline.Attributes.Lineage;
using NPipeline.Execution;
using NPipeline.Graph.PipelineDelegates;
using NPipeline.Nodes;

namespace NPipeline.Graph;

/// <summary>
///     Specifies the type of a node in the pipeline.
/// </summary>
public enum NodeKind
{
    /// <summary>A source node that produces data.</summary>
    Source,

    /// <summary>A transform node that processes data.</summary>
    Transform,

    /// <summary>A sink node that consumes data.</summary>
    Sink,

    /// <summary>A join node that combines multiple streams.</summary>
    Join,

    /// <summary>An aggregate node that combines multiple data items.</summary>
    Aggregate,
}

/// <summary>
///     Represents the identity information of a node.
/// </summary>
/// <param name="Id">The unique identifier for the node.</param>
/// <param name="Name">A descriptive name for the node.</param>
public sealed record NodeIdentity(
    string Id,
    string Name);

/// <summary>
///     Represents the type system information of a node.
/// </summary>
/// <param name="NodeType">The type of the pipeline node, which must implement <see cref="INode" />.</param>
/// <param name="Kind">The kind of node (Source, Transform, Sink, Join, or Aggregate).</param>
/// <param name="InputType">The type of input data for the node.</param>
/// <param name="OutputType">The type of output data for the node.</param>
public sealed record NodeTypeSystem(
    Type NodeType,
    NodeKind Kind,
    Type? InputType = null,
    Type? OutputType = null);

/// <summary>
///     Represents the execution configuration of a node.
/// </summary>
/// <param name="ExecutionStrategy">The execution strategy for the node. If null, a default strategy will be used.</param>
/// <param name="ErrorHandlerType">The type of the error handler for the node. If null, no specific error handler is attached.</param>
/// <param name="DeclaredCardinality">The declared cardinality transformation of the node.</param>
public sealed record NodeExecutionConfig(
    IExecutionStrategy? ExecutionStrategy = null,
    Type? ErrorHandlerType = null,
    TransformCardinality? DeclaredCardinality = null);

/// <summary>
///     Represents the merge configuration of a node.
/// </summary>
/// <param name="MergeStrategy">The merge strategy for nodes that combine multiple inputs.</param>
/// <param name="HasCustomMerge">Whether this node has custom merge logic.</param>
/// <param name="IsJoin">Whether this node is a join node.</param>
/// <param name="CustomMerge">Optional custom merge delegate for merging multiple input streams.</param>
public sealed record NodeMergeConfig(
    MergeType? MergeStrategy = null,
    bool HasCustomMerge = false,
    bool IsJoin = false,
    CustomMergeDelegate? CustomMerge = null);

/// <summary>
///     Represents the lineage configuration of a node.
/// </summary>
/// <param name="LineageAdapter">Optional delegate for transforming lineage data.</param>
/// <param name="LineageMapperType">The type of the lineage mapper for non-1:1 transformations.</param>
/// <param name="SinkLineageUnwrap">Optional delegate for unwrapping lineage in sink nodes.</param>
public sealed record NodeLineageConfig(
    LineageAdapterDelegate? LineageAdapter = null,
    Type? LineageMapperType = null,
    SinkLineageUnwrapDelegate? SinkLineageUnwrap = null);

/// <summary>
///     Represents the definition of a node within the pipeline graph.
/// </summary>
/// <remarks>
///     This record uses a nested configuration structure to organize node properties logically.
///     Pre-compiled join key selectors are stored in JoinKeySelectorRegistry and accessed at runtime,
///     not duplicated in this record. This keeps the record lightweight and makes the registry the
///     single source of truth for cached selectors.
/// </remarks>
/// <param name="Identity">The identity information of the node.</param>
/// <param name="TypeSystem">The type system information of the node.</param>
/// <param name="ExecutionConfig">The execution configuration of the node.</param>
/// <param name="MergeConfig">The merge configuration of the node.</param>
/// <param name="LineageConfig">The lineage configuration of the node.</param>
public sealed record NodeDefinition(
    NodeIdentity Identity,
    NodeTypeSystem TypeSystem,
    NodeExecutionConfig ExecutionConfig,
    NodeMergeConfig MergeConfig,
    NodeLineageConfig LineageConfig)
{
    /// <summary>
    ///     Gets the unique identifier for the node.
    /// </summary>
    public string Id => Identity.Id;

    /// <summary>
    ///     Gets a descriptive name for the node.
    /// </summary>
    public string Name => Identity.Name;

    /// <summary>
    ///     Gets the type of the pipeline node.
    /// </summary>
    public Type NodeType => TypeSystem.NodeType;

    /// <summary>
    ///     Gets the kind of node.
    /// </summary>
    public NodeKind Kind => TypeSystem.Kind;

    /// <summary>
    ///     Gets the type of input data for the node.
    /// </summary>
    public Type? InputType => TypeSystem.InputType;

    /// <summary>
    ///     Gets the type of output data for the node.
    /// </summary>
    public Type? OutputType => TypeSystem.OutputType;

    /// <summary>
    ///     Gets the execution strategy for the node.
    /// </summary>
    public IExecutionStrategy? ExecutionStrategy => ExecutionConfig.ExecutionStrategy;

    /// <summary>
    ///     Gets the type of the error handler for the node.
    /// </summary>
    public Type? ErrorHandlerType => ExecutionConfig.ErrorHandlerType;

    /// <summary>
    ///     Gets the declared cardinality transformation of the node.
    /// </summary>
    public TransformCardinality? DeclaredCardinality => ExecutionConfig.DeclaredCardinality;

    /// <summary>
    ///     Gets the merge strategy for nodes that combine multiple inputs.
    /// </summary>
    public MergeType? MergeStrategy => MergeConfig.MergeStrategy;

    /// <summary>
    ///     Gets whether this node has custom merge logic.
    /// </summary>
    public bool HasCustomMerge => MergeConfig.HasCustomMerge;

    /// <summary>
    ///     Gets whether this node is a join node.
    /// </summary>
    public bool IsJoin => MergeConfig.IsJoin;

    /// <summary>
    ///     Gets the custom merge delegate for merging multiple input streams.
    /// </summary>
    public CustomMergeDelegate? CustomMerge => MergeConfig.CustomMerge;

    /// <summary>
    ///     Gets the delegate for transforming lineage data.
    /// </summary>
    public LineageAdapterDelegate? LineageAdapter => LineageConfig.LineageAdapter;

    /// <summary>
    ///     Gets the type of the lineage mapper for non-1:1 transformations.
    /// </summary>
    public Type? LineageMapperType => LineageConfig.LineageMapperType;

    /// <summary>
    ///     Gets the delegate for unwrapping lineage in sink nodes.
    /// </summary>
    public SinkLineageUnwrapDelegate? SinkLineageUnwrap => LineageConfig.SinkLineageUnwrap;

    /// <summary>
    ///     Creates a new NodeDefinition with updated execution strategy.
    /// </summary>
    /// <param name="executionStrategy">The new execution strategy.</param>
    /// <returns>A new NodeDefinition with the updated execution strategy.</returns>
    public NodeDefinition WithExecutionStrategy(IExecutionStrategy? executionStrategy)
    {
        return this with { ExecutionConfig = ExecutionConfig with { ExecutionStrategy = executionStrategy } };
    }

    /// <summary>
    ///     Creates a new NodeDefinition with updated error handler type.
    /// </summary>
    /// <param name="errorHandlerType">The new error handler type.</param>
    /// <returns>A new NodeDefinition with the updated error handler type.</returns>
    public NodeDefinition WithErrorHandlerType(Type? errorHandlerType)
    {
        return this with { ExecutionConfig = ExecutionConfig with { ErrorHandlerType = errorHandlerType } };
    }
}
