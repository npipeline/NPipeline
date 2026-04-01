using NPipeline.Attributes.Lineage;
using NPipeline.Execution;
using NPipeline.Graph.PipelineDelegates;

namespace NPipeline.Graph;

/// <summary>
///     Specifies the type of a node in the pipeline.
/// </summary>
public enum NodeKind
{
    /// <summary>A source node that produces data.</summary>
    Source,

    /// <summary>A transform node that processes data item-by-item.</summary>
    Transform,

    /// <summary>A stream transform node that operates on the full data stream.</summary>
    StreamTransform,

    /// <summary>A tap node that sends copies of data to a side-channel sink without affecting the main flow.</summary>
    Tap,

    /// <summary>A branch node that fans out data to multiple downstream pathways.</summary>
    Branch,

    /// <summary>A lookup node that enriches data using an in-memory dictionary.</summary>
    Lookup,

    /// <summary>A batching or unbatching node that groups or ungroups data items.</summary>
    Batch,

    /// <summary>A sink node that consumes data.</summary>
    Sink,

    /// <summary>A join node that combines multiple streams.</summary>
    Join,

    /// <summary>An aggregate node that combines multiple data items.</summary>
    Aggregate,
}

/// <summary>
///     Legacy nested identity configuration kept for compatibility.
/// </summary>
public sealed record NodeIdentity(
    string Id,
    string Name);

/// <summary>
///     Legacy nested type-system configuration kept for compatibility.
/// </summary>
public sealed record NodeTypeSystem(
    Type NodeType,
    NodeKind Kind,
    Type? InputType = null,
    Type? OutputType = null);

/// <summary>
///     Legacy nested execution configuration kept for compatibility.
/// </summary>
public sealed record NodeExecutionConfig(
    IExecutionStrategy? ExecutionStrategy = null,
    Type? ErrorHandlerType = null,
    TransformCardinality? DeclaredCardinality = null);

/// <summary>
///     Legacy nested merge configuration kept for compatibility.
/// </summary>
public sealed record NodeMergeConfig(
    MergeType? MergeStrategy = null,
    bool HasCustomMerge = false,
    bool IsJoin = false,
    CustomMergeDelegate? CustomMerge = null);

/// <summary>
///     Legacy nested lineage configuration kept for compatibility.
/// </summary>
public sealed record NodeLineageConfig(
    LineageAdapterDelegate? LineageAdapter = null,
    Type? LineageMapperType = null,
    SinkLineageUnwrapDelegate? SinkLineageUnwrap = null);

/// <summary>
///     Represents the definition of a node within the pipeline graph.
/// </summary>
/// <remarks>
///     Pre-compiled join key selectors are stored in JoinKeySelectorRegistry and accessed at runtime,
///     not duplicated in this record. This keeps the record lightweight and makes the registry the
///     single source of truth for cached selectors.
/// </remarks>
public sealed record NodeDefinition(
    string Id,
    string Name,
    Type NodeType,
    NodeKind Kind,
    Type? InputType = null,
    Type? OutputType = null,
    IExecutionStrategy? ExecutionStrategy = null,
    Type? ErrorHandlerType = null,
    TransformCardinality? DeclaredCardinality = null,
    MergeType? MergeStrategy = null,
    bool HasCustomMerge = false,
    bool IsJoin = false,
    CustomMergeDelegate? CustomMerge = null,
    LineageAdapterDelegate? LineageAdapter = null,
    Type? LineageMapperType = null,
    SinkLineageUnwrapDelegate? SinkLineageUnwrap = null)
{
    /// <summary>
    ///     Compatibility constructor for older nested record callers.
    /// </summary>
    public NodeDefinition(
        NodeIdentity identity,
        NodeTypeSystem typeSystem,
        NodeExecutionConfig executionConfig,
        NodeMergeConfig mergeConfig,
        NodeLineageConfig lineageConfig)
        : this(
            identity.Id,
            identity.Name,
            typeSystem.NodeType,
            typeSystem.Kind,
            typeSystem.InputType,
            typeSystem.OutputType,
            executionConfig.ExecutionStrategy,
            executionConfig.ErrorHandlerType,
            executionConfig.DeclaredCardinality,
            mergeConfig.MergeStrategy,
            mergeConfig.HasCustomMerge,
            mergeConfig.IsJoin,
            mergeConfig.CustomMerge,
            lineageConfig.LineageAdapter,
            lineageConfig.LineageMapperType,
            lineageConfig.SinkLineageUnwrap)
    {
    }

    /// <summary>
    ///     Legacy nested identity view.
    /// </summary>
    public NodeIdentity Identity => new(Id, Name);

    /// <summary>
    ///     Legacy nested type-system view.
    /// </summary>
    public NodeTypeSystem TypeSystem => new(NodeType, Kind, InputType, OutputType);

    /// <summary>
    ///     Legacy nested execution view.
    /// </summary>
    public NodeExecutionConfig ExecutionConfig => new(ExecutionStrategy, ErrorHandlerType, DeclaredCardinality);

    /// <summary>
    ///     Legacy nested merge view.
    /// </summary>
    public NodeMergeConfig MergeConfig => new(MergeStrategy, HasCustomMerge, IsJoin, CustomMerge);

    /// <summary>
    ///     Legacy nested lineage view.
    /// </summary>
    public NodeLineageConfig LineageConfig => new(LineageAdapter, LineageMapperType, SinkLineageUnwrap);

    /// <summary>
    ///     Creates a new NodeDefinition with updated execution strategy.
    /// </summary>
    /// <param name="executionStrategy">The new execution strategy.</param>
    /// <returns>A new NodeDefinition with the updated execution strategy.</returns>
    public NodeDefinition WithExecutionStrategy(IExecutionStrategy? executionStrategy)
    {
        return this with { ExecutionStrategy = executionStrategy };
    }

    /// <summary>
    ///     Creates a new NodeDefinition with updated error handler type.
    /// </summary>
    /// <param name="errorHandlerType">The new error handler type.</param>
    /// <returns>A new NodeDefinition with the updated error handler type.</returns>
    public NodeDefinition WithErrorHandlerType(Type? errorHandlerType)
    {
        return this with { ErrorHandlerType = errorHandlerType };
    }
}
