namespace NPipeline.Lineage;

/// <summary>
///     Maps a non 1:1 transformation's inputs to its outputs for richer lineage diagnostics.
///     Implementations may perform semantic correlation (keys, aggregation grouping, splitting) and return
///     mapping records indicating which input lineage packets contributed to each output.
/// </summary>
public interface ILineageMapper
{
    /// <summary>
    ///     Maps input items to their corresponding output items for lineage tracking.
    /// </summary>
    /// <param name="inputPackets">The input items that were processed.</param>
    /// <param name="outputs">The output items that were produced.</param>
    /// <param name="context">Context information about the node performing the mapping.</param>
    /// <returns>A collection of mapping records that link outputs to their inputs.</returns>
    LineageMappingResult MapInputToOutputs(IReadOnlyList<object> inputPackets, IReadOnlyList<object> outputs, LineageMappingContext context);
}

/// <summary>
///     Provides context information for lineage mapping operations.
/// </summary>
/// <param name="NodeId">The identifier of the node performing the lineage mapping.</param>
public sealed record LineageMappingContext(string NodeId);

/// <summary>
///     Represents a mapping between a specific output item and the input items that contributed to it.
/// </summary>
/// <param name="OutputIndex">The index of the output item in the outputs collection.</param>
/// <param name="InputIndices">The indices of input items that contributed to this output.</param>
public sealed record LineageMappingRecord(int OutputIndex, IReadOnlyList<int> InputIndices);

/// <summary>
///     Contains the complete mapping results for a lineage mapping operation.
/// </summary>
/// <param name="Records">The collection of individual mapping records.</param>
public sealed record LineageMappingResult(IReadOnlyList<LineageMappingRecord> Records);

/// <summary>
///     Attribute used to associate a lineage mapper type with a transform node.
/// </summary>
/// <param name="mapperType">The type of the lineage mapper implementation.</param>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class LineageMapperAttribute(Type mapperType) : Attribute
{
    /// <summary>
    ///     Gets the type of the lineage mapper implementation.
    /// </summary>
    public Type MapperType { get; } = mapperType;
}
