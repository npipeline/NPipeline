namespace NPipeline.Lineage;

/// <summary>
///     Maps a non 1:1 transformation's inputs to its outputs for richer lineage diagnostics.
///     Implementations may perform semantic correlation (keys, aggregation grouping, splitting) and return
///     mapping records indicating which input lineage packets contributed to each output.
/// </summary>
public interface ILineageMapper
{
    LineageMappingResult MapInputToOutputs(IReadOnlyList<object> inputPackets, IReadOnlyList<object> outputs, LineageMappingContext context);
}

public sealed record LineageMappingContext(string NodeId);

public sealed record LineageMappingRecord(int OutputIndex, IReadOnlyList<int> InputIndices);

public sealed record LineageMappingResult(IReadOnlyList<LineageMappingRecord> Records);

[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class LineageMapperAttribute(Type mapperType) : Attribute
{
    public Type MapperType { get; } = mapperType;
}
