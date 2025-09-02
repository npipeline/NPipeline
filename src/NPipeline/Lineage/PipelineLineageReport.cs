using System.Text.Json.Serialization;

namespace NPipeline.Lineage;

/// <summary>
///     Represents a high-level, serializable report of a pipeline's structure.
/// </summary>
public sealed record PipelineLineageReport(
    [property: JsonPropertyName("pipeline")]
    string Pipeline,
    [property: JsonPropertyName("runId")] Guid RunId,
    [property: JsonPropertyName("nodes")] IReadOnlyList<NodeLineageInfo> Nodes,
    [property: JsonPropertyName("edges")] IReadOnlyList<EdgeLineageInfo> Edges
);
