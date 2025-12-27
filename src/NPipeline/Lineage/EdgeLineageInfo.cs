using System.Text.Json.Serialization;

namespace NPipeline.Lineage;

/// <summary>
///     Represents an edge in the pipeline for the lineage report.
/// </summary>
public sealed record EdgeLineageInfo(
    [property: JsonPropertyName("from")] string From,
    [property: JsonPropertyName("to")] string To
);
