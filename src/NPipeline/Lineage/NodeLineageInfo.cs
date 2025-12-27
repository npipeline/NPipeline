using System.Text.Json.Serialization;

namespace NPipeline.Lineage;

/// <summary>
///     Represents a node in the pipeline for the lineage report.
/// </summary>
public sealed record NodeLineageInfo(
    [property: JsonPropertyName("id")] string Id,
    [property: JsonPropertyName("type")] string Type,
    [property: JsonPropertyName("inputType")]
    string? InputType,
    [property: JsonPropertyName("outputType")]
    string? OutputType
);
