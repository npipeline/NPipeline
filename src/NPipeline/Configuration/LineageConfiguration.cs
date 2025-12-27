using NPipeline.Lineage;

namespace NPipeline.Configuration;

/// <summary>
///     Configuration for lineage settings.
/// </summary>
public sealed record LineageConfiguration
{
    /// <summary>
    ///     A flag indicating whether item-level lineage is enabled.
    /// </summary>
    public bool ItemLevelLineageEnabled { get; init; }

    /// <summary>
    ///     The optional sink for lineage information.
    /// </summary>
    public ILineageSink? LineageSink { get; init; }

    /// <summary>
    ///     The type of the lineage sink.
    /// </summary>
    public Type? LineageSinkType { get; init; }

    /// <summary>
    ///     The optional pipeline lineage sink.
    /// </summary>
    public IPipelineLineageSink? PipelineLineageSink { get; init; }

    /// <summary>
    ///     The type of the pipeline lineage sink.
    /// </summary>
    public Type? PipelineLineageSinkType { get; init; }

    /// <summary>
    ///     The lineage options.
    /// </summary>
    public LineageOptions? LineageOptions { get; init; }

    /// <summary>
    ///     Creates a new LineageConfiguration with default values.
    /// </summary>
    public static LineageConfiguration Default => new();
}
