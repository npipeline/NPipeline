using NPipeline.Configuration;

namespace NPipeline.Pipeline.Internals;

/// <summary>
///     Internal helper for constructing LineageConfiguration from builder state.
/// </summary>
/// <remarks>
///     This builder consolidates lineage configuration from both the BuilderConfig (for lineage options)
///     and BuilderConfigurationState (for sinks), eliminating duplication between builder fields
///     and the configuration object itself.
/// </remarks>
internal static class LineageConfigurationBuilder
{
    /// <summary>
    ///     Builds a LineageConfiguration from the current builder state.
    /// </summary>
    /// <param name="configState">The builder configuration state containing lineage sink properties.</param>
    /// <param name="config">The builder configuration containing lineage enablement and options.</param>
    /// <returns>A new LineageConfiguration with all properties set.</returns>
    public static LineageConfiguration Build(
        BuilderConfigurationState configState,
        PipelineBuilder.BuilderConfig config)
    {
        ArgumentNullException.ThrowIfNull(configState);
        ArgumentNullException.ThrowIfNull(config);

        return new LineageConfiguration
        {
            ItemLevelLineageEnabled = config.ItemLevelLineageEnabled,
            LineageSink = configState.LineageSink,
            LineageSinkType = configState.LineageSinkType,
            PipelineLineageSink = configState.PipelineLineageSink,
            PipelineLineageSinkType = configState.PipelineLineageSinkType,
            LineageOptions = config.LineageOptions,
        };
    }
}
