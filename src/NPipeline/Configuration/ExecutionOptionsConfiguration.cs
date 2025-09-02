using System.Collections.Immutable;
using NPipeline.Visualization;

namespace NPipeline.Configuration;

/// <summary>
///     Configuration for execution options.
/// </summary>
public sealed record ExecutionOptionsConfiguration
{
    /// <summary>
    ///     The node execution annotations.
    /// </summary>
    public ImmutableDictionary<string, object>? NodeExecutionAnnotations { get; init; }

    /// <summary>
    ///     The optional visualizer.
    /// </summary>
    public IPipelineVisualizer? Visualizer { get; init; }

    /// <summary>
    ///     Creates a new ExecutionOptionsConfiguration with default values.
    /// </summary>
    public static ExecutionOptionsConfiguration Default => new();
}
