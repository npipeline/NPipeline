using System.Collections.Immutable;
using NPipeline.Configuration;

namespace NPipeline.Pipeline.Internals;

/// <summary>
///     Internal helper for constructing ExecutionOptionsConfiguration from builder state.
/// </summary>
internal static class ExecutionOptionsConfigurationBuilder
{
    /// <summary>
    ///     Builds an ExecutionOptionsConfiguration from the current builder state.
    /// </summary>
    /// <param name="nodeState">The builder node state containing execution annotations.</param>
    /// <param name="configState">The builder configuration state containing visualizer instance.</param>
    /// <returns>A new ExecutionOptionsConfiguration with all properties set.</returns>
    public static ExecutionOptionsConfiguration Build(
        BuilderNodeState nodeState,
        BuilderConfigurationState configState)
    {
        ArgumentNullException.ThrowIfNull(nodeState);
        ArgumentNullException.ThrowIfNull(configState);

        return new ExecutionOptionsConfiguration
        {
            NodeExecutionAnnotations = nodeState.ExecutionAnnotations.Count > 0
                ? nodeState.ExecutionAnnotations.ToImmutableDictionary()
                : null,
            Visualizer = configState.Visualizer,
        };
    }
}
