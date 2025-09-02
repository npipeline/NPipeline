using NPipeline.Graph;

namespace NPipeline.Pipeline.Internals;

/// <summary>
///     Encapsulates connection-related state in the PipelineBuilder.
///     This internal class groups all edge/connection tracking.
/// </summary>
internal sealed class BuilderConnectionState
{
    /// <summary>
    ///     List of edges connecting nodes in the pipeline.
    /// </summary>
    public List<Edge> Edges { get; } = new();

    /// <summary>
    ///     Clears all connection state.
    /// </summary>
    public void Clear()
    {
        Edges.Clear();
    }
}
