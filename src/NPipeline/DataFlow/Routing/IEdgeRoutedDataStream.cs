using NPipeline.Graph;

namespace NPipeline.DataFlow.Routing;

/// <summary>
///     Exposes edge-specific views for routed multicast streams.
/// </summary>
public interface IEdgeRoutedDataStream
{
    /// <summary>
    ///     Gets the edge-specific view of this stream for the provided edge.
    /// </summary>
    IDataStream GetEdgeView(Edge edge);

    /// <summary>
    ///     Releases the provided edge once the node consuming it will not read it again, including through node retry.
    ///     The stream stops feeding the edge and discards whatever it still buffers, so an edge whose consumer stopped
    ///     early, or never read at all, cannot stall its sibling edges.
    /// </summary>
    void ReleaseEdge(Edge edge);
}
