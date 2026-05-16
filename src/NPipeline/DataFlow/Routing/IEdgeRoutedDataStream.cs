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
}
