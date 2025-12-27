using NPipeline.Graph;

namespace NPipeline.Visualization;

/// <summary>
///     Defines the contract for visualizing a pipeline graph.
/// </summary>
public interface IPipelineVisualizer
{
    /// <summary>
    ///     Visualizes the pipeline graph by serializing it and sending it to a sink.
    /// </summary>
    /// <param name="graph">The pipeline graph to visualize.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous visualization operation.</returns>
    Task VisualizeAsync(PipelineGraph graph, CancellationToken cancellationToken);
}
