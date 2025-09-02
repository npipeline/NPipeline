using NPipeline.Graph;

namespace NPipeline.Pipeline;

/// <summary>
///     Represents a data processing pipeline defined by a graph.
/// </summary>
public sealed class Pipeline
{
    internal Pipeline(PipelineGraph graph)
    {
        Graph = graph;
    }

    /// <summary>
    ///     Gets the graph definition of the pipeline.
    /// </summary>
    public PipelineGraph Graph { get; }

    internal IReadOnlyList<IAsyncDisposable> BuilderDisposables { get; set; } = Array.Empty<IAsyncDisposable>();

    internal void TransferBuilderDisposables(PipelineContext context)
    {
        if (BuilderDisposables.Count == 0)
            return;

        foreach (var d in BuilderDisposables)
        {
            context.RegisterForDisposal(d);
        }
    }
}
