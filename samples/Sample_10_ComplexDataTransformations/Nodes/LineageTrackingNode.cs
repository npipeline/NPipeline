using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_10_ComplexDataTransformations.Nodes;

/// <summary>
///     Transform node that tracks data lineage throughout the pipeline.
///     This node demonstrates data lineage tracking capabilities in NPipeline for auditability
///     and debugging purposes.
/// </summary>
public class LineageTrackingNode : TransformNode<object, LineageTrackedItem<object>>
{
    private int _processedCount;

    /// <summary>
    ///     Transforms an input item by wrapping it with lineage tracking information.
    /// </summary>
    /// <param name="item">The input item to track.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A LineageTrackedItem containing the original item and lineage information.</returns>
    public override Task<LineageTrackedItem<object>> ExecuteAsync(object item, PipelineContext context, CancellationToken cancellationToken)
    {
        _processedCount++;

        // Create lineage tracked item using the factory
        var lineageItem = LineageTrackedItemFactory.Create(item, "LineageTrackingNode");

        // Add transformation metadata
        var enrichedLineageItem = lineageItem.AddTransformation(
            "PreviousNode",
            "LineageTrackingNode",
            $"TrackTransformation_{_processedCount}"
        );

        Console.WriteLine($"Tracked lineage for item #{_processedCount}: {item?.GetType().Name}");
        Console.WriteLine($"  Lineage ID: {enrichedLineageItem.Lineage.Last().LineageId}");
        Console.WriteLine($"  Operation: {enrichedLineageItem.Lineage.Last().Operation}");
        Console.WriteLine($"  Timestamp: {enrichedLineageItem.Lineage.Last().TransformationTime:O}");

        return Task.FromResult(enrichedLineageItem);
    }
}
