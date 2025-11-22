using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_ComplexDataTransformations.Nodes;

/// <summary>
///     Sink node that outputs lineage tracking information to console.
///     This node demonstrates how to process and display lineage data
///     for auditability and debugging purposes.
/// </summary>
public class LineageSink : SinkNode<LineageTrackedItem<object>>
{
    private int _processedCount;

    /// <summary>
    ///     Processes lineage-tracked items from the data pipe by outputting their lineage information to console.
    /// </summary>
    /// <param name="input">The data pipe containing lineage-tracked items to process.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing asynchronous operation.</returns>
    public override async Task ExecuteAsync(IDataPipe<LineageTrackedItem<object>> input, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("LineageSink started processing lineage-tracked items...");

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            _processedCount++;

            Console.WriteLine();
            Console.WriteLine($"=== Lineage Tracking Output #{_processedCount} ===");
            Console.WriteLine($"Data Type: {item.Data?.GetType().Name}");
            Console.WriteLine($"Data Value: {item.Data}");
            Console.WriteLine($"Lineage Steps: {item.Lineage.Count}");

            for (var i = 0; i < item.Lineage.Count; i++)
            {
                var lineage = item.Lineage[i];
                Console.WriteLine($"  Step {i + 1}:");
                Console.WriteLine($"    Lineage ID: {lineage.LineageId}");
                Console.WriteLine($"    Source: {lineage.SourceNode}");
                Console.WriteLine($"    Target: {lineage.TargetNode}");
                Console.WriteLine($"    Operation: {lineage.Operation}");
                Console.WriteLine($"    Timestamp: {lineage.TransformationTime:O}");

                if (lineage.Metadata.Count > 0)
                {
                    Console.WriteLine("    Metadata:");

                    foreach (var kvp in lineage.Metadata)
                    {
                        Console.WriteLine($"      {kvp.Key}: {kvp.Value}");
                    }
                }
            }

            Console.WriteLine($"=== End Lineage Output #{_processedCount} ===");
            Console.WriteLine();

            // Simulate some processing time
            await Task.Delay(10, cancellationToken);
        }

        Console.WriteLine($"LineageSink completed processing {_processedCount} lineage-tracked items.");
        Console.WriteLine("Lineage tracking provides complete audit trail of data transformations.");
    }
}
