using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_ParallelProcessing.Nodes;

/// <summary>
///     Source node that generates CPU-intensive work items for parallel processing demonstration.
///     This node creates work items with varying complexity to demonstrate different processing loads.
/// </summary>
public class CpuIntensiveDataSource : SourceNode<CpuIntensiveWorkItem>
{
    /// <summary>
    ///     Generates a collection of CPU-intensive work items with varying complexity.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the generated work items.</returns>
    public override IDataPipe<CpuIntensiveWorkItem> Execute(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Generating CPU-intensive work items...");

        var random = new Random();
        var workItems = new List<CpuIntensiveWorkItem>();

        // Generate work items with varying complexity to demonstrate parallel processing benefits
        for (var i = 1; i <= 20; i++)
        {
            var dataSize = random.Next(100, 1000);
            var complexity = random.Next(1, 10);

            workItems.Add(new CpuIntensiveWorkItem(
                $"work-item-{i:D3}",
                dataSize,
                complexity,
                DateTime.UtcNow
            ));
        }

        Console.WriteLine($"Generated {workItems.Count} CPU-intensive work items");
        Console.WriteLine($"Data size range: {workItems.Min(w => w.DataSize)} - {workItems.Max(w => w.DataSize)}");
        Console.WriteLine($"Complexity range: {workItems.Min(w => w.Complexity)} - {workItems.Max(w => w.Complexity)}");

        // Return a InMemoryDataPipe containing our work items
        return new InMemoryDataPipe<CpuIntensiveWorkItem>(workItems, "CpuIntensiveDataSource");
    }
}
