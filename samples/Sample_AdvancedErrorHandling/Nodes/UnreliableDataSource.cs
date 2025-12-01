using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_AdvancedErrorHandling.Nodes;

/// <summary>
///     Source node that generates data with intermittent failures to simulate unreliable data sources.
///     This node demonstrates how to handle sources that may fail intermittently.
/// </summary>
public class UnreliableDataSource : SourceNode<SourceData>
{
    private readonly Random _random = new();

    /// <summary>
    ///     Generates a collection of SourceData records with simulated intermittent failures.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing the generated data.</returns>
    public override IDataPipe<SourceData> ExecuteAsync(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Generating data with intermittent failures...");

        var dataItems = new List<SourceData>();
        var failureRate = 0.3; // 30% failure rate for demonstration

        // Generate 20 data items with potential failures
        for (var i = 1; i <= 20; i++)
        {
            // Simulate intermittent failures - but don't fail immediately, just log them
            if (_random.NextDouble() < failureRate)
            {
                Console.WriteLine($"[FAILURE] Simulated failure for item {i} (will be handled by downstream nodes)");

                // Don't throw here - let downstream nodes handle the failures
                // This is more realistic for a data source
            }

            var dataItem = TestDataGenerator.CreateSourceData(
                $"item-{i:D3}",
                $"Data item {i} with potential processing challenges",
                DateTime.UtcNow.AddSeconds(-_random.Next(0, 300))
            );

            dataItems.Add(dataItem);
        }

        Console.WriteLine($"Successfully generated {dataItems.Count} data items (with some failures simulated for downstream processing)");

        // Return a InMemoryDataPipe containing our data items
        return new InMemoryDataPipe<SourceData>(dataItems, "UnreliableDataSource");
    }
}
