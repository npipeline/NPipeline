using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_ParquetConnector.Nodes;

/// <summary>Generates a set of synthetic sales records for the pipeline.</summary>
public sealed class SalesDataSourceNode : SourceNode<SalesRecord>
{
    private readonly int _count;

    public SalesDataSourceNode(int count = 500) => _count = count;

    public override IDataPipe<SalesRecord> Initialize(PipelineContext context, CancellationToken cancellationToken)
        => new InMemoryDataPipe<SalesRecord>(GenerateRecords().ToList(), "sales-data");

    private IEnumerable<SalesRecord> GenerateRecords()
    {
        var products = new[] { "Laptop", "Mouse", "Keyboard", "Monitor", "Headphones" };
        var regions = new[] { "EU", "US", "APAC", "LATAM" };
        var rng = new Random(42);
        var baseDate = new DateTime(2025, 1, 1);

        for (var i = 1; i <= _count; i++)
        {
            yield return new SalesRecord
            {
                Id = i,
                Product = products[rng.Next(products.Length)],
                Amount = Math.Round((decimal)(rng.NextDouble() * 950 + 50), 2),
                TransactionDate = baseDate.AddDays(rng.Next(90)),
                Region = regions[rng.Next(regions.Length)],
            };
        }
    }
}
