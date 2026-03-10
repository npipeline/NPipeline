using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_ObservabilityExtension.Nodes;

/// <summary>
///     Source node that generates a sequence of numeric values.
///     Demonstrates how observability metrics are recorded at the source stage.
/// </summary>
public class NumberGenerator : SourceNode<int>
{
    /// <summary>
    ///     Generates numbers from 1 to 100.
    /// </summary>
    public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("[NumberGenerator] Initializing number generation (1-100)");

        var numbers = Enumerable.Range(1, 100).ToList();
        return new InMemoryDataStream<int>(numbers, "number-generator");
    }
}
