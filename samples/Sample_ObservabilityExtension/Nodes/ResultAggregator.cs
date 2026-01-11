using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace Sample_ObservabilityExtension.Nodes;

/// <summary>
///     Sink node that aggregates and displays results.
///     Demonstrates how sinks record metrics for final output stages.
/// </summary>
public class ResultAggregator : SinkNode<int>
{
    private int _itemsReceived;
    private int _max = int.MinValue;
    private int _min = int.MaxValue;
    private long _sum;

    /// <summary>
    ///     Processes all result items from the input pipe, aggregating statistics.
    /// </summary>
    public override async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            _itemsReceived++;
            _sum += item;

            if (item < _min)
                _min = item;

            if (item > _max)
                _max = item;

            // Only log first and last few items to avoid clutter
            if (_itemsReceived <= 3 || _itemsReceived > 48)
                Console.WriteLine($"[ResultAggregator] Item {_itemsReceived}: {item}");
            else if (_itemsReceived == 4)
                Console.WriteLine("[ResultAggregator] ... (processing remaining items) ...");
        }

        // Display final statistics
        Console.WriteLine();
        Console.WriteLine("[ResultAggregator] === AGGREGATION RESULTS ===");
        Console.WriteLine($"  Total items received: {_itemsReceived}");
        Console.WriteLine($"  Sum: {_sum}");
        Console.WriteLine($"  Average: {(_itemsReceived > 0 ? _sum / (double)_itemsReceived : 0):F2}");
        Console.WriteLine($"  Min: {_min}");
        Console.WriteLine($"  Max: {_max}");

        // Record item metrics through the observability collector
        var collector = context.ExecutionObserver as IObservabilityCollector;

        if (collector != null)
        {
            // For sink, items processed equals items received, nothing is emitted
            collector.RecordItemMetrics(context.CurrentNodeId, _itemsReceived, 0);

            // Record performance metrics
            // Aggregation is fast: assume ~0.05ms per item
            collector.RecordPerformanceMetrics(context.CurrentNodeId, 20000.0, 0.05);
        }
    }
}
