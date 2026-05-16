using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_RouteNode.Models;

namespace Sample_RouteNode.Nodes;

/// <summary>
///     Consumes orders that did not match other route rules.
/// </summary>
public sealed class StandardOrderSink : SinkNode<OrderEvent>
{
    public override async Task ConsumeAsync(IDataStream<OrderEvent> input, PipelineContext context, CancellationToken cancellationToken)
    {
        var count = 0;

        await foreach (var order in input.WithCancellation(cancellationToken))
        {
            count++;
            Console.WriteLine($"[STANDARD] {order.OrderId} | {order.Country} | ${order.Amount:N2}");
        }

        Console.WriteLine($"StandardOrderSink processed {count} order(s).");
    }
}
