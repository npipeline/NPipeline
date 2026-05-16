using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_RouteNode.Models;

namespace Sample_RouteNode.Nodes;

/// <summary>
///     Emits a small set of sample orders for routing.
/// </summary>
public sealed class OrderSource : SourceNode<OrderEvent>
{
    public override IDataStream<OrderEvent> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        var orders = new List<OrderEvent>
        {
            new("ORD-1001", "CUST-01", "US", 120m),
            new("ORD-1002", "CUST-02", "CA", 250m),
            new("ORD-1003", "CUST-03", "US", 1250m),
            new("ORD-1004", "CUST-04", "DE", 1800m),
            new("ORD-1005", "CUST-05", "US", 75m),
            new("ORD-1006", "CUST-06", "JP", 600m),
        };

        Console.WriteLine($"OrderSource emitted {orders.Count} orders.");
        return new InMemoryDataStream<OrderEvent>(orders, "orders");
    }
}
