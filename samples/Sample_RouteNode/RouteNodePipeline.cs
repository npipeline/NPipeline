using NPipeline.DataFlow.Routing;
using NPipeline.Pipeline;
using Sample_RouteNode.Models;
using Sample_RouteNode.Nodes;

namespace Sample_RouteNode;

/// <summary>
///     Pipeline demonstrating conditional routing with a Route node.
/// </summary>
public sealed class RouteNodePipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<OrderSource, OrderEvent>("orders");

        var route = builder.AddRoute<OrderEvent>(options =>
        {
            options.WithMatchMode(RouteMatchMode.AllMatches);
        }, "route-orders");

        var highValueSink = builder.AddSink<HighValueOrderSink, OrderEvent>("high-value-sink");
        var internationalSink = builder.AddSink<InternationalOrderSink, OrderEvent>("international-sink");
        var standardSink = builder.AddSink<StandardOrderSink, OrderEvent>("standard-sink");

        builder.Connect(source, route);

        builder.ConnectWhen(route, highValueSink, order => order.Amount >= 1000m, "high-value");
        builder.ConnectWhen(route, internationalSink, order => !string.Equals(order.Country, "US", StringComparison.OrdinalIgnoreCase), "international");
        builder.ConnectOtherwise(route, standardSink, "standard");
    }

    public static string GetDescription()
    {
        return @"RouteNode Sample:

This sample demonstrates conditional routing with named outputs:
- Source emits six order events
- Route node evaluates two conditions per order
- Match mode is set to AllMatches, so an order can reach multiple outputs
- Otherwise output receives orders that match no explicit condition

Route conditions:
1. high-value: Amount >= 1000
2. international: Country != US
3. standard (otherwise): all remaining orders

Expected behavior:
- ORD-1003 and ORD-1004 go to HIGH-VALUE
- ORD-1002, ORD-1004, and ORD-1006 go to INTERNATIONAL
- ORD-1001 and ORD-1005 go to STANDARD";
    }
}
