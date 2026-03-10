using NPipeline.Connectors.RabbitMQ.Models;
using NPipeline.Connectors.RabbitMQ.Nodes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_RabbitMqConnector;

/// <summary>
///     Pipeline definition that consumes order events from RabbitMQ, enriches them,
///     and publishes the results to an output exchange.
/// </summary>
public sealed class RabbitMqConnectorPipeline : IPipelineDefinition
{
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource<RabbitMqSourceNode<OrderEvent>, RabbitMqMessage<OrderEvent>>("rabbitmq-source");
        var enricher = builder.AddTransform<OrderEnricher, RabbitMqMessage<OrderEvent>, EnrichedOrder>("order-enricher");
        var sink = builder.AddSink<RabbitMqSinkNode<EnrichedOrder>, EnrichedOrder>("rabbitmq-sink");

        builder.Connect(source, enricher);
        builder.Connect(enricher, sink);
    }
}

/// <summary>
///     Transform node that enriches raw order events with processing metadata.
/// </summary>
public sealed class OrderEnricher : TransformNode<RabbitMqMessage<OrderEvent>, EnrichedOrder>
{
    public override Task<EnrichedOrder> TransformAsync(
        RabbitMqMessage<OrderEvent> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var order = input.Body;

        var region = order.CustomerId.StartsWith("US", StringComparison.OrdinalIgnoreCase)
            ? "North America"
            : "International";

        var enriched = new EnrichedOrder(
            order.OrderId,
            order.CustomerId,
            order.Amount,
            order.CreatedAt,
            DateTime.UtcNow,
            region);

        Console.WriteLine(
            $"  Enriched order {enriched.OrderId}: ${enriched.Amount:F2} from {enriched.CustomerId} ({enriched.Region})");

        return Task.FromResult(enriched);
    }
}
