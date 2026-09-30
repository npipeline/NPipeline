using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.RabbitMQ;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.DeadLetter;
using NPipeline.Connectors.RabbitMQ.Models;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_RabbitMqConnector;

/// <summary>
///     Consumes order events from the <c>orders</c> queue, enriches them, and publishes them to
///     <c>enriched-orders-exchange</c>. Each order is acknowledged once the broker confirms its enriched copy.
/// </summary>
/// <param name="connection">The connection the nodes share; the caller owns it.</param>
public sealed class RabbitMqConnectorPipeline(IRabbitMqConnectionManager connection) : IPipelineDefinition
{
    public const string OrdersExchange = "orders-exchange";
    public const string OrdersQueue = "orders";
    public const string EnrichedExchange = "enriched-orders-exchange";
    public const string EnrichedRoutingKey = "order.enriched";
    public const string EnrichedQueue = "enriched-orders";
    public const string DeadLetterQueue = "orders-dead-letter";

    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            RabbitMqConnector.Source<OrderEvent>(connection, OrdersQueue, o => o with
            {
                PrefetchCount = 50,

                // Declare the quorum queue and bind it to the topic exchange for created and updated orders.
                Topology = new RabbitMqTopologyOptions
                {
                    QueueType = QueueType.Quorum,
                    ExchangeType = "topic",
                    Bindings =
                    [
                        new BindingOptions(OrdersExchange, "order.created"),
                        new BindingOptions(OrdersExchange, "order.updated"),
                    ],
                },

                // A body that isn't a valid OrderEvent goes to the dead-letter sink and is then rejected.
                RowErrorHandler = _ => RowErrorAction.DeadLetter,
            }),
            "rabbitmq-source");

        var enricher = builder.AddTransform<OrderEnricher, RabbitMqMessage<OrderEvent>, IAcknowledgableMessage<EnrichedOrder>>("order-enricher");

        // Acknowledging() acks each order once the broker confirms its enriched copy (publisher confirms are on by default).
        var sink = builder.AddSink(
            RabbitMqConnector.Sink<EnrichedOrder>(connection, EnrichedExchange, EnrichedRoutingKey).Acknowledging(),
            "rabbitmq-sink");

        builder.Connect(source, enricher);
        builder.Connect(enricher, sink);

        // Publishes dead letters with their original body to the dead-letter queue through the default exchange.
        builder.AddDeadLetterSink(new RabbitMqDeadLetterSink(connection, exchange: "", routingKey: DeadLetterQueue));
    }
}

/// <summary>Adds a region and processing time to each order, keeping the RabbitMQ message so it can be acknowledged downstream.</summary>
public sealed class OrderEnricher : TransformNode<RabbitMqMessage<OrderEvent>, IAcknowledgableMessage<EnrichedOrder>>
{
    public override ValueTask<IAcknowledgableMessage<EnrichedOrder>> TransformAsync(RabbitMqMessage<OrderEvent> input, PipelineContext context,
        CancellationToken cancellationToken)
    {
        var order = input.Body;

        var region = order.CustomerId.StartsWith("US", StringComparison.OrdinalIgnoreCase)
            ? "North America"
            : "International";

        var enriched = new EnrichedOrder(order.OrderId, order.CustomerId, order.Amount, order.CreatedAt, DateTime.UtcNow, region);

        Console.WriteLine($"  Enriched order {enriched.OrderId} ({input.RoutingKey}): ${enriched.Amount:F2} from {enriched.CustomerId} ({enriched.Region})");

        return ValueTask.FromResult(input.WithBody(enriched));
    }
}
