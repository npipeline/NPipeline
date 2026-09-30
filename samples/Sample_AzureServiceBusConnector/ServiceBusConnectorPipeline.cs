using Azure.Messaging.ServiceBus;
using NPipeline.Connectors.Azure.ServiceBus;
using NPipeline.Connectors.Azure.ServiceBus.Models;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Messaging;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_AzureServiceBusConnector;

/// <summary>
///     Receives orders from the <c>input-orders</c> queue, validates them, and sends the results to the
///     <c>processed-orders</c> queue. Each input message is completed once its result has been sent.
/// </summary>
/// <param name="client">The client the source and sink share; the caller owns it.</param>
public sealed class ServiceBusConnectorPipeline(ServiceBusClient client) : IPipelineDefinition
{
    public const string InputQueue = "input-orders";
    public const string OutputQueue = "processed-orders";

    /// <inheritdoc />
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // For a topic, use ServiceBusConnector.SubscriptionSource<Order>(client, topic, subscription); for a
        // session-enabled queue or subscription, ServiceBusConnector.SessionSource<Order>(client, entity).
        var source = builder.AddSource(
            ServiceBusConnector.Source<Order>(client, InputQueue, o => o with
            {
                PrefetchCount = 20,

                // Keep renewing the lock of a message that is still waiting to be settled, for up to 10 minutes.
                MaxLockRenewal = TimeSpan.FromMinutes(10),

                // A body that isn't a valid Order moves to the queue's dead-letter sub-queue.
                RowErrorHandler = _ => RowErrorAction.Skip,
            }),
            "servicebus-order-source");

        var process = builder.AddTransform<OrderProcessor, ServiceBusMessage<Order>, IAcknowledgableMessage<ProcessedOrder>>("order-processor");

        // Acknowledging() completes each input message once its result is sent. The result keeps the input's message
        // id, correlation id and application properties.
        var sink = builder.AddSink(
            ServiceBusConnector.Sink<ProcessedOrder>(client, OutputQueue, o => o with { BatchSize = 50 }).Acknowledging(),
            "servicebus-processed-order-sink");

        builder.Connect(source, process);
        builder.Connect(process, sink);
    }

    /// <summary>Describes what the pipeline does.</summary>
    public static string GetDescription() =>
        $"""
         ServiceBusConnector.Source<Order>              {InputQueue}
           -> OrderProcessor                            (message.WithBody(processed))
             -> ServiceBusConnector.Sink<ProcessedOrder>.Acknowledging()   {OutputQueue}

         Undeserializable messages -> {InputQueue}'s dead-letter sub-queue
         """;
}

/// <summary>Validates each order, keeping the Service Bus message so it can be completed downstream.</summary>
public sealed class OrderProcessor : TransformNode<ServiceBusMessage<Order>, IAcknowledgableMessage<ProcessedOrder>>
{
    /// <inheritdoc />
    public override async ValueTask<IAcknowledgableMessage<ProcessedOrder>> TransformAsync(ServiceBusMessage<Order> input, PipelineContext context,
        CancellationToken cancellationToken)
    {
        var order = input.Body;

        Console.WriteLine(
            $"[OrderProcessor] Order #{order.OrderId} for customer {order.CustomerId}, amount ${order.TotalAmount:F2}, delivery {input.DeliveryCount}");

        // Simulate processing work.
        await Task.Delay(50, cancellationToken);

        var valid = order.TotalAmount > 0;
        Console.WriteLine(valid ? $"  Order #{order.OrderId} completed" : $"  Order #{order.OrderId} rejected: invalid amount ({order.TotalAmount})");

        var processed = new ProcessedOrder(
            order.OrderId,
            order.CustomerId,
            order.TotalAmount,
            valid ? "Completed" : "Rejected",
            DateTime.UtcNow,
            valid ? "Order processed successfully" : "Invalid order amount - must be greater than zero.");

        return input.WithBody(processed);
    }
}
