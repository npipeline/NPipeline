using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Models;
using NPipeline.Connectors.Azure.ServiceBus.Nodes;
using NPipeline.Connectors.Configuration;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_AzureServiceBusConnector;

/// <summary>
///     Pipeline demonstrating Azure Service Bus connector usage for order processing.
/// </summary>
/// <remarks>
///     <para>
///         This sample demonstrates:
///         1. Consuming messages from an Azure Service Bus queue using <see cref="ServiceBusQueueSourceNode{T}" />
///         2. Processing orders through a transform node
///         3. Publishing processed orders to an output queue using <see cref="ServiceBusQueueSinkNode{T}" />
///         4. Automatic message acknowledgment (Complete) on successful sink output
///     </para>
///     <para>
///         Before running, update <see cref="InputQueueConnectionString" /> and <see cref="OutputQueueConnectionString" />
///         with real Azure Service Bus connection strings and queue names, or set the <c>SERVICEBUS_CONNECTION_STRING</c>
///         environment variable.
///     </para>
/// </remarks>
public sealed class ServiceBusConnectorPipeline : IPipelineDefinition
{
    // ── Configuration constants ──────────────────────────────────────────────────
    private const string InputQueueConnectionString =
        "Endpoint=sb://YOUR-NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR_KEY=";

    private const string OutputQueueConnectionString =
        "Endpoint=sb://YOUR-NAMESPACE.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=YOUR_KEY=";

    private const string InputQueueName = "input-orders";
    private const string OutputQueueName = "processed-orders";

    /// <inheritdoc />
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Source: consume Order messages from the input queue.
        // ServiceBusQueueSourceNode outputs ServiceBusMessage<Order>.
        var sourceNode = builder.AddSource<ServiceBusQueueSourceNode<Order>, ServiceBusMessage<Order>>(
            "ServiceBusOrderSource");

        // Transform: process each order and produce a ProcessedOrder, preserving
        // the original message envelope so the sink can acknowledge it.
        var transformNode = builder
            .AddTransform<OrderProcessor, ServiceBusMessage<Order>, IAcknowledgableMessage<ProcessedOrder>>(
                "OrderProcessor");

        // Sink: publish processed orders to the output queue.
        // Using IAcknowledgableMessage<ProcessedOrder> so the connector can
        // auto-complete the source message after successful send.
        var sinkNode = builder
            .AddSink<ServiceBusQueueSinkNode<IAcknowledgableMessage<ProcessedOrder>>,
                IAcknowledgableMessage<ProcessedOrder>>(
                "ServiceBusProcessedOrderSink");

        // Connect nodes into a linear pipeline
        _ = builder.Connect(sourceNode, transformNode);
        _ = builder.Connect(transformNode, sinkNode);
    }

    /// <summary>
    ///     Creates the source <see cref="ServiceBusConfiguration" /> for consuming from the input queue.
    /// </summary>
    public static ServiceBusConfiguration CreateSourceConfiguration()
    {
        var connectionString = Environment.GetEnvironmentVariable("SERVICEBUS_CONNECTION_STRING")
                               ?? InputQueueConnectionString;

        return new ServiceBusConfiguration
        {
            ConnectionString = connectionString,
            QueueName = InputQueueName,
            MaxConcurrentCalls = 5,
            PrefetchCount = 20,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(10),
            AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess,
            ContinueOnDeserializationError = false,
            DeadLetterOnDeserializationError = true,
        };
    }

    /// <summary>
    ///     Creates the sink <see cref="ServiceBusConfiguration" /> for publishing to the output queue.
    /// </summary>
    public static ServiceBusConfiguration CreateSinkConfiguration()
    {
        var connectionString = Environment.GetEnvironmentVariable("SERVICEBUS_CONNECTION_STRING")
                               ?? OutputQueueConnectionString;

        return new ServiceBusConfiguration
        {
            ConnectionString = connectionString,
            QueueName = OutputQueueName,
            EnableBatchSending = true,
            BatchSize = 50,
        };
    }

    /// <summary>Returns a human-readable description of the pipeline structure.</summary>
    public static string GetDescription()
    {
        return """
               Pipeline Structure:
               ┌──────────────────────────────────────────────────────────────────────────────┐
               │  Azure Service Bus Order Processing Pipeline                                  │
               └──────────────────────────────────────────────────────────────────────────────┘

               Flow:
               ┌─────────────────────────┐    ┌─────────────────────────┐    ┌──────────────────────────┐
               │  ServiceBusQueueSource  │───▶│    OrderProcessor       │───▶│ ServiceBusQueueSinkNode  │
               │  <Order>               │    │  (Transform)            │    │ <IAcknowledgableMessage  │
               └─────────────────────────┘    └─────────────────────────┘    │  <ProcessedOrder>>      │
                         │                             │                      └──────────────────────────┘
                         ▼                             ▼                                 │
               Azure Service Bus             Order Validation                  Azure Service Bus
               (input-orders queue)          & Processing                      (processed-orders queue)

               Features Demonstrated:
               • Consuming messages from Azure Service Bus with explicit settlement
               • Automatic message lock renewal during processing
               • Order processing with status tracking
               • Reconnect behavior using channel-based push-to-pull bridging
               • AutoOnSinkSuccess acknowledgment — source message Completed only after successful publish
               • Dead-lettering messages that fail deserialization
               • Batch sending to output queue for throughput optimisation
               """;
    }
}

/// <summary>
///     Transform node that processes incoming Service Bus order messages.
/// </summary>
/// <remarks>
///     Extracts the <see cref="Order" /> from the <see cref="ServiceBusMessage{T}" /> envelope,
///     validates it, and returns a new <see cref="IAcknowledgableMessage{T}" /> of
///     <see cref="ProcessedOrder" /> using <c>WithBody</c>.  This preserves the original
///     message's settlement callbacks so the sink node can acknowledge it.
/// </remarks>
public sealed class OrderProcessor
    : TransformNode<ServiceBusMessage<Order>, IAcknowledgableMessage<ProcessedOrder>>
{
    /// <inheritdoc />
    public override async Task<IAcknowledgableMessage<ProcessedOrder>> ExecuteAsync(
        ServiceBusMessage<Order> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var order = input.Body;

        Console.WriteLine(
            $"[OrderProcessor] Processing Order #{order.OrderId} for Customer {order.CustomerId}, " +
            $"Amount: ${order.TotalAmount:F2}, DeliveryCount: {input.DeliveryCount}");

        // Simulate processing work
        await Task.Delay(50, cancellationToken);

        // Business validation
        if (order.TotalAmount <= 0)
        {
            Console.WriteLine($"  ⚠ Order #{order.OrderId} rejected: invalid amount ({order.TotalAmount})");

            var rejected = new ProcessedOrder
            {
                OrderId = order.OrderId,
                CustomerId = order.CustomerId,
                TotalAmount = order.TotalAmount,
                Status = "Rejected",
                ProcessingNotes = "Invalid order amount — must be greater than zero.",
            };

            return input.WithBody(rejected);
        }

        Console.WriteLine($"  ✓ Order #{order.OrderId} processed successfully");

        var processed = new ProcessedOrder
        {
            OrderId = order.OrderId,
            CustomerId = order.CustomerId,
            TotalAmount = order.TotalAmount,
            Status = "Completed",
            ProcessingNotes = $"Processed at {DateTime.UtcNow:O}",
        };

        return input.WithBody(processed);
    }
}
