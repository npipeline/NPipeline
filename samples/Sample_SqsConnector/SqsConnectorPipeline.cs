using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Aws.Sqs.Configuration;
using NPipeline.Connectors.Aws.Sqs.Models;
using NPipeline.Connectors.Aws.Sqs.Nodes;
using NPipeline.Connectors.Configuration;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_SqsConnector;

/// <summary>
///     Pipeline demonstrating SQS connector usage for order processing.
/// </summary>
/// <remarks>
///     This sample demonstrates:
///     1. Consuming messages from an SQS queue using SqsSourceNode
///     2. Processing orders through a transform node
///     3. Publishing processed orders to an output SQS queue using SqsSinkNode
///     4. Automatic message acknowledgment (default behavior)
/// </remarks>
public sealed class SqsConnectorPipeline : IPipelineDefinition
{
    private const string InputQueueUrl = "https://sqs.{region}.amazonaws.com/{account-id}/input-orders-queue";
    private const string OutputQueueUrl = "https://sqs.{region}.amazonaws.com/{account-id}/processed-orders-queue";
    private const string Region = "us-east-1";

    /// <inheritdoc />
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add SQS source node to consume orders from input queue
        // SqsSourceNode<Order> outputs SqsMessage<Order>
        var sourceNode = builder.AddSource<SqsSourceNode<Order>, SqsMessage<Order>>(
            "SqsOrderSource");

        // Add transform node to process orders
        // Takes SqsMessage<Order> and outputs IAcknowledgableMessage<ProcessedOrder>
        var transformNode = builder.AddTransform<OrderProcessor, SqsMessage<Order>, IAcknowledgableMessage<ProcessedOrder>>(
            "OrderProcessor");

        // Add SQS sink node to publish processed orders to output queue
        var sinkNode = builder.AddSink<SqsSinkNode<IAcknowledgableMessage<ProcessedOrder>>, IAcknowledgableMessage<ProcessedOrder>>(
            "SqsProcessedOrderSink");

        // Connect the nodes to form the pipeline
        _ = builder.Connect(sourceNode, transformNode);
        _ = builder.Connect(transformNode, sinkNode);
    }

    /// <summary>
    ///     Creates a default SqsConfiguration for this sample.
    /// </summary>
    public static SqsConfiguration CreateConfiguration()
    {
        return new SqsConfiguration
        {
            Region = Region,
            SourceQueueUrl = InputQueueUrl,
            SinkQueueUrl = OutputQueueUrl,
            MaxNumberOfMessages = 10,
            WaitTimeSeconds = 20,
            VisibilityTimeout = 30,
            AcknowledgmentStrategy = AcknowledgmentStrategy.AutoOnSinkSuccess,
        };
    }

    /// <summary>
    ///     Gets a description of the pipeline structure and purpose.
    /// </summary>
    /// <returns>A human-readable description of the pipeline.</returns>
    public static string GetDescription()
    {
        return """
               Pipeline Structure:
               ┌─────────────────────────────────────────────────────────────────────────────┐
               │ SQS Order Processing Pipeline                                               │
               └─────────────────────────────────────────────────────────────────────────────┘

               Flow:
               ┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
               │  SqsSourceNode   │─────▶│  OrderProcessor  │─────▶│   SqsSinkNode    │
               │   (Order)        │      │  (Transform)     │      │ (ProcessedOrder) │
               └──────────────────┘      └──────────────────┘      └──────────────────┘
                       │                           │                         │
                       ▼                           ▼                         ▼
               Input SQS Queue              Order Processing          Output SQS Queue
               (input-orders-queue)         & Validation              (processed-orders-queue)

               Features Demonstrated:
               • Continuous message polling from SQS
               • Automatic message acknowledgment on successful processing
               • Order validation and status updates
               • Publishing processed orders to output queue
               • JSON serialization/deserialization
               """;
    }
}

/// <summary>
///     Transform node that processes SQS order messages.
/// </summary>
/// <remarks>
///     This node extracts the Order from the SqsMessage envelope,
///     validates it, and returns a ProcessedOrder with the result.
/// </remarks>
public sealed class OrderProcessor : TransformNode<SqsMessage<Order>, IAcknowledgableMessage<ProcessedOrder>>
{
    /// <inheritdoc />
    public override async Task<IAcknowledgableMessage<ProcessedOrder>> ExecuteAsync(
        SqsMessage<Order> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var order = input.Body;
        Console.WriteLine($"Processing Order ID: {order.OrderId}, Customer: {order.CustomerId}, Amount: ${order.TotalAmount:F2}");

        // Simulate order processing
        await Task.Delay(100, cancellationToken);

        // Validate order
        if (order.TotalAmount <= 0)
        {
            Console.WriteLine($"  ⚠ Order {order.OrderId} rejected: Invalid amount");

            var rejected = new ProcessedOrder
            {
                OrderId = order.OrderId,
                CustomerId = order.CustomerId,
                TotalAmount = order.TotalAmount,
                Status = "Rejected",
                ProcessedAt = DateTime.UtcNow,
                ProcessingNotes = "Invalid order amount",
            };

            return input.WithBody(rejected);
        }

        // Process successful order
        Console.WriteLine($"  ✓ Order {order.OrderId} processed successfully");

        var processed = new ProcessedOrder
        {
            OrderId = order.OrderId,
            CustomerId = order.CustomerId,
            TotalAmount = order.TotalAmount,
            Status = "Completed",
            ProcessedAt = DateTime.UtcNow,
            ProcessingNotes = "Order processed successfully",
        };

        return input.WithBody(processed);
    }
}
