using System.Text;
using Amazon.SQS;
using NPipeline.Connectors.Aws.Sqs;
using NPipeline.Connectors.Aws.Sqs.Models;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Messaging;
using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_SqsConnector;

/// <summary>
///     Receives orders from an input SQS queue, validates them, and sends the results to an output queue. Each input
///     message is deleted from its queue once its result has been sent.
/// </summary>
/// <param name="settings">The queue URLs.</param>
/// <param name="client">The SQS client the source and sink share; the caller owns it.</param>
public sealed class SqsConnectorPipeline(SqsSampleSettings settings, IAmazonSQS client) : IPipelineDefinition
{
    /// <inheritdoc />
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(
            SqsConnector.Source<Order>(settings.InputQueueUrl, o => o with
            {
                Client = client,
                MaxMessages = 10,
                WaitTime = TimeSpan.FromSeconds(20),

                // A message not acknowledged within 30 seconds becomes visible again and is redelivered.
                VisibilityTimeout = TimeSpan.FromSeconds(30),

                // A body that isn't a valid Order goes to the dead-letter sink and is then deleted.
                RowErrorHandler = _ => RowErrorAction.DeadLetter,
            }),
            "sqs-order-source");

        var process = builder.AddTransform<OrderProcessor, SqsMessage<Order>, IAcknowledgableMessage<ProcessedOrder>>("order-processor");

        // Acknowledging() deletes each input message once SQS has accepted its result.
        var sink = builder.AddSink(
            SqsConnector.Sink<ProcessedOrder>(settings.OutputQueueUrl, o => o with { Client = client }).Acknowledging(),
            "sqs-processed-order-sink");

        builder.Connect(source, process);
        builder.Connect(process, sink);

        builder.AddDeadLetterSink(new ConsoleDeadLetterSink());
    }

    /// <summary>Describes what the pipeline does.</summary>
    public static string GetDescription(SqsSampleSettings settings) =>
        $"""
         SqsConnector.Source<Order>              {settings.InputQueueUrl}
           -> OrderProcessor                     (message.WithBody(processed))
             -> SqsConnector.Sink<ProcessedOrder>.Acknowledging()   {settings.OutputQueueUrl}

         Undeserializable messages -> ConsoleDeadLetterSink
         """;
}

/// <summary>Validates each order, keeping the SQS message so it can be acknowledged downstream.</summary>
public sealed class OrderProcessor : TransformNode<SqsMessage<Order>, IAcknowledgableMessage<ProcessedOrder>>
{
    /// <inheritdoc />
    public override async ValueTask<IAcknowledgableMessage<ProcessedOrder>> TransformAsync(SqsMessage<Order> input, PipelineContext context,
        CancellationToken cancellationToken)
    {
        var order = input.Body;
        Console.WriteLine($"Processing order {order.OrderId} (receive #{input.ReceiveCount}), customer {order.CustomerId}, amount ${order.TotalAmount:F2}");

        // Simulate processing work.
        await Task.Delay(100, cancellationToken);

        var valid = order.TotalAmount > 0;
        Console.WriteLine(valid ? $"  Order {order.OrderId} completed" : $"  Order {order.OrderId} rejected: invalid amount");

        var processed = new ProcessedOrder(
            order.OrderId,
            order.CustomerId,
            order.TotalAmount,
            valid ? "Completed" : "Rejected",
            DateTime.UtcNow,
            valid ? "Order processed successfully" : "Invalid order amount");

        return input.WithBody(processed);
    }
}

/// <summary>Prints dead-lettered items: here, messages whose body didn't deserialize.</summary>
public sealed class ConsoleDeadLetterSink : IDeadLetterSink
{
    /// <inheritdoc />
    public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var description = envelope.Item is MessageFailure failure
            ? $"message {failure.MessageId} from {failure.Source}: {Encoding.UTF8.GetString(failure.Body.Span)}"
            : envelope.Item.ToString();

        Console.WriteLine($"  Dead-lettered {description} ({envelope.Error.Message})");
        return Task.CompletedTask;
    }
}
