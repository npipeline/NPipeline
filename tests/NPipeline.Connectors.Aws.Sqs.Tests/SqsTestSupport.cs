using System.Collections.Concurrent;
using Amazon.SQS;
using Amazon.SQS.Model;
using FakeItEasy;
using NPipeline.Connectors.Aws.Sqs.Models;
using NPipeline.Connectors.Messaging;
using NPipeline.DataFlow;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Aws.Sqs.Tests;

public sealed record Order(int Id, string Name);

/// <summary>Fakes and helpers shared by the SQS tests.</summary>
internal static class SqsTestSupport
{
    public const string QueueUrl = "https://sqs.us-east-1.amazonaws.com/123456789012/orders";

    /// <summary>
    ///     A client whose receives return <paramref name="batches" /> in turn, then wait until cancelled, as a long poll of an
    ///     empty queue does.
    /// </summary>
    public static IAmazonSQS ReceivingClient(params List<Message>?[] batches)
    {
        var client = A.Fake<IAmazonSQS>();
        var pending = new ConcurrentQueue<List<Message>?>(batches);

        A.CallTo(() => client.ReceiveMessageAsync(A<ReceiveMessageRequest>._, A<CancellationToken>._))
            .ReturnsLazily((ReceiveMessageRequest _, CancellationToken cancellationToken) => ReceiveAsync(pending, cancellationToken));

        return client;
    }

    public static Message Received(string id, string body, Dictionary<string, MessageAttributeValue>? attributes = null,
        Dictionary<string, string>? systemAttributes = null) =>
        new()
        {
            MessageId = id,
            ReceiptHandle = $"receipt-{id}",
            Body = body,
            MessageAttributes = attributes,
            Attributes = systemAttributes,
        };

    public static Message ReceivedOrder(int id) => Received($"m{id}", $$"""{"id":{{id}},"name":"order-{{id}}"}""");

    /// <summary>Every receipt handle the client was asked to delete, in order.</summary>
    public static List<string> DeletedHandles(IAmazonSQS client) =>
        [.. DeleteBatches(client).SelectMany(batch => batch)];

    /// <summary>The receipt handles of each <c>DeleteMessageBatch</c> call.</summary>
    public static List<List<string>> DeleteBatches(IAmazonSQS client) =>
    [
        .. Fake.GetCalls(client)
            .Where(call => call.Method.Name == nameof(IAmazonSQS.DeleteMessageBatchAsync))
            .Select(call => call.Arguments[1] switch
            {
                List<DeleteMessageBatchRequestEntry> entries => entries.Select(entry => entry.ReceiptHandle).ToList(),
                DeleteMessageBatchRequest request => request.Entries.Select(entry => entry.ReceiptHandle).ToList(),
                _ => [],
            }),
    ];

    /// <summary>An <see cref="SqsMessage{T}" /> as the source would build it, settled through the given callbacks.</summary>
    public static SqsMessage<T> Message<T>(T body, string id = "m1", Dictionary<string, MessageAttributeValue>? attributes = null,
        Dictionary<string, string>? systemAttributes = null, Func<CancellationToken, Task>? acknowledge = null,
        Func<bool, CancellationToken, Task>? reject = null) =>
        new(body, id, $"receipt-{id}", QueueUrl, attributes ?? [], systemAttributes ?? [],
            new MessageSettlement(acknowledge ?? (_ => Task.CompletedTask), reject ?? ((_, _) => Task.CompletedTask)));

    /// <summary>Reads <paramref name="count" /> messages, then ends the read.</summary>
    public static async Task<List<SqsMessage<T>>> ReadAsync<T>(IDataStream<SqsMessage<T>> stream, int count, CancellationToken cancellationToken = default)
    {
        var messages = new List<SqsMessage<T>>();

        if (count == 0)
            return messages;

        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));

        await foreach (var message in stream.WithCancellation(timeout.Token))
        {
            messages.Add(message);

            if (messages.Count == count)
                break;
        }

        return messages;
    }

    /// <summary>Waits up to five seconds for <paramref name="condition" />.</summary>
    public static async Task EventuallyAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);

        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("The condition did not become true.");

            await Task.Delay(10);
        }
    }

    private static async Task<ReceiveMessageResponse> ReceiveAsync(ConcurrentQueue<List<Message>?> pending, CancellationToken cancellationToken)
    {
        if (pending.TryDequeue(out var batch))
            return new ReceiveMessageResponse { Messages = batch };

        await Task.Delay(Timeout.Infinite, cancellationToken);
        throw new OperationCanceledException(cancellationToken);
    }
}

/// <summary>Bodies as plain UTF-8 text.</summary>
internal sealed class TextSerializer : IMessageSerializer
{
    public string ContentType => "text/plain";

    public byte[] Serialize<T>(T value, MessageContext context) => System.Text.Encoding.UTF8.GetBytes(value?.ToString() ?? string.Empty);

    public T Deserialize<T>(ReadOnlySpan<byte> body, MessageContext context) => (T)(object)System.Text.Encoding.UTF8.GetString(body);
}

internal sealed class CapturingDeadLetterSink : IDeadLetterSink
{
    public List<DeadLetterEnvelope> Captured { get; } = [];

    public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
    {
        lock (Captured)
        {
            Captured.Add(envelope);
        }

        return Task.CompletedTask;
    }
}
