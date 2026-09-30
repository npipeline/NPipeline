using System.Runtime.CompilerServices;
using Azure.Messaging.ServiceBus;
using NPipeline.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Configuration;
using NPipeline.Connectors.Azure.ServiceBus.Models;
using NPipeline.Connectors.Azure.ServiceBus.Nodes;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Messaging;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;
using SdkMessage = Azure.Messaging.ServiceBus.ServiceBusMessage;

namespace NPipeline.Connectors.Azure.ServiceBus.Tests.Integration;

public sealed record Order(int Id, string Customer);

[Collection(ServiceBusEmulator.Name)]
public sealed class ServiceBusConnectorIntegrationTests(ServiceBusFixture broker)
{
    private static readonly TimeSpan Wait = TimeSpan.FromSeconds(30);

    /// <summary>Long enough for a message that should not arrive to have arrived.</summary>
    private static readonly TimeSpan Quiet = TimeSpan.FromSeconds(3);

    private ServiceBusClient Client => broker.Client;

    [Fact]
    public async Task A_sink_sends_what_a_source_receives()
    {
        var queue = await broker.CreateQueueAsync();
        var orders = Enumerable.Range(1, 5).Select(i => new Order(i, $"customer {i}")).ToList();

        await WriteAsync(queue, orders);

        await using var source = ServiceBusConnector.Source<Order>(Client, queue);
        var received = await TakeAsync(source, new PipelineContext(), 5, Wait, m => m.AcknowledgeAsync());

        received.Select(m => m.Body).Should().Equal(orders);
        received.Should().OnlyContain(m => m.IsSettled && m.ContentType == "application/json");
        (await ReceiveRawAsync(queue, 1, Quiet)).Should().BeEmpty("every message was completed");
    }

    [Fact]
    public async Task A_source_feeds_an_acknowledging_sink_with_default_settings_and_is_completed()
    {
        var from = await broker.CreateQueueAsync();
        var to = await broker.CreateQueueAsync();
        await WriteAsync(from, Enumerable.Range(1, 5).Select(i => new Order(i, "c")).ToList());

        await using (var source = ServiceBusConnector.Source<Order>(Client, from))
        await using (var sink = ServiceBusConnector.Sink<Order>(Client, to))
        {
            using var cts = new CancellationTokenSource(Wait);
            var messages = Limit(source.OpenStream(new PipelineContext(), cts.Token), 5, cts.Token);

            var write = () => sink.Acknowledging().ConsumeAsync(Stream<IAcknowledgableMessage<Order>>(messages), new PipelineContext(), cts.Token);

            await write.Should().NotThrowAsync("the pipeline must finish, not hang");
        }

        (await ReceiveRawAsync(to, 5, Wait)).Select(m => m.Body.ToObjectFromJson<Order>(Web)!.Id).Should().Equal(1, 2, 3, 4, 5);
        (await ReceiveRawAsync(from, 1, Quiet)).Should().BeEmpty("the sink completed every message it sent");
    }

    [Fact]
    public async Task Message_properties_survive_a_queue_to_queue_hop()
    {
        var from = await broker.CreateQueueAsync(sessions: true);
        var to = await broker.CreateQueueAsync(sessions: true);

        await SendRawAsync(from, new SdkMessage(BinaryData.FromObjectAsJson(new Order(1, "c"), Web))
        {
            MessageId = "order-1",
            CorrelationId = "corr-1",
            SessionId = "session-a",
            Subject = "orders",
            ApplicationProperties = { ["tenant"] = "contoso", ["priority"] = 3 },
        });

        await using (var source = ServiceBusConnector.SessionSource<Order>(Client, from, configure: o => o with { SessionIdleTimeout = TimeSpan.FromSeconds(1) }))
        await using (var sink = ServiceBusConnector.Sink<Order>(Client, to))
        {
            using var cts = new CancellationTokenSource(Wait);
            var messages = Limit(source.OpenStream(new PipelineContext(), cts.Token), 1, cts.Token);
            await sink.Acknowledging().ConsumeAsync(Stream<IAcknowledgableMessage<Order>>(messages), new PipelineContext(), cts.Token);
        }

        await using var session = await Client.AcceptSessionAsync(to, "session-a");
        var copy = await session.ReceiveMessageAsync(Wait);

        copy.Should().NotBeNull();
        copy.MessageId.Should().Be("order-1");
        copy.CorrelationId.Should().Be("corr-1");
        copy.SessionId.Should().Be("session-a");
        copy.Subject.Should().Be("orders");
        copy.ApplicationProperties.Should().Contain("tenant", "contoso").And.Contain("priority", 3);
        copy.Body.ToObjectFromJson<Order>(Web).Should().Be(new Order(1, "c"));
        await session.CompleteMessageAsync(copy);
    }

    [Fact]
    public async Task A_session_source_reads_each_session_in_order()
    {
        var queue = await broker.CreateQueueAsync(sessions: true);

        await using (var sink = ServiceBusConnector.Sink<Order>(Client, queue, o => o with { SessionId = order => order.Customer }))
        {
            var orders = Enumerable.Range(1, 6).Select(i => new Order(i, i % 2 == 0 ? "even" : "odd")).ToList();
            await sink.ConsumeAsync(Stream(ToAsync(orders)), new PipelineContext(), CancellationToken.None);
        }

        await using var source = ServiceBusConnector.SessionSource<Order>(Client, queue,
            configure: o => o with { MaxConcurrentSessions = 2, SessionIdleTimeout = TimeSpan.FromSeconds(1) });

        var received = await TakeAsync(source, new PipelineContext(), 6, Wait, m => m.AcknowledgeAsync());

        received.Should().HaveCount(6);
        received.Should().OnlyContain(m => m.SessionId == m.Body.Customer);
        received.Where(m => m.SessionId == "odd").Select(m => m.Body.Id).Should().Equal(1, 3, 5);
        received.Where(m => m.SessionId == "even").Select(m => m.Body.Id).Should().Equal(2, 4, 6);
    }

    [Fact]
    public async Task A_skipped_message_goes_to_the_dead_letter_sub_queue()
    {
        var queue = await broker.CreateQueueAsync();
        await SendRawAsync(queue, new SdkMessage("not json") { MessageId = "bad" }, new SdkMessage(BinaryData.FromObjectAsJson(new Order(2, "c"), Web)));

        await using (var source = ServiceBusConnector.Source<Order>(Client, queue, o => o with { RowErrorHandler = _ => RowErrorAction.Skip }))
        {
            var received = await TakeAsync(source, new PipelineContext(), 1, Wait, m => m.AcknowledgeAsync());
            received.Select(m => m.Body.Id).Should().Equal(2);
        }

        var deadLettered = (await ReceiveRawAsync(queue, 1, Wait, SubQueue.DeadLetter)).Should().ContainSingle().Subject;
        deadLettered.MessageId.Should().Be("bad");
        deadLettered.DeadLetterReason.Should().Be("DeserializationFailed");
        deadLettered.Body.ToString().Should().Be("not json");
        (await ReceiveRawAsync(queue, 1, Quiet)).Should().BeEmpty();
    }

    [Fact]
    public async Task A_dead_lettered_message_reaches_the_pipeline_dead_letter_sink_and_is_completed()
    {
        var queue = await broker.CreateQueueAsync();
        await SendRawAsync(queue, new SdkMessage("not json") { MessageId = "bad" }, new SdkMessage(BinaryData.FromObjectAsJson(new Order(2, "c"), Web)));
        var deadLetters = new CapturingDeadLetterSink();
        var context = new PipelineContext(new PipelineContextConfiguration(DeadLetterSink: deadLetters));

        await using (var source = ServiceBusConnector.Source<Order>(Client, queue, o => o with { RowErrorHandler = _ => RowErrorAction.DeadLetter }))
        {
            var received = await TakeAsync(source, context, 1, Wait, m => m.AcknowledgeAsync());
            received.Select(m => m.Body.Id).Should().Equal(2);
        }

        var failure = deadLetters.Captured.Should().ContainSingle().Which.Item.Should().BeOfType<MessageFailure>().Subject;
        failure.MessageId.Should().Be("bad");
        failure.Source.Should().Be(queue);
        System.Text.Encoding.UTF8.GetString(failure.Body.Span).Should().Be("not json");

        (await ReceiveRawAsync(queue, 1, Quiet)).Should().BeEmpty("the message was completed");
        (await ReceiveRawAsync(queue, 1, Quiet, SubQueue.DeadLetter)).Should().BeEmpty("the pipeline's dead-letter sink handled it");
    }

    [Fact]
    public async Task A_message_that_does_not_deserialize_fails_the_read_by_default_and_is_delivered_again()
    {
        var queue = await broker.CreateQueueAsync();
        await SendRawAsync(queue, new SdkMessage("not json") { MessageId = "bad" });

        await using (var source = ServiceBusConnector.Source<Order>(Client, queue))
        {
            var read = () => TakeAsync(source, new PipelineContext(), 1, Wait, m => m.AcknowledgeAsync());
            await read.Should().ThrowAsync<Exception>();
        }

        var again = (await ReceiveRawAsync(queue, 1, Wait)).Should().ContainSingle().Subject;
        again.MessageId.Should().Be("bad");
        again.DeliveryCount.Should().Be(2, "the failed read abandoned it");
    }

    [Fact]
    public async Task A_message_rejected_with_requeue_is_delivered_again()
    {
        var queue = await broker.CreateQueueAsync();
        await WriteAsync(queue, [new Order(1, "c")]);

        await using var source = ServiceBusConnector.Source<Order>(Client, queue);
        var first = true;

        var received = await TakeAsync(source, new PipelineContext(), 2, Wait, async m =>
        {
            if (first)
            {
                first = false;
                await m.RejectAsync(true);
            }
            else
            {
                await m.AcknowledgeAsync();
            }
        });

        received.Select(m => m.Body.Id).Should().Equal(1, 1);
        received.Select(m => m.DeliveryCount).Should().Equal(1, 2);
        received[0].MessageId.Should().Be(received[1].MessageId);
    }

    [Fact]
    public async Task A_message_rejected_without_requeue_goes_to_the_dead_letter_sub_queue()
    {
        var queue = await broker.CreateQueueAsync();
        await WriteAsync(queue, [new Order(1, "c")]);

        await using (var source = ServiceBusConnector.Source<Order>(Client, queue))
        {
            (await TakeAsync(source, new PipelineContext(), 1, Wait, m => m.RejectAsync(false))).Should().ContainSingle();
        }

        var deadLettered = (await ReceiveRawAsync(queue, 1, Wait, SubQueue.DeadLetter)).Should().ContainSingle().Subject;
        deadLettered.DeadLetterReason.Should().Be("Rejected");
    }

    [Fact]
    public async Task A_message_held_longer_than_its_lock_is_still_completable()
    {
        var queue = await broker.CreateQueueAsync();
        await WriteAsync(queue, [new Order(1, "c")]);

        await using var source = ServiceBusConnector.Source<Order>(Client, queue);
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var messages = source.OpenStream(new PipelineContext(), cts.Token).GetAsyncEnumerator(cts.Token);

        try
        {
            (await messages.MoveNextAsync()).Should().BeTrue();
            var held = messages.Current;
            var next = messages.MoveNextAsync().AsTask();

            // Twice the lock duration: without renewal the lock would expire and the message be delivered again.
            await Task.Delay(ServiceBusFixture.LockDuration * 2 + TimeSpan.FromSeconds(1));

            next.IsCompleted.Should().BeFalse("the renewed lock kept the message from being delivered again");
            held.Received.LockedUntil.Should().BeAfter(DateTimeOffset.UtcNow, "the lock was renewed");

            var complete = () => held.AcknowledgeAsync();
            await complete.Should().NotThrowAsync();

            await cts.CancelAsync();
            await IgnoreCancellation(next);
        }
        finally
        {
            await messages.DisposeAsync();
        }

        (await ReceiveRawAsync(queue, 1, Quiet)).Should().BeEmpty("the message was completed");
    }

    [Fact]
    public async Task Without_renewal_a_message_held_longer_than_its_lock_is_delivered_again()
    {
        var queue = await broker.CreateQueueAsync();
        await WriteAsync(queue, [new Order(1, "c")]);

        await using var source = ServiceBusConnector.Source<Order>(Client, queue, o => o with { MaxLockRenewal = TimeSpan.Zero });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var messages = source.OpenStream(new PipelineContext(), cts.Token).GetAsyncEnumerator(cts.Token);

        try
        {
            (await messages.MoveNextAsync()).Should().BeTrue();
            var held = messages.Current;

            // The control for the test above: the emulator does expire locks.
            (await messages.MoveNextAsync().AsTask().WaitAsync(Wait)).Should().BeTrue("the lock expired, so the message was delivered again");
            messages.Current.DeliveryCount.Should().Be(2);

            var complete = () => held.AcknowledgeAsync();
            (await complete.Should().ThrowAsync<ServiceBusException>()).Which.Reason.Should().Be(ServiceBusFailureReason.MessageLockLost);
            await messages.Current.AcknowledgeAsync();
        }
        finally
        {
            await messages.DisposeAsync();
        }
    }

    [Fact]
    public async Task MaxInFlight_bounds_the_unsettled_messages()
    {
        var queue = await broker.CreateQueueAsync();
        await WriteAsync(queue, Enumerable.Range(1, 5).Select(i => new Order(i, "c")).ToList());

        await using var source = ServiceBusConnector.Source<Order>(Client, queue, o => o with { MaxInFlight = 2 });
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        var messages = source.OpenStream(new PipelineContext(), cts.Token).GetAsyncEnumerator(cts.Token);
        var received = new List<ServiceBusMessage<Order>>();

        try
        {
            for (var i = 0; i < 2; i++)
            {
                (await messages.MoveNextAsync()).Should().BeTrue();
                received.Add(messages.Current);
            }

            var third = messages.MoveNextAsync().AsTask();
            await Task.Delay(Quiet);
            third.IsCompleted.Should().BeFalse("two messages are unsettled, the most the source holds");

            await received[0].AcknowledgeAsync();
            (await third.WaitAsync(Wait)).Should().BeTrue("settling one made room for another");
            received.Add(messages.Current);

            var fourth = messages.MoveNextAsync().AsTask();
            await Task.Delay(Quiet);
            fourth.IsCompleted.Should().BeFalse();

            await received[1].AcknowledgeAsync();
            await received[2].AcknowledgeAsync();
            (await fourth.WaitAsync(Wait)).Should().BeTrue();
            received.Add(messages.Current);
            (await messages.MoveNextAsync().AsTask().WaitAsync(Wait)).Should().BeTrue();
            received.Add(messages.Current);

            foreach (var message in received.Skip(3))
            {
                await message.AcknowledgeAsync();
            }
        }
        finally
        {
            await messages.DisposeAsync();
        }

        received.Select(m => m.Body.Id).Should().Equal(1, 2, 3, 4, 5);
    }

    [Fact]
    public async Task A_failed_send_fails_the_write_by_default()
    {
        var queue = await broker.CreateQueueAsync();
        await using var sink = ServiceBusConnector.Sink<string>(Client, queue);

        var write = () => sink.ConsumeAsync(Stream(ToAsync([TooLarge()])), new PipelineContext(), CancellationToken.None);

        (await write.Should().ThrowAsync<ServiceBusException>()).Which.Reason.Should().Be(ServiceBusFailureReason.MessageSizeExceeded);
    }

    [Fact]
    public async Task FailedMessages_DeadLetter_sends_the_body_to_the_dead_letter_sink_and_continues()
    {
        var queue = await broker.CreateQueueAsync();
        var deadLetters = new CapturingDeadLetterSink();
        var context = new PipelineContext(new PipelineContextConfiguration(DeadLetterSink: deadLetters));
        var tooLarge = TooLarge();

        await using (var sink = ServiceBusConnector.Sink<string>(Client, queue, o => o with { FailedMessages = FailedMessageAction.DeadLetter }))
        {
            await sink.ConsumeAsync(Stream(ToAsync(["first", tooLarge, "last"])), context, CancellationToken.None);
        }

        var envelope = deadLetters.Captured.Should().ContainSingle().Subject;
        envelope.Item.Should().Be(tooLarge);
        envelope.Error.Should().BeOfType<ServiceBusException>().Which.Reason.Should().Be(ServiceBusFailureReason.MessageSizeExceeded);

        (await ReceiveRawAsync(queue, 2, Wait)).Select(m => m.Body.ToObjectFromJson<string>()).Should().Equal("first", "last");
    }

    [Fact]
    public async Task FailedMessages_DeadLetter_completes_the_source_message()
    {
        var from = await broker.CreateQueueAsync();
        var to = await broker.CreateQueueAsync();
        await WriteAsync(from, [new Order(1, "c")]);
        var deadLetters = new CapturingDeadLetterSink();
        var context = new PipelineContext(new PipelineContextConfiguration(DeadLetterSink: deadLetters));

        await using (var source = ServiceBusConnector.Source<Order>(Client, from))
        await using (var sink = ServiceBusConnector.Sink<string>(Client, to, o => o with { FailedMessages = FailedMessageAction.DeadLetter }))
        {
            using var cts = new CancellationTokenSource(Wait);
            var messages = Enlarge(Limit(source.OpenStream(context, cts.Token), 1, cts.Token));
            await sink.Acknowledging().ConsumeAsync(Stream(messages), context, cts.Token);
        }

        deadLetters.Captured.Should().ContainSingle();
        (await ReceiveRawAsync(from, 1, Quiet)).Should().BeEmpty("the dead-lettered message was completed");
    }

    [Fact]
    public async Task FailedMessages_Requeue_abandons_the_source_message()
    {
        var from = await broker.CreateQueueAsync();
        var to = await broker.CreateQueueAsync();
        await WriteAsync(from, [new Order(1, "c")]);

        await using (var source = ServiceBusConnector.Source<Order>(Client, from))
        await using (var sink = ServiceBusConnector.Sink<string>(Client, to, o => o with { FailedMessages = FailedMessageAction.Requeue }))
        {
            using var cts = new CancellationTokenSource(Wait);
            var messages = Enlarge(Limit(source.OpenStream(new PipelineContext(), cts.Token), 1, cts.Token));
            await sink.Acknowledging().ConsumeAsync(Stream(messages), new PipelineContext(), cts.Token);
        }

        var again = (await ReceiveRawAsync(from, 1, Wait)).Should().ContainSingle().Subject;
        again.DeliveryCount.Should().Be(2, "the failed send abandoned it, so it was delivered again");
        (await ReceiveRawAsync(to, 1, Quiet)).Should().BeEmpty();
    }

    private static readonly System.Text.Json.JsonSerializerOptions Web = new(System.Text.Json.JsonSerializerDefaults.Web);

    /// <summary>A body larger than any Service Bus message, so its send fails.</summary>
    private static string TooLarge() => new('x', 2 * 1024 * 1024);

    private static async IAsyncEnumerable<IAcknowledgableMessage<string>> Enlarge(IAsyncEnumerable<ServiceBusMessage<Order>> messages)
    {
        await foreach (var message in messages)
        {
            yield return message.WithBody(TooLarge());
        }
    }

    private async Task WriteAsync(string queue, IReadOnlyList<Order> orders)
    {
        using var cts = new CancellationTokenSource(Wait);
        await using var sink = ServiceBusConnector.Sink<Order>(Client, queue);
        await sink.ConsumeAsync(Stream(ToAsync(orders)), new PipelineContext(), cts.Token);
    }

    private async Task SendRawAsync(string queue, params SdkMessage[] messages)
    {
        await using var sender = Client.CreateSender(queue);
        await sender.SendMessagesAsync(messages);
    }

    /// <summary>Receives and completes up to <paramref name="count" /> messages, waiting at most <paramref name="timeout" />.</summary>
    private async Task<List<ServiceBusReceivedMessage>> ReceiveRawAsync(string queue, int count, TimeSpan timeout, SubQueue subQueue = SubQueue.None)
    {
        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions { SubQueue = subQueue });
        var messages = new List<ServiceBusReceivedMessage>();
        var deadline = DateTime.UtcNow + timeout;

        while (messages.Count < count && DateTime.UtcNow < deadline)
        {
            foreach (var message in await receiver.ReceiveMessagesAsync(count - messages.Count, TimeSpan.FromSeconds(1)))
            {
                messages.Add(message);
                await receiver.CompleteMessageAsync(message);
            }
        }

        return messages;
    }

    /// <summary>The first <paramref name="count" /> messages, or fewer if <paramref name="timeout" /> passes first.</summary>
    private static async Task<List<ServiceBusMessage<T>>> TakeAsync<T>(ServiceBusSourceNode<T> source, PipelineContext context, int count,
        TimeSpan timeout, Func<ServiceBusMessage<T>, Task>? onItem = null)
    {
        using var cts = new CancellationTokenSource(timeout);
        var items = new List<ServiceBusMessage<T>>();

        try
        {
            await foreach (var item in source.OpenStream(context, cts.Token).WithCancellation(cts.Token))
            {
                items.Add(item);

                if (onItem is not null)
                    await onItem(item);

                if (items.Count >= count)
                    break;
            }
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested)
        {
            // The timeout ends the read with what arrived.
        }

        return items;
    }

    private static async IAsyncEnumerable<T> Limit<T>(IAsyncEnumerable<T> source, int count, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var taken = 0;

        await foreach (var item in source.WithCancellation(cancellationToken))
        {
            yield return item;

            if (++taken >= count)
                yield break;
        }
    }

    private static async IAsyncEnumerable<T> ToAsync<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        await Task.CompletedTask;
    }

    private static IDataStream<T> Stream<T>(IAsyncEnumerable<T> items) => new DataStream<T>(items, "test");

    private static async Task IgnoreCancellation(Task task)
    {
        try
        {
            await task;
        }
        catch (OperationCanceledException)
        {
            // Expected: the read was stopped.
        }
    }
}

/// <summary>Collects dead letters.</summary>
public sealed class CapturingDeadLetterSink : IDeadLetterSink
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
