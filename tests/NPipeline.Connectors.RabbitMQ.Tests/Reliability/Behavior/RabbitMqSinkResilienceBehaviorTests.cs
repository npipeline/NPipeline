using System.Security.Authentication;
using FakeItEasy;
using FakeItEasy.Core;
using NPipeline.Configuration;
using NPipeline.Connectors.Messaging;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Nodes;
using NPipeline.Connectors.RabbitMQ.Reliability;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;
using NResilience;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace NPipeline.Connectors.RabbitMQ.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the RabbitMQ sink's publish resilience and settlement of source messages.
/// </summary>
public sealed class RabbitMqSinkResilienceBehaviorTests
{
    private static readonly Resilience FastRetries = RabbitMqConnectorResilience.Default with
    {
        Backoff = RabbitMqConnectorResilience.Default.Backoff with { TransientBase = TimeSpan.FromMilliseconds(1) },
    };

    // RabbitMqSinkNode publishes a batch once outside the resilience policy, then retries each failure through the full
    // policy: a failed message gets Attempts + 1 publishes, a permanent failure is published twice, and the first retry
    // has no backoff. These tests describe the documented behaviour and are skipped until the sink is fixed.
    // Classifier

    public static TheoryData<Exception, VerdictKind> ClassifiedExceptions => new()
    {
        { ConnectionLost(), VerdictKind.Transient },
        { Closed(Constants.InternalError), VerdictKind.Transient },
        { new BrokerUnreachableException(new IOException("connection refused")), VerdictKind.Transient },
        { new ConnectFailureException("connect failed", new IOException("reset")), VerdictKind.Transient },
        { new ChannelAllocationException(), VerdictKind.Transient },
        { new PublishException(1, false), VerdictKind.Transient },
        { new TimeoutException(), VerdictKind.Transient },
        { Closed(Constants.AccessRefused), VerdictKind.Permanent },
        { Closed(Constants.NotFound), VerdictKind.Permanent },
        { Closed(Constants.PreconditionFailed), VerdictKind.Permanent },
        { Closed(Constants.ResourceLocked), VerdictKind.Permanent },
        { Closed(Constants.NotAllowed), VerdictKind.Permanent },
        { new OperationInterruptedException(Shutdown(Constants.NotFound, ShutdownInitiator.Peer)), VerdictKind.Permanent },
        { Closed(Constants.ReplySuccess, ShutdownInitiator.Application), VerdictKind.Permanent },
        { new PublishReturnException(1, "returned", "orders", "missing", Constants.NoRoute, "NO_ROUTE"), VerdictKind.Permanent },
        { new BrokerUnreachableException(new AuthenticationFailureException("ACCESS_REFUSED")), VerdictKind.Permanent },
        { new AuthenticationFailureException("ACCESS_REFUSED"), VerdictKind.Permanent },
        { new AuthenticationException("tls"), VerdictKind.Permanent },
        { new InvalidOperationException("serializer"), VerdictKind.Permanent },
    };

    // Preset

    [Fact]
    public void DefaultPreset_ReproducesTheOldRetrySettings()
    {
        var preset = RabbitMqConnectorResilience.Default;

        // MaxRetries = 3 made four calls; RetryBaseDelayMs = 100 was the first delay.
        preset.Attempts.Should().Be(4);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromMilliseconds(100));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(30));
        preset.Backoff.Jitter.Should().Be(Jitter.Full);
        preset.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Adaptive.Should().BeFalse();
        preset.Classifier.Should().BeSameAs(RabbitMqConnectorResilience.Classifier);
        new RabbitMqWriteOptions<string> { Exchange = "orders", Connection = A.Fake<IRabbitMqConnectionManager>() }.Resilience.Should().BeSameAs(preset);
    }

[Fact]
    public async Task DefaultPreset_MakesFourAttempts_BeforeATransientFailureSurfaces()
    {
        var (channel, connectionManager) = CreateChannel();
        FailPublishes(channel, () => ConnectionLost());

        var sink = new RabbitMqSinkNode<string>(Options(connectionManager));

        var act = () => RunAsync(sink, "order-1");

        _ = await act.Should().ThrowAsync<AlreadyClosedException>();
        PublishCount(channel).Should().Be(4, "MaxRetries = 3 made one call and three retries");
    }

    [Theory]
    [MemberData(nameof(ClassifiedExceptions), DisableDiscoveryEnumeration = true)]
    public void Classifier_JudgesRabbitMqExceptions(Exception exception, VerdictKind expected)
    {
        RabbitMqConnectorResilience.Classifier.ClassifyException(exception).Kind.Should().Be(expected);
    }

[Fact]
    public async Task PermanentFailure_IsPublishedOnce()
    {
        var (channel, connectionManager) = CreateChannel();
        FailPublishes(channel, () => Closed(Constants.NotFound));

        var sink = new RabbitMqSinkNode<string>(Options(connectionManager, "missing"));

        var act = () => RunAsync(sink, "order-1");

        _ = await act.Should().ThrowAsync<AlreadyClosedException>();
        PublishCount(channel).Should().Be(1, "publishing to an exchange that does not exist fails the same way every time");
    }

    [Fact]
    public async Task ClosedChannel_IsReplacedBeforeTheRetry()
    {
        var closed = A.Fake<IChannel>();
        A.CallTo(() => closed.IsOpen).Returns(false);
        FailPublishes(closed, () => ConnectionLost());

        var fresh = A.Fake<IChannel>();
        A.CallTo(() => fresh.IsOpen).Returns(true);

        var connectionManager = A.Fake<IRabbitMqConnectionManager>();
        A.CallTo(() => connectionManager.GetPooledChannelAsync(A<bool>._, A<CancellationToken>._)).ReturnsNextFromSequence(closed, fresh);

        var sink = new RabbitMqSinkNode<string>(Options(connectionManager));

        await RunAsync(sink, "order-1");

        PublishCount(closed).Should().Be(1);
        PublishCount(fresh).Should().Be(1, "a closed channel never reopens, so the retry takes another from the pool");
        A.CallTo(() => connectionManager.ReturnChannel(closed)).MustHaveHappenedOnceExactly();
        A.CallTo(() => connectionManager.ReturnChannel(fresh)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task EveryRetry_TakesAPooledChannel_AndReturnsIt()
    {
        var (channel, connectionManager) = CreateChannel();
        FailPublishes(channel, () => ConnectionLost(), 2);

        await RunAsync(new RabbitMqSinkNode<string>(Options(connectionManager)), "order-1");

        // One channel for the batch, then one for each retry.
        A.CallTo(() => connectionManager.GetPooledChannelAsync(true, A<CancellationToken>._)).MustHaveHappened(3, Times.Exactly);
        A.CallTo(() => connectionManager.ReturnChannel(channel)).MustHaveHappened(3, Times.Exactly);
    }

    // Cancellation

[Fact]
    public async Task PipelineCancellation_DuringBackoff_StopsWithoutRetrying()
    {
        var (channel, connectionManager) = CreateChannel();
        FailPublishes(channel, () => ConnectionLost());

        var slowRetries = RabbitMqConnectorResilience.Default with
        {
            Backoff = Backoff.Constant(TimeSpan.FromSeconds(30)),
        };

        var message = Message("order-1");
        var sink = new RabbitMqSinkNode<string>(Options(connectionManager) with { Resilience = slowRetries, FailedMessages = FailedMessageAction.Requeue });

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        var act = () => RunMessagesAsync(sink, cts.Token, message);

        _ = await act.Should().ThrowAsync<OperationCanceledException>("FailedMessages must not swallow the pipeline's own cancellation");
        PublishCount(channel).Should().Be(1, "the pipeline stopped during the backoff before the first retry");
        A.CallTo(() => message.RejectAsync(A<bool>._, A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => message.AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task CancellationNotRequestedByThePipeline_IsAFailure()
    {
        var (channel, connectionManager) = CreateChannel();
        FailPublishes(channel, () => new OperationCanceledException("internal timeout"));

        var sink = new RabbitMqSinkNode<string>(Options(connectionManager));

        var act = () => RunAsync(sink, "order-1");

        // Not the pipeline's cancellation and not the confirm deadline, so it is a permanent failure, published once.
        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        PublishCount(channel).Should().Be(1);
    }

    // Idempotency

    [Fact]
    public async Task AcknowledgementFailure_AfterASuccessfulPublish_DoesNotRepublish()
    {
        var (channel, connectionManager) = CreateChannel();

        // The source message's acknowledgement fails after the publish to the exchange has succeeded.
        var message = Message("order-1");
        A.CallTo(() => message.AcknowledgeAsync(A<CancellationToken>._)).ThrowsAsync(new InvalidOperationException("ack failed"));

        var sink = new RabbitMqSinkNode<string>(Options(connectionManager));

        var act = () => RunMessagesAsync(sink, CancellationToken.None, message);

        _ = await act.Should().ThrowAsync<InvalidOperationException>();
        PublishCount(channel).Should().Be(1, "the message already reached the exchange; publishing it again duplicates it");
    }

    [Fact]
    public async Task FailedPublish_DoesNotAcknowledgeTheSourceMessage()
    {
        var (channel, connectionManager) = CreateChannel();
        FailPublishes(channel, () => Closed(Constants.AccessRefused));

        var message = Message("order-1");
        var sink = new RabbitMqSinkNode<string>(Options(connectionManager));

        var act = () => RunMessagesAsync(sink, CancellationToken.None, message);

        _ = await act.Should().ThrowAsync<AlreadyClosedException>();
        A.CallTo(() => message.AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => message.RejectAsync(A<bool>._, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task Retries_CarryTheSameMessageId()
    {
        var (channel, connectionManager) = CreateChannel();
        FailPublishes(channel, () => ConnectionLost(), 2);

        await RunAsync(new RabbitMqSinkNode<string>(Options(connectionManager)), "order-1");

        var messageIds = PublishCalls(channel).Select(call => ((BasicProperties)call.Arguments[3]!).MessageId).ToList();
        messageIds.Should().HaveCount(3);
        messageIds.Distinct().Should().ContainSingle("a consumer can only discard a duplicate that carries the original's ID");
    }

    [Fact]
    public async Task Republished_Message_KeepsTheSourceMessageId()
    {
        var (channel, connectionManager) = CreateChannel();
        var message = Message("order-1");
        A.CallTo(() => message.MessageId).Returns("source-id");

        await RunMessagesAsync(new RabbitMqSinkNode<string>(Options(connectionManager)), CancellationToken.None, message);

        PublishCalls(channel).Select(call => ((BasicProperties)call.Arguments[3]!).MessageId).Should().Equal("source-id");
    }

    [Fact]
    public async Task BatchedPublish_RetriesOnlyTheFailedMessage()
    {
        var (channel, connectionManager) = CreateChannel();
        var bodies = new List<string>();

        A.CallTo(channel)
            .Where(call => call.Method.Name == nameof(IChannel.BasicPublishAsync))
            .WithReturnType<ValueTask>()
            .ReturnsLazily(call =>
            {
                var routingKey = (string)call.Arguments[1]!;
                bodies.Add(routingKey);

                // The second message fails once; the first must not be published again when it is retried.
                return routingKey == "order-2" && bodies.Count(b => b == "order-2") == 1
                    ? ValueTask.FromException(ConnectionLost())
                    : ValueTask.CompletedTask;
            });

        await RunAsync(new RabbitMqSinkNode<string>(BatchedOptions(connectionManager, 2)), "order-1", "order-2");

        bodies.Should().Equal("order-1", "order-2", "order-2");
    }

    // Publisher confirms

[Fact]
    public async Task ConfirmThatNeverArrives_FailsEachAttemptAsATimeout_AndIsRetried()
    {
        var (channel, connectionManager) = CreateChannel();
        WaitForeverForConfirm(channel);

        var sink = new RabbitMqSinkNode<string>(Options(connectionManager) with { ConfirmTimeout = TimeSpan.FromMilliseconds(50) });

        var act = () => RunAsync(sink, "order-1");

        _ = await act.Should().ThrowAsync<TimeoutException>();
        PublishCount(channel).Should().Be(4, "a missing confirm is transient, so every attempt is spent");
    }

    [Fact]
    public async Task ConfirmTimeout_ThenAConfirmedRetry_Succeeds()
    {
        var (channel, connectionManager) = CreateChannel();
        WaitForeverForConfirm(channel, 1);

        var message = Message("order-1");
        var sink = new RabbitMqSinkNode<string>(Options(connectionManager) with { ConfirmTimeout = TimeSpan.FromMilliseconds(50) });

        await RunMessagesAsync(sink, CancellationToken.None, message);

        PublishCount(channel).Should().Be(2);
        A.CallTo(() => message.AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task ConfirmsOff_UsesAChannelWithoutConfirms_AndConfirmTimeoutDoesNotApply()
    {
        var (channel, connectionManager) = CreateChannel();

        // Slower than ConfirmTimeout; with confirms off there is no confirm wait to time out.
        A.CallTo(channel)
            .Where(call => call.Method.Name == nameof(IChannel.BasicPublishAsync))
            .WithReturnType<ValueTask>()
            .ReturnsLazily(call => new ValueTask(Task.Delay(150, (CancellationToken)call.Arguments[5]!)));

        var options = Options(connectionManager) with
        {
            PublisherConfirms = false,
            ConfirmTimeout = TimeSpan.FromMilliseconds(20),
        };

        await RunAsync(new RabbitMqSinkNode<string>(options), "order-1");

        PublishCount(channel).Should().Be(1);
        A.CallTo(() => connectionManager.GetPooledChannelAsync(false, A<CancellationToken>._)).MustHaveHappened();
        A.CallTo(() => connectionManager.GetPooledChannelAsync(true, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task ConfirmsOn_UsesAChannelWithConfirms()
    {
        var (_, connectionManager) = CreateChannel();

        await RunAsync(new RabbitMqSinkNode<string>(Options(connectionManager)), "order-1");

        A.CallTo(() => connectionManager.GetPooledChannelAsync(true, A<CancellationToken>._)).MustHaveHappened();
        A.CallTo(() => connectionManager.GetPooledChannelAsync(false, A<CancellationToken>._)).MustNotHaveHappened();
    }

    // Failed messages

    [Fact]
    public async Task Fail_AcknowledgesTheBatchesPublishedMessages_AndLeavesTheFailedOneUnsettled()
    {
        var (channel, connectionManager) = CreateChannel();
        FailRoutingKey(channel, "order-2");
        var messages = Enumerable.Range(1, 3).Select(i => Message($"order-{i}")).ToArray();

        var sink = new RabbitMqSinkNode<string>(BatchedOptions(connectionManager, 3));

        var act = () => RunMessagesAsync(sink, CancellationToken.None, messages);

        _ = await act.Should().ThrowAsync<AlreadyClosedException>();

        // order-1 and order-3 reached the exchange; leaving them unacknowledged would redeliver and republish them.
        A.CallTo(() => messages[0].AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => messages[1].AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => messages[1].RejectAsync(A<bool>._, A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => messages[2].AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task Requeue_RejectsTheFailedSourceMessageWithRequeue_AndContinues()
    {
        var (channel, connectionManager) = CreateChannel();
        FailRoutingKey(channel, "order-2");
        var messages = Enumerable.Range(1, 3).Select(i => Message($"order-{i}")).ToArray();

        var sink = new RabbitMqSinkNode<string>(BatchedOptions(connectionManager, 3) with { FailedMessages = FailedMessageAction.Requeue });

        await RunMessagesAsync(sink, CancellationToken.None, messages);

        A.CallTo(() => messages[0].AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => messages[1].RejectAsync(true, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => messages[1].AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => messages[2].AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task DeadLetter_SendsTheBodyToTheDeadLetterSink_AndAcknowledgesTheSourceMessage()
    {
        var (channel, connectionManager) = CreateChannel();
        FailRoutingKey(channel, "order-2");
        var messages = Enumerable.Range(1, 3).Select(i => Message($"order-{i}")).ToArray();
        var deadLetters = new CapturingDeadLetterSink();

        var sink = new RabbitMqSinkNode<string>(BatchedOptions(connectionManager, 3) with { FailedMessages = FailedMessageAction.DeadLetter });

        await using var context = new PipelineContext(new PipelineContextConfiguration(DeadLetterSink: deadLetters));
        await using var input = new InMemoryDataStream<IAcknowledgableMessage<string>>(messages);
        await sink.ConsumeMessagesAsync(input, context, CancellationToken.None);

        deadLetters.Captured.Should().ContainSingle();
        deadLetters.Captured[0].Item.Should().Be("order-2");
        deadLetters.Captured[0].Error.Should().BeOfType<AlreadyClosedException>();

        foreach (var message in messages)
        {
            A.CallTo(() => message.AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        }

        A.CallTo(() => messages[1].RejectAsync(A<bool>._, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task Acknowledging_RoutesMessagesThroughTheSinksOwnSettlement()
    {
        var (channel, connectionManager) = CreateChannel();
        FailRoutingKey(channel, "order-2");
        var messages = Enumerable.Range(1, 2).Select(i => Message($"order-{i}")).ToArray();

        var sink = new RabbitMqSinkNode<string>(BatchedOptions(connectionManager, 2) with { FailedMessages = FailedMessageAction.Requeue }).Acknowledging();

        await using var input = new InMemoryDataStream<IAcknowledgableMessage<string>>(messages);
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);

        // The generic wrapper would acknowledge everything at the end; the sink's own handling requeues the failure.
        A.CallTo(() => messages[0].AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => messages[1].RejectAsync(true, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => messages[1].AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
    }

    private static RabbitMqWriteOptions<string> Options(IRabbitMqConnectionManager connectionManager, string exchange = "orders") => new()
    {
        Exchange = exchange,
        Connection = connectionManager,
        Serializer = A.Fake<IMessageSerializer>(),
        Resilience = FastRetries,
    };

    /// <summary>One batch of <paramref name="size" />, routed by body so a test can fail one message.</summary>
    private static RabbitMqWriteOptions<string> BatchedOptions(IRabbitMqConnectionManager connectionManager, int size) =>
        Options(connectionManager) with
        {
            RoutingKeySelector = body => body,
            BatchSize = size,
            BatchLinger = TimeSpan.FromMinutes(1),
        };

    private static IAcknowledgableMessage<string> Message(string body)
    {
        var message = A.Fake<IAcknowledgableMessage<string>>();
        A.CallTo(() => message.Body).Returns(body);
        return message;
    }

    private static void FailRoutingKey(IChannel channel, string routingKey)
    {
        A.CallTo(channel)
            .Where(call => call.Method.Name == nameof(IChannel.BasicPublishAsync))
            .WithReturnType<ValueTask>()
            .ReturnsLazily(call => (string)call.Arguments[1]! == routingKey
                ? ValueTask.FromException(Closed(Constants.AccessRefused))
                : ValueTask.CompletedTask);
    }

    private static void WaitForeverForConfirm(IChannel channel, int? times = null)
    {
        var rule = A.CallTo(channel)
            .Where(call => call.Method.Name == nameof(IChannel.BasicPublishAsync))
            .WithReturnType<ValueTask>()
            .ReturnsLazily(call => new ValueTask(Task.Delay(Timeout.Infinite, (CancellationToken)call.Arguments[5]!)));

        if (times is { } count)
            rule.NumberOfTimes(count);
    }

    private static (IChannel Channel, IRabbitMqConnectionManager ConnectionManager) CreateChannel()
    {
        var channel = A.Fake<IChannel>();
        A.CallTo(() => channel.IsOpen).Returns(true);

        var connectionManager = A.Fake<IRabbitMqConnectionManager>();
        A.CallTo(() => connectionManager.GetPooledChannelAsync(A<bool>._, A<CancellationToken>._)).Returns(channel);
        return (channel, connectionManager);
    }

    private static void FailPublishes(IChannel channel, Func<Exception> failure, int? times = null)
    {
        var rule = A.CallTo(channel)
            .Where(call => call.Method.Name == nameof(IChannel.BasicPublishAsync))
            .WithReturnType<ValueTask>()
            .ReturnsLazily(() => ValueTask.FromException(failure()));

        if (times is { } count)
            rule.NumberOfTimes(count);
    }

    private static IEnumerable<ICompletedFakeObjectCall> PublishCalls(IChannel channel)
    {
        return Fake.GetCalls(channel).Where(call => call.Method.Name == nameof(IChannel.BasicPublishAsync));
    }

    private static int PublishCount(IChannel channel) => PublishCalls(channel).Count();

    private static async Task RunAsync(RabbitMqSinkNode<string> sink, params string[] items)
    {
        await using var input = new InMemoryDataStream<string>(items);
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
    }

    private static async Task RunMessagesAsync(RabbitMqSinkNode<string> sink, CancellationToken cancellationToken,
        params IAcknowledgableMessage<string>[] messages)
    {
        await using var input = new InMemoryDataStream<IAcknowledgableMessage<string>>(messages);
        await sink.ConsumeMessagesAsync(input, new PipelineContext(), cancellationToken);
    }

    private static ShutdownEventArgs Shutdown(ushort replyCode, ShutdownInitiator initiator) =>
        new(initiator, replyCode, "closed", (object?)null, CancellationToken.None);

    private static AlreadyClosedException Closed(ushort replyCode, ShutdownInitiator initiator = ShutdownInitiator.Peer) => new(Shutdown(replyCode, initiator));

    private static AlreadyClosedException ConnectionLost() => Closed(Constants.ConnectionForced);

    private sealed class CapturingDeadLetterSink : IDeadLetterSink
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
}
