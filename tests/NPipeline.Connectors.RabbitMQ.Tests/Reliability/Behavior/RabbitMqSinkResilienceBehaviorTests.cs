using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using FakeItEasy;
using FakeItEasy.Core;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.RabbitMQ.Configuration;
using NPipeline.Connectors.RabbitMQ.Connection;
using NPipeline.Connectors.RabbitMQ.Metrics;
using NPipeline.Connectors.RabbitMQ.Nodes;
using NPipeline.Connectors.RabbitMQ.Reliability;
using NPipeline.Connectors.Serialization;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using NResilience;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace NPipeline.Connectors.RabbitMQ.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the RabbitMQ sink's publish resilience (Q1, Q2 in <c>plans/resilience-improvements.md</c>).
/// </summary>
public sealed class RabbitMqSinkResilienceBehaviorTests
{
    private static readonly Resilience FastRetries = RabbitMqConnectorResilience.Default with
    {
        Backoff = RabbitMqConnectorResilience.Default.Backoff with { TransientBase = TimeSpan.FromMilliseconds(1) },
    };

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
        new RabbitMqSinkOptions { ExchangeName = "orders" }.Resilience.Should().BeSameAs(preset);
    }

    [Fact]
    public async Task DefaultPreset_MakesFourAttempts_BeforeATransientFailureSurfaces()
    {
        var (channel, connectionManager) = CreateChannel();
        FailPublishes(channel, () => ConnectionLost());
        var metrics = A.Fake<IRabbitMqMetrics>();

        var sink = CreateSink(new RabbitMqSinkOptions { ExchangeName = "orders" }, connectionManager, metrics);

        var act = () => RunAsync(sink, "order-1");

        _ = await act.Should().ThrowAsync<AlreadyClosedException>();
        PublishCount(channel).Should().Be(4, "MaxRetries = 3 made one call and three retries");
        A.CallTo(() => metrics.RecordPublishError("orders", A<string>._)).MustHaveHappenedOnceExactly();
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

        var sink = CreateSink(new RabbitMqSinkOptions { ExchangeName = "missing", Resilience = FastRetries }, connectionManager);

        var act = () => RunAsync(sink, "order-1");

        _ = await act.Should().ThrowAsync<AlreadyClosedException>();
        PublishCount(channel).Should().Be(1, "publishing to an exchange that does not exist fails the same way every time");
    }

    [Fact]
    public async Task ClosedChannel_IsReplacedBeforeTheRetry()
    {
        var closed = A.Fake<IChannel>();
        A.CallTo(() => closed.IsOpen).ReturnsNextFromSequence(true, false);
        FailPublishes(closed, () => ConnectionLost());

        var fresh = A.Fake<IChannel>();
        A.CallTo(() => fresh.IsOpen).Returns(true);

        var connectionManager = A.Fake<IRabbitMqConnectionManager>();
        A.CallTo(() => connectionManager.GetPooledChannelAsync(A<bool>._, A<CancellationToken>._)).ReturnsNextFromSequence(closed, fresh);

        var sink = CreateSink(new RabbitMqSinkOptions { ExchangeName = "orders", Resilience = FastRetries }, connectionManager);

        await RunAsync(sink, "order-1");

        PublishCount(closed).Should().Be(1);
        PublishCount(fresh).Should().Be(1, "a closed channel never reopens, so the retry needs a new one");
        A.CallTo(() => connectionManager.ReturnChannel(closed)).MustHaveHappenedOnceExactly();
        A.CallTo(() => connectionManager.ReturnChannel(fresh)).MustHaveHappenedOnceExactly();
    }

    // Cancellation

    [Fact]
    public async Task PipelineCancellation_DuringBackoff_StopsWithoutRetrying()
    {
        var (channel, connectionManager) = CreateChannel();
        FailPublishes(channel, () => ConnectionLost());
        var metrics = A.Fake<IRabbitMqMetrics>();

        var slowRetries = RabbitMqConnectorResilience.Default with
        {
            Backoff = Backoff.Constant(TimeSpan.FromSeconds(30)),
        };

        var sink = CreateSink(new RabbitMqSinkOptions { ExchangeName = "orders", Resilience = slowRetries, ContinueOnError = true },
            connectionManager, metrics);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
        var act = () => RunAsync(sink, cts.Token, "order-1");

        _ = await act.Should().ThrowAsync<OperationCanceledException>("ContinueOnError must not swallow the pipeline's own cancellation");
        PublishCount(channel).Should().Be(1);
        A.CallTo(() => metrics.RecordPublishError(A<string>._, A<string>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task CancellationNotRequestedByThePipeline_IsAFailure()
    {
        var (channel, connectionManager) = CreateChannel();
        FailPublishes(channel, () => new OperationCanceledException("internal timeout"));
        var metrics = A.Fake<IRabbitMqMetrics>();

        var sink = CreateSink(new RabbitMqSinkOptions { ExchangeName = "orders", Resilience = FastRetries }, connectionManager, metrics);

        var act = () => RunAsync(sink, "order-1");

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        PublishCount(channel).Should().Be(1, "a cancellation the pipeline did not ask for is a permanent failure");
        A.CallTo(() => metrics.RecordPublishError("orders", A<string>._)).MustHaveHappenedOnceExactly();
    }

    // Idempotency

    [Fact]
    public async Task AcknowledgementFailure_AfterASuccessfulPublish_DoesNotRepublish()
    {
        var (channel, connectionManager) = CreateChannel();

        // The source message's acknowledgement fails after the publish to the exchange has succeeded.
        var sourceMessage = A.Fake<IAcknowledgableMessage>();
        A.CallTo(() => sourceMessage.Body).Returns("order-1");
        A.CallTo(() => sourceMessage.IsAcknowledged).Returns(false);
        A.CallTo(() => sourceMessage.AcknowledgeAsync(A<CancellationToken>._)).ThrowsAsync(new InvalidOperationException("ack failed"));

        var options = new RabbitMqSinkOptions { ExchangeName = "orders", Resilience = FastRetries };
        var sink = new RabbitMqSinkNode<IAcknowledgableMessage>(options, connectionManager, A.Fake<IMessageSerializer>());

        await using var input = new InMemoryDataStream<IAcknowledgableMessage>([sourceMessage]);

        try
        {
            await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
        }
        catch (InvalidOperationException)
        {
            // Whether the acknowledgement failure surfaces is not what this test is about.
        }

        PublishCount(channel).Should().Be(1, "the message already reached the exchange; publishing it again duplicates it");
    }

    [Fact]
    public async Task FailedPublish_DoesNotAcknowledgeTheSourceMessage()
    {
        var (channel, connectionManager) = CreateChannel();
        FailPublishes(channel, () => Closed(Constants.AccessRefused));

        var sourceMessage = A.Fake<IAcknowledgableMessage>();
        A.CallTo(() => sourceMessage.Body).Returns("order-1");

        var options = new RabbitMqSinkOptions { ExchangeName = "orders", Resilience = FastRetries, ContinueOnError = true };
        var sink = new RabbitMqSinkNode<IAcknowledgableMessage>(options, connectionManager, A.Fake<IMessageSerializer>());

        await using var input = new InMemoryDataStream<IAcknowledgableMessage>([sourceMessage]);
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);

        A.CallTo(() => sourceMessage.AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task Retries_CarryTheSameMessageId()
    {
        var (channel, connectionManager) = CreateChannel();
        FailPublishes(channel, () => ConnectionLost(), 2);

        var sink = CreateSink(new RabbitMqSinkOptions { ExchangeName = "orders", Resilience = FastRetries }, connectionManager);

        await RunAsync(sink, "order-1");

        var messageIds = PublishCalls(channel).Select(call => ((BasicProperties)call.Arguments[3]!).MessageId).ToList();
        messageIds.Should().HaveCount(3);
        messageIds.Distinct().Should().ContainSingle("a consumer can only discard a duplicate that carries the original's ID");
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

        var options = new RabbitMqSinkOptions
        {
            ExchangeName = "orders",
            RoutingKeySelector = item => (string)item,
            Resilience = FastRetries,
            Batching = new BatchPublishOptions { BatchSize = 2, LingerTime = TimeSpan.FromMinutes(1) },
        };

        var sink = CreateSink(options, connectionManager);

        await RunAsync(sink, "order-1", "order-2");

        bodies.Should().Equal("order-1", "order-2", "order-2");
    }

    // Publisher confirms

    [Fact]
    public async Task ConfirmThatNeverArrives_FailsEachAttemptAsATimeout_AndIsRetried()
    {
        var (channel, connectionManager) = CreateChannel();
        WaitForeverForConfirm(channel);

        var options = new RabbitMqSinkOptions
        {
            ExchangeName = "orders",
            ConfirmTimeout = TimeSpan.FromMilliseconds(50),
            Resilience = FastRetries,
        };

        var sink = CreateSink(options, connectionManager);

        var act = () => RunAsync(sink, "order-1");

        _ = await act.Should().ThrowAsync<TimeoutException>();
        PublishCount(channel).Should().Be(4, "a missing confirm is transient, so every attempt is spent");
    }

    [Fact]
    public async Task ConfirmTimeout_ThenAConfirmedRetry_Succeeds()
    {
        var (channel, connectionManager) = CreateChannel();
        WaitForeverForConfirm(channel, 1);

        var sourceMessage = A.Fake<IAcknowledgableMessage>();
        A.CallTo(() => sourceMessage.Body).Returns("order-1");

        var options = new RabbitMqSinkOptions
        {
            ExchangeName = "orders",
            ConfirmTimeout = TimeSpan.FromMilliseconds(50),
            Resilience = FastRetries,
        };

        var sink = new RabbitMqSinkNode<IAcknowledgableMessage>(options, connectionManager, A.Fake<IMessageSerializer>());
        await using var input = new InMemoryDataStream<IAcknowledgableMessage>([sourceMessage]);
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);

        PublishCount(channel).Should().Be(2);
        A.CallTo(() => sourceMessage.AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    // Batching

    [Fact]
    public async Task LingerFlush_RacingSizeFlushes_NeverOverlaps_AndPublishesEachMessageOnce()
    {
        var (channel, connectionManager) = CreateChannel();
        var published = new ConcurrentBag<string>();
        var active = 0;
        var overlapped = false;

        A.CallTo(channel)
            .Where(call => call.Method.Name == nameof(IChannel.BasicPublishAsync))
            .WithReturnType<ValueTask>()
            .ReturnsLazily(call => PublishSlowlyAsync((string)call.Arguments[1]!));

        async ValueTask PublishSlowlyAsync(string routingKey)
        {
            if (Interlocked.Increment(ref active) > 1)
                overlapped = true;

            await Task.Delay(1).ConfigureAwait(false);
            published.Add(routingKey);
            _ = Interlocked.Decrement(ref active);
        }

        var options = new RabbitMqSinkOptions
        {
            ExchangeName = "orders",
            RoutingKeySelector = item => (string)item,
            Resilience = FastRetries,

            // The linger timer fires constantly, racing the size-triggered flushes of the main loop.
            Batching = new BatchPublishOptions { BatchSize = 3, LingerTime = TimeSpan.FromMilliseconds(1) },
        };

        var sink = CreateSink(options, connectionManager);
        var items = Enumerable.Range(1, 200).Select(i => $"order-{i}").ToList();

        await using var input = new DataStream<string>(TrickleAsync(items), "trickle");
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);

        overlapped.Should().BeFalse("two flushes must never publish at the same time");
        published.Should().BeEquivalentTo(items, "no message may be lost or published twice");
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BatchFailure_AcknowledgesTheMessagesPublishedBeforeIt(bool continueOnError)
    {
        var (channel, connectionManager) = CreateChannel();

        A.CallTo(channel)
            .Where(call => call.Method.Name == nameof(IChannel.BasicPublishAsync))
            .WithReturnType<ValueTask>()
            .ReturnsLazily(call => (string)call.Arguments[1]! == "order-2"
                ? ValueTask.FromException(Closed(Constants.AccessRefused))
                : ValueTask.CompletedTask);

        var messages = Enumerable.Range(1, 3).Select(i =>
        {
            var message = A.Fake<IAcknowledgableMessage>();
            A.CallTo(() => message.Body).Returns($"order-{i}");
            return message;
        }).ToList();

        var options = new RabbitMqSinkOptions
        {
            ExchangeName = "orders",
            RoutingKeySelector = item => (string)item,
            Resilience = FastRetries,
            ContinueOnError = continueOnError,
            Batching = new BatchPublishOptions { BatchSize = 3, LingerTime = TimeSpan.FromMinutes(1) },
        };

        var sink = new RabbitMqSinkNode<IAcknowledgableMessage>(options, connectionManager, A.Fake<IMessageSerializer>());
        await using var input = new InMemoryDataStream<IAcknowledgableMessage>(messages);

        var act = () => sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);

        if (continueOnError)
            await act.Should().NotThrowAsync();
        else
            _ = await act.Should().ThrowAsync<AlreadyClosedException>();

        // order-1 reached the exchange; leaving it unacknowledged would redeliver and republish it.
        A.CallTo(() => messages[0].AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => messages[1].AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => messages[2].AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
        PublishCalls(channel).Select(call => (string)call.Arguments[1]!).Should().Equal("order-1", "order-2");
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

        var options = new RabbitMqSinkOptions
        {
            ExchangeName = "orders",
            EnablePublisherConfirms = false,
            ConfirmTimeout = TimeSpan.FromMilliseconds(20),
            Resilience = FastRetries,
        };

        await RunAsync(CreateSink(options, connectionManager), "order-1");

        PublishCount(channel).Should().Be(1);
        A.CallTo(() => connectionManager.GetPooledChannelAsync(false, A<CancellationToken>._)).MustHaveHappened();
        A.CallTo(() => connectionManager.GetPooledChannelAsync(true, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task ConfirmsOn_UsesAChannelWithConfirms()
    {
        var (_, connectionManager) = CreateChannel();

        await RunAsync(CreateSink(new RabbitMqSinkOptions { ExchangeName = "orders", Resilience = FastRetries }, connectionManager), "order-1");

        A.CallTo(() => connectionManager.GetPooledChannelAsync(true, A<CancellationToken>._)).MustHaveHappened();
        A.CallTo(() => connectionManager.GetPooledChannelAsync(false, A<CancellationToken>._)).MustNotHaveHappened();
    }

    // Shutdown

    [Fact]
    public async Task PipelineCancellation_PublishesAndAcknowledgesWhatIsAlreadyBatched()
    {
        var (channel, connectionManager) = CreateChannel();
        var messages = Enumerable.Range(1, 3).Select(i => AcknowledgableMessage($"order-{i}")).ToList();
        using var cts = new CancellationTokenSource();

        var sink = new RabbitMqSinkNode<IAcknowledgableMessage>(BatchedOptions(), connectionManager, A.Fake<IMessageSerializer>());
        await using var input = new DataStream<IAcknowledgableMessage>(ThenCancelAsync(messages, cts), "then-cancel");

        var act = () => sink.ConsumeAsync(input, new PipelineContext(), cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        PublishCalls(channel).Select(call => (string)call.Arguments[1]!).Should().Equal("order-1", "order-2", "order-3");

        foreach (var message in messages)
        {
            A.CallTo(() => message.AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        }
    }

    [Fact]
    public async Task ShutdownFlush_IsBoundedByShutdownFlushTimeout()
    {
        var (channel, connectionManager) = CreateChannel();
        WaitForeverForConfirm(channel);

        var messages = Enumerable.Range(1, 2).Select(i => AcknowledgableMessage($"order-{i}")).ToList();
        using var cts = new CancellationTokenSource();

        var options = BatchedOptions() with
        {
            ConfirmTimeout = TimeSpan.FromMinutes(5),
            ShutdownFlushTimeout = TimeSpan.FromMilliseconds(100),
        };

        var sink = new RabbitMqSinkNode<IAcknowledgableMessage>(options, connectionManager, A.Fake<IMessageSerializer>());
        await using var input = new DataStream<IAcknowledgableMessage>(ThenCancelAsync(messages, cts), "then-cancel");
        var started = DateTime.UtcNow;

        var act = () => sink.ConsumeAsync(input, new PipelineContext(), cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        (DateTime.UtcNow - started).Should().BeLessThan(TimeSpan.FromSeconds(5));

        foreach (var message in messages)
        {
            A.CallTo(() => message.AcknowledgeAsync(A<CancellationToken>._)).MustNotHaveHappened();
        }
    }

    [Fact]
    public async Task CancellationDuringABatchFlush_DoesNotRepublishWhatWasAlreadyPublished()
    {
        var (channel, connectionManager) = CreateChannel();
        using var cts = new CancellationTokenSource();
        var blocked = false;

        // order-2's first publish hangs until the pipeline is cancelled mid-flush.
        A.CallTo(channel)
            .Where(call => call.Method.Name == nameof(IChannel.BasicPublishAsync))
            .WithReturnType<ValueTask>()
            .ReturnsLazily(call =>
            {
                if ((string)call.Arguments[1]! != "order-2" || blocked)
                    return ValueTask.CompletedTask;

                blocked = true;
                cts.Cancel();
                return new ValueTask(Task.Delay(Timeout.Infinite, (CancellationToken)call.Arguments[5]!));
            });

        var messages = Enumerable.Range(1, 3).Select(i => AcknowledgableMessage($"order-{i}")).ToList();
        var options = BatchedOptions() with { Batching = new BatchPublishOptions { BatchSize = 3, LingerTime = TimeSpan.FromMinutes(1) } };

        var sink = new RabbitMqSinkNode<IAcknowledgableMessage>(options, connectionManager, A.Fake<IMessageSerializer>());
        await using var input = new InMemoryDataStream<IAcknowledgableMessage>(messages);

        var act = () => sink.ConsumeAsync(input, new PipelineContext(), cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        PublishCalls(channel).Select(call => (string)call.Arguments[1]!).Should().Equal("order-1", "order-2", "order-2", "order-3");

        foreach (var message in messages)
        {
            A.CallTo(() => message.AcknowledgeAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        }
    }

    private static RabbitMqSinkOptions BatchedOptions()
    {
        return new RabbitMqSinkOptions
        {
            ExchangeName = "orders",
            RoutingKeySelector = item => (string)item,
            Resilience = FastRetries,
            Batching = new BatchPublishOptions { BatchSize = 100, LingerTime = TimeSpan.FromMinutes(1) },
        };
    }

    private static IAcknowledgableMessage AcknowledgableMessage(string body)
    {
        var message = A.Fake<IAcknowledgableMessage>();
        A.CallTo(() => message.Body).Returns(body);
        return message;
    }

    /// <summary>Yields the items, then cancels the pipeline and waits, as an input with nothing more to give would.</summary>
    private static async IAsyncEnumerable<IAcknowledgableMessage> ThenCancelAsync(
        IEnumerable<IAcknowledgableMessage> items,
        CancellationTokenSource cts,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        foreach (var item in items)
        {
            yield return item;
        }

        cts.CancelAfter(TimeSpan.FromMilliseconds(50));
        await Task.Delay(Timeout.Infinite, cancellationToken).ConfigureAwait(false);
    }

    private static async IAsyncEnumerable<string> TrickleAsync(IEnumerable<string> items)
    {
        var count = 0;

        foreach (var item in items)
        {
            if (++count % 5 == 0)
                await Task.Delay(1).ConfigureAwait(false);

            yield return item;
        }
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

    private static RabbitMqSinkNode<string> CreateSink(RabbitMqSinkOptions options, IRabbitMqConnectionManager connectionManager,
        IRabbitMqMetrics? metrics = null) =>
        new(options, connectionManager, A.Fake<IMessageSerializer>(), metrics);

    private static Task RunAsync(RabbitMqSinkNode<string> sink, params string[] items) => RunAsync(sink, CancellationToken.None, items);

    private static async Task RunAsync(RabbitMqSinkNode<string> sink, CancellationToken cancellationToken, params string[] items)
    {
        await using var input = new InMemoryDataStream<string>(items);
        await sink.ConsumeAsync(input, new PipelineContext(), cancellationToken);
    }

    private static ShutdownEventArgs Shutdown(ushort replyCode, ShutdownInitiator initiator) =>
        new(initiator, replyCode, "closed", (object?)null, CancellationToken.None);

    private static AlreadyClosedException Closed(ushort replyCode, ShutdownInitiator initiator = ShutdownInitiator.Peer) => new(Shutdown(replyCode, initiator));

    private static AlreadyClosedException ConnectionLost() => Closed(Constants.ConnectionForced);
}
