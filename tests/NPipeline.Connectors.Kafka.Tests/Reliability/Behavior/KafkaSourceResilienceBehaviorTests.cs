using Confluent.Kafka;
using FakeItEasy;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Metrics;
using NPipeline.Connectors.Kafka.Nodes;
using NPipeline.Connectors.Kafka.Reliability;
using NPipeline.Pipeline;
using NResilience;

namespace NPipeline.Connectors.Kafka.Tests.Reliability.Behavior;

/// <summary>
///     Behavior tests for the Kafka source's consume resilience (K1-K5 in <c>plans/resilience-improvements.md</c>).
/// </summary>
public sealed class KafkaSourceResilienceBehaviorTests
{
    private static readonly NResilience.Resilience FastRetries = KafkaConnectorResilience.Default with
    {
        Backoff = KafkaConnectorResilience.Default.Backoff with
        {
            TransientBase = TimeSpan.FromMilliseconds(1),
            ThrottledBase = TimeSpan.FromMilliseconds(1),
        },
    };

    // Preset

    [Fact]
    public void DefaultPreset_MakesTheDocumentedThreeRetries()
    {
        var preset = KafkaConnectorResilience.Default;

        // MaxRetries = 3 documented three retries (K5: the old check made two). BaseDelayMs = 100, MaxDelayMs = 30000.
        preset.Attempts.Should().Be(4);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromMilliseconds(100));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(30));
        preset.Backoff.Jitter.Should().Be(Jitter.Full);
        preset.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Adaptive.Should().BeFalse();
        preset.Classifier.Should().BeSameAs(KafkaConnectorResilience.Classifier);
        new KafkaConfiguration().Resilience.Should().BeSameAs(preset);
    }

    [Fact]
    public async Task DefaultPreset_ConsumesFourTimes_BeforeAPersistentRetriableErrorSurfaces()
    {
        var consumer = A.Fake<IConsumer<string, string>>();
        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).Throws(() => ConsumeError(ErrorCode.Local_Transport));
        var metrics = A.Fake<IKafkaMetrics>();

        var node = CreateNode(consumer, KafkaConnectorResilience.Default, metrics);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var act = () => DrainAsync(node, cts.Token);

        _ = await act.Should().ThrowAsync<ConsumeException>("a consume error that never clears must fail the stream once retries run out");
        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).MustHaveHappened(4, Times.Exactly);

        // K4: consume errors are consume errors, not commit errors.
        A.CallTo(() => metrics.RecordConsumeError("orders", A<Exception>._)).MustHaveHappened(4, Times.Exactly);
        A.CallTo(() => metrics.RecordCommitError(A<string>._, A<Exception>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task ErrorThatClears_RestartsTheAttemptCount()
    {
        // Three failures and a message, twice: each consume stays within its four attempts.
        var consumer = Script(
            () => throw ConsumeError(ErrorCode.Local_Transport),
            () => throw ConsumeError(ErrorCode.Local_Transport),
            () => throw ConsumeError(ErrorCode.Local_Transport),
            () => Result("order-1", 1),
            () => throw ConsumeError(ErrorCode.Local_Transport),
            () => throw ConsumeError(ErrorCode.Local_Transport),
            () => throw ConsumeError(ErrorCode.Local_Transport),
            () => Result("order-2", 2));

        var node = CreateNode(consumer, FastRetries);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        var bodies = await DrainAsync(node, cts.Token);

        bodies.Should().Equal("order-1", "order-2");
    }

    [Fact]
    public async Task MessagesConsumedBeforeAFailure_AreDeliveredFirst()
    {
        var consumer = Script(
            () => Result("order-1", 1),
            () => throw ConsumeError(ErrorCode.TopicAuthorizationFailed));

        var node = CreateNode(consumer, FastRetries);
        var bodies = new List<string>();

        var act = async () =>
        {
            await foreach (var message in node.OpenStream(new PipelineContext(), CancellationToken.None))
            {
                bodies.Add(message.Body);
            }
        };

        _ = await act.Should().ThrowAsync<ConsumeException>();
        bodies.Should().Equal("order-1");
    }

    // Classifier

    public static TheoryData<Exception, VerdictKind> ClassifiedExceptions => new()
    {
        { ConsumeError(ErrorCode.Local_Transport), VerdictKind.Transient },
        { ConsumeError(ErrorCode.Local_AllBrokersDown), VerdictKind.Transient },
        { ConsumeError(ErrorCode.Local_TimedOut), VerdictKind.Transient },
        { ConsumeError(ErrorCode.NotLeaderForPartition), VerdictKind.Transient },
        { ConsumeError(ErrorCode.GroupCoordinatorNotAvailable), VerdictKind.Transient },
        { ConsumeError(ErrorCode.Local_MaxPollExceeded), VerdictKind.Transient },
        { new KafkaRetriableException(new Error(ErrorCode.Local_Fail)), VerdictKind.Transient },
        { new KafkaException(ErrorCode.NetworkException), VerdictKind.Transient },
        { ConsumeError(ErrorCode.ThrottlingQuotaExceeded), VerdictKind.Throttled },
        { ProduceError(ErrorCode.Local_QueueFull), VerdictKind.Throttled },
        { ConsumeError(new Error(ErrorCode.Local_Transport, "fenced", true)), VerdictKind.Permanent },
        { ConsumeError(new Error(ErrorCode.Local_Fatal, "fatal", true)), VerdictKind.Permanent },
        { ConsumeError(ErrorCode.Local_ValueDeserialization), VerdictKind.Permanent },
        { ConsumeError(ErrorCode.Local_KeyDeserialization), VerdictKind.Permanent },
        { ConsumeError(ErrorCode.TopicAuthorizationFailed), VerdictKind.Permanent },
        { ConsumeError(ErrorCode.GroupAuthorizationFailed), VerdictKind.Permanent },
        { ConsumeError(ErrorCode.OffsetOutOfRange), VerdictKind.Permanent },
        { ConsumeError(ErrorCode.Local_UnknownPartition), VerdictKind.Permanent },
        { new KafkaTxnRequiresAbortException(new Error(ErrorCode.Local_Transport)), VerdictKind.Permanent },
        { ProduceError(ErrorCode.Local_MsgTimedOut), VerdictKind.Permanent },
        { ProduceError(ErrorCode.MsgSizeTooLarge), VerdictKind.Permanent },
        { new InvalidOperationException("bug"), VerdictKind.Permanent },
    };

    [Theory]
    [MemberData(nameof(ClassifiedExceptions))]
    public void Classifier_JudgesKafkaErrors(Exception exception, VerdictKind expected)
    {
        KafkaConnectorResilience.Classifier.ClassifyException(exception).Kind.Should().Be(expected);
    }

    [Theory]
    [InlineData(ErrorCode.Local_ValueDeserialization)]
    [InlineData(ErrorCode.TopicAuthorizationFailed)]
    public async Task NonRetriableConsumeError_SurfacesOnTheFirstAttempt(ErrorCode code)
    {
        var consumer = A.Fake<IConsumer<string, string>>();
        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).Throws(() => ConsumeError(code));

        var node = CreateNode(consumer, FastRetries);

        var act = () => DrainAsync(node, CancellationToken.None);

        _ = await act.Should().ThrowAsync<ConsumeException>();

        // A retried consume moves past a message that failed to deserialize, dropping it silently.
        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task FatalConsumeError_SurfacesOnTheFirstAttempt()
    {
        var consumer = A.Fake<IConsumer<string, string>>();
        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).Throws(() => ConsumeError(new Error(ErrorCode.Local_Fatal, "fenced", true)));

        var node = CreateNode(consumer, FastRetries);

        var act = () => DrainAsync(node, CancellationToken.None);

        _ = await act.Should().ThrowAsync<ConsumeException>();
        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).MustHaveHappenedOnceExactly();
    }

    // Cancellation

    [Fact]
    public async Task CancellationNotRequestedByThePipeline_FailsTheStream()
    {
        // An internal timeout surfacing as OperationCanceledException is a failure, not a request to shut down.
        var consumer = A.Fake<IConsumer<string, string>>();
        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).Throws(() => new OperationCanceledException("internal timeout"));

        var node = CreateNode(consumer, FastRetries);

        var act = () => DrainAsync(node, CancellationToken.None);

        _ = await act.Should().ThrowAsync<OperationCanceledException>("the stream must not end as if it had succeeded");
        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task PipelineCancellation_EndsTheStreamGracefully()
    {
        var consumer = A.Fake<IConsumer<string, string>>();
        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).Returns(null!);

        var node = CreateNode(consumer, FastRetries);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));

        var act = () => DrainAsync(node, cts.Token);

        await act.Should().NotThrowAsync("shutting the pipeline down closes the consumer and ends the stream");
    }

    [Fact]
    public async Task PipelineCancellation_DuringBackoff_EndsTheStreamWithoutRetrying()
    {
        var consumer = A.Fake<IConsumer<string, string>>();
        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).Throws(() => ConsumeError(ErrorCode.Local_Transport));

        var slowRetries = KafkaConnectorResilience.Default with { Backoff = Backoff.Constant(TimeSpan.FromSeconds(30)) };
        var node = CreateNode(consumer, slowRetries);
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        var act = () => DrainAsync(node, cts.Token);

        await act.Should().NotThrowAsync("the pipeline asked the source to stop");
        A.CallTo(() => consumer.Consume(A<TimeSpan>._)).MustHaveHappenedOnceExactly();
    }

    private static IConsumer<string, string> Script(params Func<ConsumeResult<string, string>?>[] steps)
    {
        var consumer = A.Fake<IConsumer<string, string>>();
        var next = 0;

        A.CallTo(() => consumer.Consume(A<TimeSpan>._))
            .ReturnsLazily(() => next < steps.Length ? steps[next++]() : null);

        return consumer;
    }

    private static ConsumeResult<string, string> Result(string value, long offset)
    {
        return new ConsumeResult<string, string>
        {
            TopicPartitionOffset = new TopicPartitionOffset("orders", 0, offset),
            Message = new Message<string, string> { Key = value, Value = value, Timestamp = Timestamp.Default },
        };
    }

    private static ConsumeException ConsumeError(ErrorCode code)
    {
        return ConsumeError(new Error(code));
    }

    private static ConsumeException ConsumeError(Error error)
    {
        return new ConsumeException(new ConsumeResult<byte[], byte[]>(), error);
    }

    private static ProduceException<string, string> ProduceError(ErrorCode code)
    {
        return new ProduceException<string, string>(new Error(code), new DeliveryResult<string, string>());
    }

    private static KafkaSourceNode<string> CreateNode(
        IConsumer<string, string> consumer,
        NResilience.Resilience resilience,
        IKafkaMetrics? metrics = null)
    {
        var configuration = new KafkaConfiguration
        {
            BootstrapServers = "localhost:9092",
            SourceTopic = "orders",
            ConsumerGroupId = "reliability-tests",
            PollTimeoutMs = 1,
            Resilience = resilience,
        };

        return new KafkaSourceNode<string>(consumer, configuration, metrics ?? NullKafkaMetrics.Instance);
    }

    private static async Task<List<string>> DrainAsync(KafkaSourceNode<string> node, CancellationToken cancellationToken)
    {
        var bodies = new List<string>();

        await foreach (var message in node.OpenStream(new PipelineContext(), cancellationToken).WithCancellation(cancellationToken))
        {
            bodies.Add(message.Body);
        }

        return bodies;
    }
}
