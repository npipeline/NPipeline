using Confluent.Kafka;
using NPipeline.Connectors.Kafka.Configuration;
using NPipeline.Connectors.Kafka.Reliability;
using NResilience;

namespace NPipeline.Connectors.Kafka.Tests.Reliability;

/// <summary>
///     Tests for <see cref="KafkaConnectorResilience" />: the classifier that judges Kafka errors and the source's default preset.
/// </summary>
public sealed class KafkaConnectorResilienceTests
{
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
    [MemberData(nameof(ClassifiedExceptions), DisableDiscoveryEnumeration = true)]
    public void Classifier_JudgesKafkaErrors(Exception exception, VerdictKind expected)
    {
        KafkaConnectorResilience.Classifier.ClassifyException(exception).Kind.Should().Be(expected);
    }

    [Theory]
    [InlineData(ErrorCode.Local_Transport, true)]
    [InlineData(ErrorCode.Local_AllBrokersDown, true)]
    [InlineData(ErrorCode.TopicAuthorizationFailed, false)]
    [InlineData(ErrorCode.Local_ValueDeserialization, false)]
    public void IsRetriable_MatchesTheClassifier(ErrorCode code, bool retriable)
    {
        KafkaConnectorResilience.IsRetriable(new Error(code)).Should().Be(retriable);
    }

    [Fact]
    public void FatalErrors_AreNeverRetriable()
    {
        KafkaConnectorResilience.IsRetriable(new Error(ErrorCode.Local_Transport, "fenced", true)).Should().BeFalse();
    }

    [Fact]
    public void DefaultPreset_MakesTheDocumentedThreeRetries()
    {
        var preset = KafkaConnectorResilience.Default;

        preset.Attempts.Should().Be(4);
        preset.Backoff.TransientBase.Should().Be(TimeSpan.FromMilliseconds(100));
        preset.Backoff.MaximumDelay.Should().Be(TimeSpan.FromSeconds(30));
        preset.Backoff.Jitter.Should().Be(Jitter.Full);
        preset.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        preset.Adaptive.Should().BeFalse();
        preset.Classifier.Should().BeSameAs(KafkaConnectorResilience.Classifier);

        new KafkaReadOptions { BootstrapServers = "localhost:9092", Topic = "orders", GroupId = "billing" }.Resilience.Should().BeSameAs(preset);
    }

    [Fact]
    public void DefaultPreset_IsValid()
    {
        var act = () => KafkaConnectorResilience.Default.Validate();

        act.Should().NotThrow();
    }

    private static ConsumeException ConsumeError(ErrorCode code) => ConsumeError(new Error(code));

    private static ConsumeException ConsumeError(Error error) => new(new ConsumeResult<byte[], byte[]>(), error);

    private static ProduceException<string, string> ProduceError(ErrorCode code) => new(new Error(code), new DeliveryResult<string, string>());
}
