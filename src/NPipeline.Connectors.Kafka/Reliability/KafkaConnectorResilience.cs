using Confluent.Kafka;
using NResilience;

namespace NPipeline.Connectors.Kafka.Reliability;

/// <summary>
///     Resilience presets for <see cref="Nodes.KafkaSourceNode{T}" />. Assign one to
///     <see cref="Configuration.KafkaConfiguration.Resilience" />, or derive your own with a <c>with</c> expression.
/// </summary>
/// <remarks>
///     <para>
///         The source runs each consume (poll) through the policy, so the attempt count applies to one consume call:
///         a retriable error that clears restarts the count for the next call, and one that persists surfaces once the
///         attempts are spent.
///     </para>
///     <para>
///         The sink does not retry. librdkafka retries every produce until <c>delivery.timeout.ms</c>, and the
///         idempotent producer (<c>EnableIdempotence</c>, on by default) removes the duplicates its retries would
///         cause. A retry above librdkafka would send a new message the idempotent producer cannot recognize, so it
///         could duplicate a message whose delivery timed out but still reached the broker.
///     </para>
/// </remarks>
public static class KafkaConnectorResilience
{
    /// <summary>
    ///     <see cref="NResilience.Classifier.Default" /> (timeouts and I/O errors are transient), plus a rule for every
    ///     <see cref="KafkaException" />, judged by its <see cref="Error" />:
    ///     <list type="bullet">
    ///         <item>A fatal error (<see cref="Error.IsFatal" />) is permanent: the client cannot recover from it.</item>
    ///         <item>
    ///             Quota throttling (<see cref="ErrorCode.ThrottlingQuotaExceeded" />) and a full producer queue
    ///             (<see cref="ErrorCode.Local_QueueFull" />) are throttled, and take the long backoff curve.
    ///         </item>
    ///         <item>
    ///             A <see cref="KafkaRetriableException" />, and an error code Kafka documents as retriable (a lost
    ///             broker connection, a timeout, a leader or coordinator moving, a rebalance, too few in-sync replicas),
    ///             is transient.
    ///         </item>
    ///         <item>
    ///             Anything else is permanent, including deserialization errors, authorization failures, and a
    ///             partition the client does not know (<see cref="ErrorCode.Local_UnknownPartition" />).
    ///         </item>
    ///     </list>
    /// </summary>
    /// <remarks>
    ///     Confluent.Kafka's <see cref="Error" /> has no retriable flag (only <see cref="Error.IsFatal" />), so the
    ///     retriable codes are listed here, following the Kafka protocol's <c>RetriableException</c> set and
    ///     librdkafka's transient local errors.
    /// </remarks>
    public static Classifier Classifier { get; } = NResilience.Classifier.Default
        .On<KafkaException>(static e => Classify(e));

    /// <summary>
    ///     Four attempts (three retries) per consume, with exponential backoff and full jitter from 100 milliseconds up
    ///     to 30 seconds. There is no attempt timeout or overall deadline: a consume is a blocking poll that the
    ///     connector's <c>PollTimeoutMs</c> already bounds, and the attempt count bounds the call.
    /// </summary>
    /// <remarks>
    ///     Replaces <c>ExponentialBackoffRetryStrategy</c> (<c>MaxRetries = 3</c>, <c>BaseDelayMs = 100</c>,
    ///     <c>MaxDelayMs = 30000</c>). Its <c>attempt &gt;= MaxRetries</c> check made only three calls (two retries),
    ///     one fewer than <c>MaxRetries</c> documented; this preset makes the documented three retries. It also retried
    ///     every exception, where this preset retries only retriable Kafka errors.
    /// </remarks>
    public static NResilience.Resilience Default { get; } = new()
    {
        Name = "npipeline.kafka.consume",
        Attempts = 4,
        AttemptTimeout = Timeout.InfiniteTimeSpan,
        Deadline = Timeout.InfiniteTimeSpan,
        Backoff = Backoff.Default with
        {
            TransientBase = TimeSpan.FromMilliseconds(100),
            MaximumDelay = TimeSpan.FromSeconds(30),
        },
        // Declared above so it is initialized first; a static initializer reads fields in declaration order.
        Classifier = Classifier,
        Adaptive = false,
    };

    /// <summary>
    ///     Whether Kafka reports <paramref name="error" /> as retriable: the same request can succeed later without
    ///     any change on the client's side.
    /// </summary>
    /// <param name="error">The error to judge.</param>
    /// <returns><see langword="true" /> for a retriable, non-fatal error.</returns>
    public static bool IsRetriable(Error error)
    {
        ArgumentNullException.ThrowIfNull(error);

        return !error.IsFatal && error.Code switch
        {
            // Broker errors the Kafka protocol marks retriable.
            ErrorCode.UnknownTopicOrPart
                or ErrorCode.LeaderNotAvailable
                or ErrorCode.NotLeaderForPartition
                or ErrorCode.RequestTimedOut
                or ErrorCode.BrokerNotAvailable
                or ErrorCode.ReplicaNotAvailable
                or ErrorCode.NetworkException
                or ErrorCode.GroupLoadInProgress
                or ErrorCode.GroupCoordinatorNotAvailable
                or ErrorCode.NotCoordinatorForGroup
                or ErrorCode.NotEnoughReplicas
                or ErrorCode.NotEnoughReplicasAfterAppend
                or ErrorCode.RebalanceInProgress
                or ErrorCode.NotController
                or ErrorCode.KafkaStorageError
                or ErrorCode.FetchSessionIdNotFound
                or ErrorCode.InvalidFetchSessionEpoch
                or ErrorCode.FencedLeaderEpoch
                or ErrorCode.UnknownLeaderEpoch
                or ErrorCode.OffsetNotAvailable
                or ErrorCode.PreferredLeaderNotAvailable
                or ErrorCode.UnstableOffsetCommit

                // librdkafka's transient local errors.
                or ErrorCode.Local_Transport
                or ErrorCode.Local_AllBrokersDown
                or ErrorCode.Local_Resolve
                or ErrorCode.Local_TimedOut
                or ErrorCode.Local_TimedOutQueue
                or ErrorCode.Local_WaitCoord
                or ErrorCode.Local_Retry
                or ErrorCode.Local_MaxPollExceeded => true,
            _ => false,
        };
    }

    private static Verdict Classify(KafkaException exception)
    {
        var error = exception.Error;

        if (error.IsFatal || exception is KafkaTxnRequiresAbortException)
            return Verdict.Permanent;

        if (error.Code is ErrorCode.ThrottlingQuotaExceeded or ErrorCode.Local_QueueFull)
            return Verdict.Throttled();

        return exception is KafkaRetriableException || IsRetriable(error)
            ? Verdict.Transient
            : Verdict.Permanent;
    }
}
