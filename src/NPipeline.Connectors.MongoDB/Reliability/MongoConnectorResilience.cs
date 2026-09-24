using MongoDB.Driver;
using NResilience;
using OurMongoWriteException = NPipeline.Connectors.MongoDB.Exceptions.MongoWriteException;

namespace NPipeline.Connectors.MongoDB.Reliability;

/// <summary>
///     Resilience presets for the MongoDB connector. Assign one to
///     <see cref="Configuration.MongoConfiguration.Resilience" /> (the sink) or
///     <see cref="ChangeStream.MongoChangeStreamConfiguration.Resilience" /> (opening a change stream), or derive your
///     own with a <c>with</c> expression.
/// </summary>
/// <remarks>
///     <para>
///         The driver's retryable writes and reads (<c>retryWrites</c> and <c>retryReads</c>, on by default) stay on.
///         They retry a single command once, immediately, and the server discards a retried write it already applied,
///         so they are the protocol's own exactly-once mechanism rather than a second retry policy. These presets
///         handle what they don't: longer outages, with backoff between attempts.
///     </para>
///     <para>
///         <see cref="NResilience.Resilience.AttemptTimeout" /> and <see cref="NResilience.Resilience.Deadline" /> are
///         infinite in these presets. A bulk write of a large batch can take a long time, and the driver already bounds
///         each command with its server selection and socket timeouts.
///     </para>
/// </remarks>
public static class MongoConnectorResilience
{
    // The server error codes the connector has always treated as transient.
    private static readonly HashSet<int> RetryableCommandCodes =
    [
        6, // HostUnreachable
        7, // HostNotFound
        89, // NetworkTimeout
        91, // ShutdownInProgress
        189, // PrimarySteppedDown
        262, // ExceededTimeLimit
        9001, // SocketException
        10107, // NotWritablePrimary
        11600, // InterruptedAtShutdown
        11602, // InterruptedDueToReplStateChange
        13435, // NotPrimaryNoSecondaryOk
        13436, // NotPrimaryOrSecondary
    ];

    /// <summary>
    ///     <see cref="NResilience.Classifier.Default" /> (a <see cref="TimeoutException" />, such as a server selection
    ///     timeout, is transient) plus MongoDB knowledge:
    ///     <list type="bullet">
    ///         <item>A bulk write that reported write errors is permanent: part of it may already be applied.</item>
    ///         <item>An error labelled <c>SystemOverloadedError</c> is throttled.</item>
    ///         <item>
    ///             An error labelled <c>RetryableWriteError</c>, <c>RetryableError</c>, or <c>ResumableChangeStreamError</c>, a
    ///             connection failure, a paused connection pool, and a command error with a network, shutdown, or
    ///             primary-stepped-down code are transient.
    ///         </item>
    ///         <item>
    ///             The connector's own <see cref="OurMongoWriteException" /> (a mapping failure or a partially applied
    ///             bulk write) and every other error are permanent.
    ///         </item>
    ///     </list>
    /// </summary>
    public static Classifier Classifier { get; } = Classifier.Default
        .On<OurMongoWriteException>(Verdict.Permanent)
        .On<MongoException>(Classify);

    /// <summary>
    ///     The sink preset: four attempts (three retries) with exponential backoff and full jitter from one second up
    ///     to 30 seconds. It replaces <c>MaxRetryAttempts = 3</c> and <c>RetryDelay = 1s</c>, whose loop made four
    ///     calls with delays of 1, 2, and 4 seconds.
    /// </summary>
    public static Resilience Default { get; } = new()
    {
        Name = "npipeline.mongodb.sink",
        Attempts = 4,
        AttemptTimeout = Timeout.InfiniteTimeSpan,
        Deadline = Timeout.InfiniteTimeSpan,
        Backoff = Backoff.Default with
        {
            TransientBase = TimeSpan.FromSeconds(1),
            MaximumDelay = TimeSpan.FromSeconds(30),
        },

        // Declared above so it is initialized first; a static initializer reads fields in declaration order.
        Classifier = Classifier,
        Adaptive = false,
    };

    /// <summary>
    ///     The change stream preset, applied to opening the stream: four attempts (three retries) with exponential
    ///     backoff and full jitter from two seconds up to 30 seconds. It replaces <c>MaxRetryAttempts = 3</c> and
    ///     <c>RetryDelay = 2s</c>, whose loop made four attempts with a fixed two-second delay.
    /// </summary>
    public static Resilience ChangeStream { get; } = Default with
    {
        Name = "npipeline.mongodb.changestream",
        Backoff = Default.Backoff with { TransientBase = TimeSpan.FromSeconds(2) },
    };

    private static Verdict Classify(MongoException exception)
    {
        // Some of the batch may already be written, so sending it again could duplicate documents.
        if (exception is MongoBulkWriteException)
            return Verdict.Permanent;

        if (exception.HasErrorLabel("SystemOverloadedError"))
            return Verdict.Throttled();

        if (exception.HasErrorLabel("RetryableWriteError") ||
            exception.HasErrorLabel("RetryableError") ||
            exception.HasErrorLabel("ResumableChangeStreamError"))
            return Verdict.Transient;

        return exception switch
        {
            MongoConnectionException or MongoConnectionPoolPausedException => Verdict.Transient,
            MongoCommandException command when RetryableCommandCodes.Contains(command.Code) => Verdict.Transient,
            _ => Verdict.Permanent,
        };
    }
}
