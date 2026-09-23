using System.Data.Common;
using NPipeline.Connectors.Snowflake.Exceptions;
using NResilience;

namespace NPipeline.Connectors.Snowflake.Reliability;

/// <summary>
///     Resilience presets for the Snowflake connector. Assign one to
///     <see cref="Configuration.SnowflakeConfiguration.Resilience" />, or derive your own with a <c>with</c> expression.
/// </summary>
/// <remarks>
///     <para>
///         The sink retries each unit of work a writer owns: one row for the per-row writer and one statement for the
///         batch writer, each of which commits all or nothing. The staged copy writer retries its two steps separately: the
///         <c>PUT</c> of the staged file, which writes nothing to the table, and then the <c>COPY INTO</c> of that same file.
///         Snowflake's load metadata skips a file it has already loaded, so a <c>COPY INTO</c> retried after a failure that
///         hid its success loads nothing twice. Inside a transaction the writer does not own
///         (<see cref="NPipeline.Connectors.Configuration.DeliverySemantic.ExactlyOnce" />), the writer makes one attempt and
///         leaves the failure to the transaction's owner.
///     </para>
///     <para>
///         Exactly one layer retries each kind of failure. The Snowflake.Data driver retries every HTTP request of a
///         statement (transport errors, HTTP timeouts, and 5xx, 403, 408, and 429 responses) under its own
///         <c>MAXHTTPRETRIES</c> and <c>RETRY_TIMEOUT</c> settings, including the polling for a running query's result,
///         which the connector could only repeat by running the statement again. The connector therefore treats what the
///         driver reports after those retries as permanent, and retries only errors the server returns for the statement
///         itself.
///     </para>
///     <para>
///         The presets have no attempt timeout and no deadline. Each attempt is bounded by
///         <see cref="Configuration.SnowflakeConfiguration.CommandTimeout" /> instead, which suits a <c>COPY INTO</c> that
///         runs for minutes. A timeout set on the policy applies to every write strategy, staged copy included.
///     </para>
/// </remarks>
public static class SnowflakeConnectorResilience
{
    /// <summary>
    ///     <see cref="NResilience.Classifier.Default" />, plus <see cref="SnowflakeTransientErrorDetector" />. Failures the
    ///     driver has already retried (<see cref="SnowflakeTransientErrorDetector.IsRetriedByDriver" />: HTTP errors,
    ///     request timeouts, PUT upload errors, a lost session) are permanent, so exactly one layer retries them. Of the
    ///     errors the server returns for a statement, a throttling message is throttled, the detector's transient errors
    ///     are transient, and every other <see cref="DbException" /> is permanent.
    /// </summary>
    public static Classifier Classifier { get; } = Classifier.Default
        .On<InvalidOperationException>(Judge)
        .On<ObjectDisposedException>(Verdict.Permanent)
        .On<HttpRequestException>(Judge)
        .On<DbException>(Judge);

    /// <summary>
    ///     Four attempts (three retries) of statement-level transient errors, with exponential backoff and full jitter
    ///     from two seconds up to 60 seconds, or from ten seconds when Snowflake throttles. There is no attempt timeout or deadline;
    ///     <see cref="Configuration.SnowflakeConfiguration.CommandTimeout" /> bounds each attempt. Replaces
    ///     <c>MaxRetryAttempts = 3</c> and <c>RetryDelay = 2 s</c>.
    /// </summary>
    public static NResilience.Resilience Default { get; } = new()
    {
        Name = "npipeline.snowflake",
        Attempts = 4,
        AttemptTimeout = Timeout.InfiniteTimeSpan,
        Deadline = Timeout.InfiniteTimeSpan,
        Backoff = Backoff.Default with
        {
            TransientBase = TimeSpan.FromSeconds(2),
            ThrottledBase = TimeSpan.FromSeconds(10),
            MaximumDelay = TimeSpan.FromSeconds(60),
        },
        // Declared above so it is initialized first; a static initializer reads fields in declaration order.
        Classifier = Classifier,
        Adaptive = false,
    };

    private static Verdict Judge(Exception exception)
    {
        // The driver retries each HTTP request of a statement itself; by the time one of these reaches the connector it
        // has given up, and rerunning the statement would multiply its attempts.
        if (SnowflakeTransientErrorDetector.IsRetriedByDriver(exception))
            return Verdict.Permanent;

        if (SnowflakeTransientErrorDetector.IsThrottling(exception))
            return Verdict.Throttled();

        return SnowflakeTransientErrorDetector.IsTransient(exception)
            ? Verdict.Transient
            : Verdict.Permanent;
    }
}
