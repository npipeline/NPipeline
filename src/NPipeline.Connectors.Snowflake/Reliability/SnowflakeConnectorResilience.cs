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
///         The presets have no attempt timeout and no deadline. Each attempt is bounded by
///         <see cref="Configuration.SnowflakeConfiguration.CommandTimeout" /> instead, which suits a <c>COPY INTO</c> that
///         runs for minutes. A timeout set on the policy applies to every write strategy, staged copy included.
///     </para>
/// </remarks>
public static class SnowflakeConnectorResilience
{
    /// <summary>
    ///     <see cref="NResilience.Classifier.Default" />, plus <see cref="SnowflakeTransientErrorDetector" />: throttling
    ///     (HTTP 429 or a throttling message) is throttled, the detector's other transient errors (network errors,
    ///     service unavailable, statement timeout) are transient, and every other <see cref="DbException" /> is permanent.
    /// </summary>
    public static Classifier Classifier { get; } = Classifier.Default
        .On<InvalidOperationException>(Judge)
        .On<ObjectDisposedException>(Verdict.Permanent)
        .On<HttpRequestException>(Judge)
        .On<DbException>(Judge);

    /// <summary>
    ///     Four attempts (three retries) and exponential backoff with full jitter from two seconds up to 60 seconds, or
    ///     from ten seconds when Snowflake throttles. There is no attempt timeout or deadline;
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
        if (SnowflakeTransientErrorDetector.IsThrottling(exception))
            return Verdict.Throttled();

        return SnowflakeTransientErrorDetector.IsTransient(exception)
            ? Verdict.Transient
            : Verdict.Permanent;
    }
}
