using Microsoft.Data.SqlClient;
using NPipeline.Connectors.SqlServer.Exceptions;
using NResilience;

namespace NPipeline.Connectors.SqlServer.Reliability;

/// <summary>
///     Resilience presets for the SQL Server connector. Assign one to
///     <see cref="Configuration.SqlServerConfiguration.Resilience" />, or derive your own with a <c>with</c> expression.
/// </summary>
/// <remarks>
///     <para>
///         The policy retries each unit of work a writer owns: one row for the per-row writer, one statement for the batch
///         writer, and one transaction for the bulk copy writer. A unit that fails is rolled back before it is retried, so
///         a retry never inserts rows that an earlier attempt committed. Inside a transaction the writer does not own
///         (<see cref="NPipeline.Connectors.Configuration.DeliverySemantic.ExactlyOnce" />), the writer makes one attempt and
///         leaves the failure to the transaction's owner.
///     </para>
///     <para>
///         The presets have no attempt timeout and no deadline. Each attempt is bounded by the driver's own timeout
///         instead: <see cref="Configuration.SqlServerConfiguration.CommandTimeout" /> for rows and batches, and
///         <see cref="Configuration.SqlServerConfiguration.BulkCopyTimeout" /> for bulk copy. A timeout set on the policy
///         applies to every write strategy, bulk copy included.
///     </para>
/// </remarks>
public static class SqlServerConnectorResilience
{
    /// <summary>
    ///     <see cref="NResilience.Classifier.Default" />, plus <see cref="SqlServerTransientErrorDetector" />: Azure SQL
    ///     throttling (40501, 10928, 10929, 49918-49920) is throttled, the detector's other transient errors are transient,
    ///     and every other <see cref="SqlException" /> is permanent.
    /// </summary>
    public static Classifier Classifier { get; } = Classifier.Default
        .On<InvalidOperationException>(static e => SqlServerTransientErrorDetector.IsTransient(e)
            ? Verdict.Transient
            : Verdict.Permanent)
        .On<ObjectDisposedException>(Verdict.Permanent)
        .On<SqlException>(static e => SqlServerTransientErrorDetector.IsThrottling(e)
            ? Verdict.Throttled()
            : SqlServerTransientErrorDetector.IsTransient(e)
                ? Verdict.Transient
                : Verdict.Permanent);

    /// <summary>
    ///     Four attempts (three retries) and exponential backoff with full jitter from one second up to 30 seconds, or
    ///     from ten seconds when Azure SQL throttles. There is no attempt timeout or deadline; the driver's command and
    ///     bulk copy timeouts bound each attempt. Replaces <c>MaxRetryAttempts = 3</c> and <c>RetryDelay = 1 s</c>.
    /// </summary>
    public static NResilience.Resilience Default { get; } = new()
    {
        Name = "npipeline.sqlserver",
        Attempts = 4,
        AttemptTimeout = Timeout.InfiniteTimeSpan,
        Deadline = Timeout.InfiniteTimeSpan,
        Backoff = Backoff.Default with
        {
            TransientBase = TimeSpan.FromSeconds(1),
            ThrottledBase = TimeSpan.FromSeconds(10),
            MaximumDelay = TimeSpan.FromSeconds(30),
        },
        // Declared above so it is initialized first; a static initializer reads fields in declaration order.
        Classifier = Classifier,
        Adaptive = false,
    };
}
