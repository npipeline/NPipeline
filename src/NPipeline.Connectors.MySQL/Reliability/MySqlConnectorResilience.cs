using NPipeline.Connectors.MySql.Exceptions;
using NResilience;
using MySqlException = MySqlConnector.MySqlException;

namespace NPipeline.Connectors.MySql.Reliability;

/// <summary>
///     Resilience presets for the MySQL connector. Assign one to
///     <see cref="Configuration.MySqlConfiguration.Resilience" />, or derive your own with a <c>with</c> expression.
/// </summary>
/// <remarks>
///     <para>
///         The sink retries each unit of work a writer owns: one row for the per-row writer, one statement for the batch
///         writer, and one <c>LOAD DATA</c> for the bulk load writer. On a transactional engine such as InnoDB each unit
///         commits all or nothing, so a retry never inserts rows that an earlier attempt committed. A non-transactional
///         engine such as MyISAM keeps the rows a failed statement wrote before it failed; for such tables, turn retries
///         off with <see cref="NResilience.Resilience.None" />. Inside a transaction the writer does not own
///         (<see cref="NPipeline.Connectors.Configuration.DeliverySemantic.ExactlyOnce" />), the writer makes one attempt and
///         leaves the failure to the transaction's owner.
///     </para>
///     <para>
///         The presets have no attempt timeout and no deadline. Each attempt is bounded by the driver's own timeout
///         instead: <see cref="Configuration.MySqlConfiguration.CommandTimeout" /> for rows and batches, and
///         <see cref="Configuration.MySqlConfiguration.BulkLoadTimeout" /> for bulk loads. A timeout set on the policy
///         applies to every write strategy, bulk load included.
///     </para>
/// </remarks>
public static class MySqlConnectorResilience
{
    /// <summary>
    ///     <see cref="NResilience.Classifier.Default" />, plus <see cref="MySqlTransientErrorDetector" />: too many
    ///     connections (1040, 1203) is throttled, the detector's other transient errors (deadlock, lock wait timeout, lost
    ///     connection) are transient, and every other <see cref="MySqlConnector.MySqlException" /> is permanent.
    /// </summary>
    public static Classifier Classifier { get; } = Classifier.Default
        .On<InvalidOperationException>(static e => MySqlTransientErrorDetector.IsTransient(e)
            ? Verdict.Transient
            : Verdict.Permanent)
        .On<ObjectDisposedException>(Verdict.Permanent)
        .On<MySqlException>(static e => MySqlTransientErrorDetector.IsThrottlingError(e.Number)
            ? Verdict.Throttled()
            : MySqlTransientErrorDetector.IsTransient(e)
                ? Verdict.Transient
                : Verdict.Permanent);

    /// <summary>
    ///     Four attempts (three retries) and exponential backoff with full jitter from two seconds up to 30 seconds, or
    ///     from five seconds when the server has too many connections. There is no attempt timeout or deadline; the
    ///     driver's command and bulk load timeouts bound each attempt. Replaces <c>MaxRetryAttempts = 3</c> and
    ///     <c>RetryDelay = 2 s</c>.
    /// </summary>
    public static Resilience Default { get; } = new()
    {
        Name = "npipeline.mysql",
        Attempts = 4,
        AttemptTimeout = Timeout.InfiniteTimeSpan,
        Deadline = Timeout.InfiniteTimeSpan,
        Backoff = Backoff.Default with
        {
            TransientBase = TimeSpan.FromSeconds(2),
            ThrottledBase = TimeSpan.FromSeconds(5),
            MaximumDelay = TimeSpan.FromSeconds(30),
        },

        // Declared above so it is initialized first; a static initializer reads fields in declaration order.
        Classifier = Classifier,
        Adaptive = false,
    };
}
