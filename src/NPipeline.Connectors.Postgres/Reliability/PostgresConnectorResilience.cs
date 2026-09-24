using Npgsql;
using NPipeline.Connectors.Postgres.Exceptions;
using NResilience;
using PostgresException = NPipeline.Connectors.Postgres.Exceptions.PostgresException;

namespace NPipeline.Connectors.Postgres.Reliability;

/// <summary>
///     Resilience presets for the PostgreSQL connector. Assign one to
///     <see cref="Configuration.PostgresConfiguration.Resilience" />, or derive your own with a <c>with</c> expression.
/// </summary>
/// <remarks>
///     <para>
///         The sink retries each unit of work a writer owns: one row for the per-row writer, one statement for the batch
///         writer, and one <c>COPY</c> for the copy writer. Each unit commits all or nothing, so a retry never inserts rows
///         that an earlier attempt committed. Inside a transaction the writer does not own
///         (<see cref="NPipeline.Connectors.Configuration.DeliverySemantic.ExactlyOnce" />), PostgreSQL aborts the whole
///         transaction on the first error, so the writer makes one attempt and leaves the failure to the transaction's owner.
///     </para>
///     <para>
///         The source (<see cref="Nodes.PostgresSourceNode{T}" />) retries getting a connection and opening the query's
///         reader. Once rows are flowing a failure is not retried, because the rows already emitted would be emitted again.
///     </para>
///     <para>
///         The presets have no attempt timeout and no deadline. Each attempt is bounded by
///         <see cref="Configuration.PostgresConfiguration.CommandTimeout" /> instead, or
///         <see cref="Configuration.PostgresConfiguration.CopyTimeout" /> for <c>COPY</c>. A timeout set on the policy applies to
///         every operation, <c>COPY</c> included.
///     </para>
/// </remarks>
public static class PostgresConnectorResilience
{
    /// <summary>
    ///     <see cref="NResilience.Classifier.Default" />, plus the connector's SQLSTATE knowledge: too many connections
    ///     (53300) is throttled; connection failures, serialization failures, deadlocks, resource errors, and shutdowns are
    ///     transient; any other server error is permanent; and a client-side <see cref="NpgsqlException" /> follows
    ///     <see cref="NpgsqlException.IsTransient" />.
    /// </summary>
    public static Classifier Classifier { get; } = Classifier.Default
        .On<NpgsqlException>(Judge)
        .On<PostgresException>(Judge);

    /// <summary>
    ///     Four attempts (three retries) and exponential backoff with full jitter from one second up to 30 seconds, or
    ///     from five seconds when the server has too many connections. There is no attempt timeout or deadline;
    ///     <see cref="Configuration.PostgresConfiguration.CommandTimeout" /> (or <c>CopyTimeout</c> for <c>COPY</c>) bounds
    ///     each attempt. Replaces
    ///     <c>MaxRetryAttempts = 3</c> and <c>RetryDelay = 1 s</c>.
    /// </summary>
    public static Resilience Default { get; } = new()
    {
        Name = "npipeline.postgres",
        Attempts = 4,
        AttemptTimeout = Timeout.InfiniteTimeSpan,
        Deadline = Timeout.InfiniteTimeSpan,
        Backoff = Backoff.Default with
        {
            TransientBase = TimeSpan.FromSeconds(1),
            ThrottledBase = TimeSpan.FromSeconds(5),
            MaximumDelay = TimeSpan.FromSeconds(30),
        },

        // Declared above so it is initialized first; a static initializer reads fields in declaration order.
        Classifier = Classifier,
        Adaptive = false,
    };

    private static Verdict Judge(Exception exception)
    {
        return exception switch
        {
            Npgsql.PostgresException server => FromSqlState(server.SqlState),
            NpgsqlException client => client.IsTransient
                ? Verdict.Transient
                : Verdict.Permanent,

            // The connector's own wrapper, thrown by the source: judge the SQLSTATE it carries, or what it wraps.
            PostgresException { ErrorCode: { } sqlState } => FromSqlState(sqlState),
            PostgresException { InnerException: NpgsqlException or PostgresException } wrapper => Judge(wrapper.InnerException!),
            PostgresException { InnerException: { } inner } => Classifier.Default.ClassifyException(inner),
            _ => Verdict.Permanent,
        };
    }

    private static Verdict FromSqlState(string sqlState)
    {
        if (PostgresTransientErrorDetector.IsThrottlingSqlState(sqlState))
            return Verdict.Throttled();

        return PostgresTransientErrorDetector.IsTransientSqlState(sqlState) || PostgresExceptionHandler.IsTransientSqlState(sqlState)
            ? Verdict.Transient
            : Verdict.Permanent;
    }
}
