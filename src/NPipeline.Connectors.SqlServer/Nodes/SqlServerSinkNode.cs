using System.Data.Common;
using Microsoft.Data.SqlClient;
using NPipeline.Connectors.Database;
using NPipeline.Connectors.Sql;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Writers;

namespace NPipeline.Connectors.SqlServer.Nodes;

/// <summary>
///     Writes records to a SQL Server table: with multi-row statements, one statement per row, or <c>SqlBulkCopy</c>, as
///     <see cref="SqlServerWriteOptions.WriteStrategy" /> says. Create one with <see cref="SqlServerConnector" />.
/// </summary>
/// <remarks>
///     Members marked <c>[SqlServerColumn(Identity = true)]</c> are read but not written. Upserts use <c>MERGE</c> with
///     <c>HOLDLOCK</c>; a batch must not repeat a key.
/// </remarks>
/// <typeparam name="T">The record type.</typeparam>
public sealed class SqlServerSinkNode<T> : SqlSinkNode<T>
{
    private static readonly Lazy<IDatabaseConnectionProvider> Provider = new(() => new SqlServerDatabaseConnectionProvider());

    private readonly SqlServerWriteOptions _options;

    /// <summary>Creates a sink that writes <typeparamref name="T" />'s readable members as columns.</summary>
    /// <exception cref="NotSupportedException">A member is not a single value.</exception>
    public SqlServerSinkNode(SqlServerWriteOptions options)
        : base(options, SqlServerDialect.Instance, SqlServerShape.Write((options ?? throw new ArgumentNullException(nameof(options))).Naming))
    {
        _options = options;
    }

    /// <inheritdoc />
    protected override IDatabaseConnectionProvider DefaultProvider => Provider.Value;

    /// <inheritdoc />
    protected override DbConnection CreateConnection(string connectionString) => new SqlConnection(connectionString);

    /// <inheritdoc />
    protected override async Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken) =>
        _options.ConnectionPool is { } pool
            ? _options.ConnectionName is { Length: > 0 } name
                ? await pool.GetConnectionAsync(name, cancellationToken).ConfigureAwait(false)
                : await pool.GetConnectionAsync(cancellationToken).ConfigureAwait(false)
            : await base.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    protected override SqlWriter<T> CreateWriter() => _options.WriteStrategy switch
    {
        SqlServerWriteStrategy.PerRow => new SqlPerRowWriter<T>(Target),
        SqlServerWriteStrategy.BulkCopy => new SqlServerBulkCopyWriter<T>(Target, _options.BulkCopyTimeout),
        _ => new SqlBatchWriter<T>(Target),
    };

    /// <inheritdoc />
    protected override async Task ExecuteAsync(DbConnection connection, Func<CancellationToken, Task> write, CancellationToken cancellationToken)
    {
        var attempt = 0;

        _ = await _options.Resilience.RunAsync(async ct =>
        {
            // A connection-level failure closes the connection; reopen it so the retry has somewhere to run.
            if (attempt++ > 0 && connection.State != System.Data.ConnectionState.Open)
            {
                await connection.CloseAsync().ConfigureAwait(false);
                await connection.OpenAsync(ct).ConfigureAwait(false);
            }

            await write(ct).ConfigureAwait(false);
            return true;
        }, cancellationToken).ConfigureAwait(false);
    }
}
