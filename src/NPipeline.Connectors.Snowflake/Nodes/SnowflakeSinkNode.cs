using System.Data.Common;
using Snowflake.Data.Client;
using NPipeline.Connectors.Database;
using NPipeline.Connectors.Sql;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Writers;

namespace NPipeline.Connectors.Snowflake.Nodes;

/// <summary>
///     Writes records to a Snowflake table: with multi-row statements, one statement per row, or a staged CSV file loaded
///     with <c>COPY INTO</c>, as <see cref="SnowflakeWriteOptions.WriteStrategy" /> says. Create one with
///     <see cref="SnowflakeConnector" />.
/// </summary>
/// <remarks>
///     Members marked <c>[SnowflakeColumn(Identity = true)]</c> are read but not written. Upserts use <c>MERGE</c>; a
///     batch must not repeat a key.
/// </remarks>
/// <typeparam name="T">The record type.</typeparam>
public sealed class SnowflakeSinkNode<T> : SqlSinkNode<T>
{
    private static readonly Lazy<IDatabaseConnectionProvider> Provider = new(() => new SnowflakeDatabaseConnectionProvider());

    private readonly SnowflakeWriteOptions _options;

    /// <summary>Creates a sink that writes <typeparamref name="T" />'s readable members as columns.</summary>
    /// <exception cref="NotSupportedException">A member is not a single value.</exception>
    public SnowflakeSinkNode(SnowflakeWriteOptions options)
        : base(options, SnowflakeDialect.Instance, SnowflakeShape.Write((options ?? throw new ArgumentNullException(nameof(options))).Naming))
    {
        _options = options;
    }

    /// <inheritdoc />
    protected override IDatabaseConnectionProvider DefaultProvider => Provider.Value;

    /// <inheritdoc />
    protected override DbConnection CreateConnection(string connectionString) => new SnowflakeDbConnection(connectionString);

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
        SnowflakeWriteStrategy.PerRow => new SqlPerRowWriter<T>(Target),
        SnowflakeWriteStrategy.StagedCopy => new SnowflakeStagedCopyWriter<T>(Target, _options.Stage, _options.StageFilePrefix, _options.PurgeStagedFiles),
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
