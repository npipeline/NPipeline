using System.Data.Common;
using DuckDB.NET.Data;
using NPipeline.Connectors.Database;
using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Writers;
using NPipeline.Connectors.Sql;

namespace NPipeline.Connectors.DuckDB.Nodes;

/// <summary>
///     Writes records to a DuckDB table with its appender or with <c>INSERT</c> statements, creating the table first when
///     <see cref="DuckDBWriteOptions.AutoCreateTable" /> is set, and exporting it to a file afterwards when
///     <see cref="DuckDBWriteOptions.ExportTo" /> is. Create one with <see cref="DuckDBConnector" />.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public sealed class DuckDBSinkNode<T> : SqlSinkNode<T>
{
    private readonly DuckDBWriteOptions _options;

    /// <summary>Creates a sink that writes <typeparamref name="T" />'s readable members as columns.</summary>
    /// <exception cref="NotSupportedException">A member is not a single value.</exception>
    public DuckDBSinkNode(DuckDBWriteOptions options)
        : base(options, DuckDBDialect.Instance, DuckDBShape.For((options ?? throw new ArgumentNullException(nameof(options))).Naming))
    {
        _options = options;
    }

    /// <inheritdoc />
    protected override IDatabaseConnectionProvider DefaultProvider { get; } = new DuckDBDatabaseConnectionProvider();

    /// <inheritdoc />
    protected override DbConnection CreateConnection(string connectionString) => new DuckDBConnection(connectionString);

    /// <inheritdoc />
    protected override Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken) =>
        _options.ConnectionString is null && _options.Uri is null
            ? _options.Database.OpenAsync(cancellationToken)
            : base.OpenConnectionAsync(cancellationToken);

    /// <inheritdoc />
    protected override SqlWriter<T> CreateWriter() => _options.WriteStrategy switch
    {
        DuckDBWriteStrategy.Sql => new SqlBatchWriter<T>(Target),
        _ => new DuckDBAppenderWriter<T>(Target, _options.Schema, _options.Table),
    };

    /// <inheritdoc />
    protected override async Task PrepareAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        if (_options.AutoCreateTable)
            await ExecuteAsync(connection, DuckDBDialect.CreateTable(Target), cancellationToken).ConfigureAwait(false);

        if (_options.TruncateBeforeWrite)
            await ExecuteAsync(connection, $"DELETE FROM {Target.QualifiedTable}", cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    protected override async Task CompleteAsync(DbConnection connection, CancellationToken cancellationToken)
    {
        if (_options.ExportTo is not { } path)
            return;

        var directory = Path.GetDirectoryName(Path.GetFullPath(path));

        if (!string.IsNullOrEmpty(directory))
            _ = Directory.CreateDirectory(directory);

        var sql = $"COPY {Target.QualifiedTable} TO '{path.Replace("'", "''", StringComparison.Ordinal)}' ({_options.Export.BuildCopyOptions(path)})";
        await ExecuteAsync(connection, sql, cancellationToken).ConfigureAwait(false);
    }

    private async Task ExecuteAsync(DbConnection connection, string sql, CancellationToken cancellationToken)
    {
        var command = connection.CreateCommand();

        await using (command.ConfigureAwait(false))
        {
            command.CommandText = sql;
            command.CommandTimeout = _options.CommandTimeout;
            _ = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
