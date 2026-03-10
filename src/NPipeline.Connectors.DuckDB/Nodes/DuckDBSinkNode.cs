using DuckDB.NET.Data;
using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Connection;
using NPipeline.Connectors.DuckDB.Exceptions;
using NPipeline.Connectors.DuckDB.Mapping;
using NPipeline.Connectors.DuckDB.Writers;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.DuckDB.Nodes;

/// <summary>
///     Writes pipeline data into DuckDB tables using the high-performance Appender API (default)
///     or SQL INSERT statements. Also supports exporting to Parquet/CSV files via COPY TO.
/// </summary>
public sealed class DuckDBSinkNode<T> : SinkNode<T>
{
    private readonly DuckDBConfiguration _configuration;
    private readonly IDuckDBConnectionFactory _connectionFactory;
    private readonly string? _exportFilePath;
    private readonly string _tableName;

    /// <summary>
    ///     Write to a table in a DuckDB database.
    /// </summary>
    /// <param name="databasePath">Path to .duckdb file, or null/empty for in-memory.</param>
    /// <param name="tableName">Target table name.</param>
    /// <param name="configuration">Optional configuration.</param>
    public DuckDBSinkNode(string? databasePath, string tableName, DuckDBConfiguration? configuration = null)
        : this(new DuckDBConnectionFactory(databasePath, configuration?.AccessMode ?? DuckDBAccessMode.Automatic),
            tableName, configuration)
    {
    }

    /// <summary>
    ///     Write using an externally managed connection factory.
    /// </summary>
    /// <param name="connectionFactory">The connection factory.</param>
    /// <param name="tableName">Target table name.</param>
    /// <param name="configuration">Optional configuration.</param>
    public DuckDBSinkNode(IDuckDBConnectionFactory connectionFactory, string tableName,
        DuckDBConfiguration? configuration = null)
    {
        _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        _configuration = configuration ?? new DuckDBConfiguration();
    }

    // Private constructor for file export
    private DuckDBSinkNode(IDuckDBConnectionFactory connectionFactory, string tableName,
        string exportFilePath, DuckDBConfiguration? configuration)
    {
        _connectionFactory = connectionFactory;
        _tableName = tableName;
        _exportFilePath = exportFilePath;
        _configuration = configuration ?? new DuckDBConfiguration();
    }

    /// <summary>
    ///     Create a sink that writes pipeline data to a file (Parquet/CSV/JSON) via DuckDB's COPY TO.
    ///     Data is first loaded into a temporary in-memory table, then exported.
    /// </summary>
    /// <param name="filePath">Output file path (format inferred from extension).</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <returns>A configured sink node.</returns>
#pragma warning disable CA1000 // Do not declare static members on generic types
    public static DuckDBSinkNode<T> ToFile(string filePath, DuckDBConfiguration? configuration = null)
#pragma warning restore CA1000
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be empty.", nameof(filePath));

        var stagingTable = $"__npipeline_export_{Guid.NewGuid():N}";
        var factory = new DuckDBConnectionFactory();

        return new DuckDBSinkNode<T>(factory, stagingTable, filePath, configuration);
    }

    /// <inheritdoc />
    public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context,
        CancellationToken cancellationToken)
    {
        _configuration.Validate();

        await using var connection = _connectionFactory.CreateConnection();
        await connection.OpenAsync(cancellationToken);

        await ConfigureConnectionAsync(connection, cancellationToken);

        if (_configuration.AutoCreateTable)
            await EnsureTableExistsAsync(connection, cancellationToken);

        if (_configuration.TruncateBeforeWrite)
            await TruncateTableAsync(connection, cancellationToken);

        // Write data
        if (_configuration.UseTransaction && _configuration.WriteStrategy != DuckDBWriteStrategy.Appender)
            await WriteWithTransactionAsync(connection, input, cancellationToken);
        else
            await WriteWithoutTransactionAsync(connection, input, cancellationToken);

        // File export: COPY TO
        if (_exportFilePath is not null)
            await ExportToFileAsync(connection, cancellationToken);
    }

    private async Task WriteWithTransactionAsync(DuckDBConnection connection, IDataStream<T> input,
        CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);

        try
        {
            await WriteDataAsync(connection, input, ct);
            await transaction.CommitAsync(ct);
        }
        catch
        {
            await transaction.RollbackAsync(ct);
            throw;
        }
    }

    private async Task WriteWithoutTransactionAsync(DuckDBConnection connection, IDataStream<T> input,
        CancellationToken ct)
    {
        await WriteDataAsync(connection, input, ct);
    }

    private async Task WriteDataAsync(DuckDBConnection connection, IDataStream<T> input, CancellationToken ct)
    {
        await using var writer = CreateWriter(connection);

        long rowCount = 0;

        await foreach (var item in input.WithCancellation(ct))
        {
            await writer.WriteAsync(item, ct);
            rowCount++;
            _configuration.Observer?.OnRowWritten(rowCount);
        }

        await writer.FlushAsync(ct);
        _configuration.Observer?.OnWriteCompleted(rowCount);
    }

    private IDuckDBWriter<T> CreateWriter(DuckDBConnection connection)
    {
        return _configuration.WriteStrategy switch
        {
            DuckDBWriteStrategy.Appender => new DuckDBAppenderWriter<T>(connection, _tableName),
            DuckDBWriteStrategy.Sql => new DuckDBSqlWriter<T>(connection, _tableName, _configuration.BatchSize,
                _configuration.Observer),
            _ => throw new DuckDBConnectorException($"Unknown write strategy: {_configuration.WriteStrategy}"),
        };
    }

    private async Task EnsureTableExistsAsync(DuckDBConnection connection, CancellationToken ct)
    {
        try
        {
            var ddl = DuckDBSchemaBuilder.BuildCreateTable<T>(_tableName);
            await using var command = connection.CreateCommand();
            command.CommandText = ddl;
            await command.ExecuteNonQueryAsync(ct);
        }
        catch (Exception ex) when (ex is not DuckDBConnectorException and not OperationCanceledException)
        {
            throw new DuckDBQueryException(
                $"Failed to auto-create table '{_tableName}'.",
                DuckDBSchemaBuilder.BuildCreateTable<T>(_tableName),
                ex);
        }
    }

    private async Task TruncateTableAsync(DuckDBConnection connection, CancellationToken ct)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM \"{_tableName}\"";
        await command.ExecuteNonQueryAsync(ct);
    }

    private async Task ExportToFileAsync(DuckDBConnection connection, CancellationToken ct)
    {
        var options = _configuration.FileExportOptions ?? new DuckDBFileExportOptions();
        var copyOptions = options.BuildCopyOptions(_exportFilePath!);

        // Ensure output directory exists
        var directory = Path.GetDirectoryName(_exportFilePath!);

        if (!string.IsNullOrEmpty(directory))
            Directory.CreateDirectory(directory);

        await using var command = connection.CreateCommand();
        command.CommandText = $"COPY \"{_tableName}\" TO '{_exportFilePath!.Replace("'", "''")}' ({copyOptions})";
        await command.ExecuteNonQueryAsync(ct);
    }

    private async Task ConfigureConnectionAsync(DuckDBConnection connection, CancellationToken ct)
    {
        // Load extensions
        if (_configuration.Extensions is { Length: > 0 })
        {
            foreach (var extension in _configuration.Extensions)
            {
                await using var cmd = connection.CreateCommand();
                cmd.CommandText = $"INSTALL '{extension}'; LOAD '{extension}';";
                await cmd.ExecuteNonQueryAsync(ct);
                _configuration.Observer?.OnExtensionLoaded(extension);
            }
        }

        // Apply settings
        var settings = new List<string>();

        if (_configuration.MemoryLimit is not null)
            settings.Add($"SET memory_limit = '{_configuration.MemoryLimit}'");

        if (_configuration.Threads > 0)
            settings.Add($"SET threads = {_configuration.Threads}");

        if (_configuration.TempDirectory is not null)
            settings.Add($"SET temp_directory = '{_configuration.TempDirectory}'");

        if (_configuration.Settings is not null)
        {
            foreach (var (key, value) in _configuration.Settings)
            {
                settings.Add($"SET {key} = '{value}'");
            }
        }

        foreach (var setting in settings)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = setting;
            await cmd.ExecuteNonQueryAsync(ct);
        }
    }
}
