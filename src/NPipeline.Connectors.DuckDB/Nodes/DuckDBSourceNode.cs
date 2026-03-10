using System.Data;
using System.Runtime.CompilerServices;
using DuckDB.NET.Data;
using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Connection;
using NPipeline.Connectors.DuckDB.Exceptions;
using NPipeline.Connectors.DuckDB.Mapping;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.DuckDB.Nodes;

/// <summary>
///     Reads data from DuckDB via SQL query and streams rows as <typeparamref name="T" />.
///     Supports querying in-memory tables, Parquet files, CSV files, and any DuckDB-supported source.
/// </summary>
public sealed class DuckDBSourceNode<T> : SourceNode<T>
{
    private readonly DuckDBConfiguration _configuration;
    private readonly IDuckDBConnectionFactory _connectionFactory;
    private readonly string _query;
    private readonly Func<DuckDBRow, T>? _rowMapper;

    /// <summary>
    ///     Query an in-memory or file-based DuckDB database.
    /// </summary>
    /// <param name="databasePath">Path to .duckdb file, or null/empty for in-memory.</param>
    /// <param name="query">SQL query to execute.</param>
    /// <param name="configuration">Optional configuration.</param>
    public DuckDBSourceNode(string? databasePath, string query, DuckDBConfiguration? configuration = null)
        : this(new DuckDBConnectionFactory(databasePath, configuration?.AccessMode ?? DuckDBAccessMode.Automatic),
            query, null, configuration)
    {
    }

    /// <summary>
    ///     Query with a custom row mapper.
    /// </summary>
    /// <param name="databasePath">Path to .duckdb file, or null/empty for in-memory.</param>
    /// <param name="query">SQL query to execute.</param>
    /// <param name="rowMapper">Custom function to map rows to objects.</param>
    /// <param name="configuration">Optional configuration.</param>
    public DuckDBSourceNode(string? databasePath, string query, Func<DuckDBRow, T> rowMapper,
        DuckDBConfiguration? configuration = null)
        : this(new DuckDBConnectionFactory(databasePath, configuration?.AccessMode ?? DuckDBAccessMode.Automatic),
            query, rowMapper, configuration)
    {
    }

    /// <summary>
    ///     Query using an externally managed connection factory (DI-friendly).
    /// </summary>
    /// <param name="connectionFactory">The connection factory.</param>
    /// <param name="query">SQL query to execute.</param>
    /// <param name="configuration">Optional configuration.</param>
    public DuckDBSourceNode(IDuckDBConnectionFactory connectionFactory, string query,
        DuckDBConfiguration? configuration = null)
        : this(connectionFactory, query, null, configuration)
    {
    }

    /// <summary>
    ///     Query using an externally managed connection factory with a custom mapper.
    /// </summary>
    /// <param name="connectionFactory">The connection factory.</param>
    /// <param name="query">SQL query to execute.</param>
    /// <param name="rowMapper">Custom function to map rows to objects.</param>
    /// <param name="configuration">Optional configuration.</param>
    public DuckDBSourceNode(IDuckDBConnectionFactory connectionFactory, string query,
        Func<DuckDBRow, T>? rowMapper, DuckDBConfiguration? configuration = null)
    {
        _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
        _query = query ?? throw new ArgumentNullException(nameof(query));
        _rowMapper = rowMapper;
        _configuration = configuration ?? new DuckDBConfiguration();
    }

    /// <summary>
    ///     Query a Parquet/CSV/JSON file directly via DuckDB SQL.
    ///     File format is auto-detected from the extension.
    /// </summary>
    /// <param name="filePath">Path to the file (supports glob patterns like *.parquet).</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <returns>A configured source node.</returns>
#pragma warning disable CA1000 // Do not declare static members on generic types
    public static DuckDBSourceNode<T> FromFile(string filePath, DuckDBConfiguration? configuration = null)
#pragma warning restore CA1000
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be empty.", nameof(filePath));

        // Determine the file extension — for glob patterns, strip the glob chars
        var extensionSource = filePath.Replace("*", "").Replace("?", "");
        var extension = Path.GetExtension(extensionSource).ToLowerInvariant();

        var function = extension switch
        {
            ".parquet" => "read_parquet",
            ".csv" or ".tsv" => "read_csv",
            ".json" or ".ndjson" or ".jsonl" => "read_json",
            _ => throw new DuckDBConnectorException(
                $"Unsupported file format: '{extension}'. Supported formats: .parquet, .csv, .tsv, .json, .ndjson, .jsonl"),
        };

        var escapedPath = filePath.Replace("'", "''");

        var query = function == "read_csv"
            ? $"SELECT * FROM {function}('{escapedPath}', auto_detect=true)"
            : $"SELECT * FROM {function}('{escapedPath}')";

        return new DuckDBSourceNode<T>((string?)null, query, configuration);
    }

    /// <summary>
    ///     Query a file with custom SQL. Use <c>{file}</c> as a placeholder for the file path.
    /// </summary>
    /// <param name="filePath">Path to the file.</param>
    /// <param name="queryTemplate">SQL template containing a {file} placeholder.</param>
    /// <param name="configuration">Optional configuration.</param>
    /// <returns>A configured source node.</returns>
#pragma warning disable CA1000
    public static DuckDBSourceNode<T> FromFile(string filePath, string queryTemplate,
        DuckDBConfiguration? configuration = null)
#pragma warning restore CA1000
    {
        if (string.IsNullOrWhiteSpace(filePath))
            throw new ArgumentException("File path cannot be empty.", nameof(filePath));

        if (string.IsNullOrWhiteSpace(queryTemplate))
            throw new ArgumentException("Query template cannot be empty.", nameof(queryTemplate));

        if (!queryTemplate.Contains("{file}", StringComparison.Ordinal))
            throw new DuckDBConnectorException("queryTemplate must include a {file} placeholder.");

        var escapedPath = filePath.Replace("'", "''");
        var query = queryTemplate.Replace("{file}", escapedPath, StringComparison.Ordinal);

        return new DuckDBSourceNode<T>((string?)null, query, configuration);
    }

    /// <inheritdoc />
    public override IDataStream<T> OpenStream(PipelineContext context, CancellationToken cancellationToken)
    {
        _configuration.Validate();

        var stream = StreamRowsAsync(cancellationToken);
        return new DataStream<T>(stream, $"DuckDBSourceNode<{typeof(T).Name}>");
    }

    private async IAsyncEnumerable<T> StreamRowsAsync(
        [EnumeratorCancellation] CancellationToken ct)
    {
        DuckDBConnection? connection = null;

        try
        {
            connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync(ct);

            await ConfigureConnectionAsync(connection, ct);

            _configuration.Observer?.OnQueryStarted(_query);

            await using var command = connection.CreateCommand();
            command.CommandText = _query;

            await using var reader = await command.ExecuteReaderAsync(
                _configuration.StreamResults
                    ? CommandBehavior.SequentialAccess
                    : CommandBehavior.Default,
                ct);

            var mapper = _rowMapper ?? DuckDBMapperBuilder.Build<T>(reader, _configuration);
            var row = new DuckDBRow(reader, _configuration.CaseInsensitiveMapping);
            long rowIndex = 0;

            while (await reader.ReadAsync(ct))
            {
                row.SetCurrentRow(rowIndex);
                T item;

                try
                {
                    item = mapper(row);
                }
                catch (Exception ex) when (_configuration.RowErrorHandler?.Invoke(ex, rowIndex) == true)
                {
                    _configuration.Observer?.OnRowSkipped(rowIndex, ex);
                    rowIndex++;
                    continue;
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    throw new DuckDBMappingException(
                        $"Failed to map row {rowIndex} to type '{typeof(T).Name}'.",
                        rowIndex, ex);
                }

                _configuration.Observer?.OnRowRead(rowIndex);
                rowIndex++;
                yield return item;
            }

            _configuration.Observer?.OnReadCompleted(rowIndex);
        }
        finally
        {
            if (connection is not null)
                await connection.DisposeAsync();
        }
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
