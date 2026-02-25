using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Connection;
using NPipeline.Connectors.PostgreSQL.Exceptions;
using NPipeline.Connectors.PostgreSQL.Mapping;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using NPipeline.StorageProviders.Utilities;
using PostgresException = NPipeline.Connectors.PostgreSQL.Exceptions.PostgresException;

namespace NPipeline.Connectors.PostgreSQL.Writers;

/// <summary>
///     High-performance COPY write strategy for PostgreSQL using PostgreSQL's COPY protocol.
///     Supports both binary and text COPY formats for maximum throughput.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal sealed class PostgresCopyWriter<T> : IDatabaseWriter<T>
{
    private readonly PostgresConfiguration _configuration;
    private readonly IDatabaseConnection _connection;
    private readonly int _flushThreshold;
    private readonly PropertyMapping[] _mappings;
    private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
    private readonly List<T> _pendingRows;
    private readonly string _schema;
    private readonly string _tableName;
    private readonly Func<T, object?[]> _valueFactory;

    /// <summary>
    ///     Initializes a new instance of <see cref="PostgresCopyWriter{T}" /> class.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="schema">The schema name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="parameterMapper">Optional parameter mapper function.</param>
    /// <param name="configuration">The configuration.</param>
    public PostgresCopyWriter(
        IDatabaseConnection connection,
        string schema,
        string tableName,
        Func<T, IEnumerable<DatabaseParameter>>? parameterMapper,
        PostgresConfiguration configuration)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();
        _parameterMapper = parameterMapper;
        _mappings = BuildMappings();
        _valueFactory = BuildValueFactory(_mappings);
        _flushThreshold = Math.Clamp(_configuration.BatchSize, 1, _configuration.MaxBatchSize);
        _pendingRows = new List<T>(_flushThreshold);
    }

    /// <summary>
    ///     Writes a single item to the buffer.
    /// </summary>
    /// <param name="item">The item to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        _pendingRows.Add(item);

        if (_pendingRows.Count >= _flushThreshold)
            await FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Writes a batch of items using COPY protocol.
    /// </summary>
    /// <param name="items">The items to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task WriteBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        foreach (var item in items)
        {
            _pendingRows.Add(item);

            if (_pendingRows.Count >= _flushThreshold)
                await FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        if (_pendingRows.Count > 0)
            await FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Flushes the buffer to the database using COPY protocol.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        if (_pendingRows.Count == 0)
            return;

        var attempt = 0;
        var maxAttempts = _configuration.MaxRetryAttempts + 1;

        while (attempt < maxAttempts)
        {
            try
            {
                var npgsqlConnection = GetNpgsqlConnection();

                if (_configuration.UseBinaryCopy)
                    await ExecuteBinaryCopyAsync(npgsqlConnection, cancellationToken).ConfigureAwait(false);
                else
                    await ExecuteTextCopyAsync(npgsqlConnection, cancellationToken).ConfigureAwait(false);

                _pendingRows.Clear();
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts - 1 && PostgresExceptionHandler.IsTransient(ex))
            {
                attempt++;
                var delay = CalculateRetryDelay(attempt);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        // If we get here, all retries failed - execute one final time to throw the exception
        var finalConnection = GetNpgsqlConnection();

        if (_configuration.UseBinaryCopy)
            await ExecuteBinaryCopyAsync(finalConnection, cancellationToken).ConfigureAwait(false);
        else
            await ExecuteTextCopyAsync(finalConnection, cancellationToken).ConfigureAwait(false);

        _pendingRows.Clear();
    }

    /// <summary>
    ///     Disposes the writer.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // Flush any buffered items before disposal
        try
        {
            await FlushAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Log the exception but don't throw during disposal
            // This is important for debugging flush failures during disposal
            Debug.WriteLine(
                $"Warning: Failed to flush during disposal for PostgresCopyWriter<{typeof(T).Name}>: {ex.Message}");
        }
    }

    /// <summary>
    ///     Calculates the retry delay using exponential backoff with jitter.
    /// </summary>
    /// <param name="attempt">The current attempt number (1-based).</param>
    /// <returns>The delay duration.</returns>
    private TimeSpan CalculateRetryDelay(int attempt)
    {
        var baseDelay = _configuration.RetryDelay;
        var exponentialDelay = TimeSpan.FromMilliseconds(baseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1));

        // Add jitter (10-20% of the delay)
        var jitter = TimeSpan.FromMilliseconds(exponentialDelay.TotalMilliseconds * (0.1 + Random.Shared.NextDouble() * 0.1));

        return exponentialDelay + jitter;
    }

    /// <summary>
    ///     Gets the underlying NpgsqlConnection from the database connection.
    /// </summary>
    private NpgsqlConnection GetNpgsqlConnection()
    {
        if (_connection is PostgresDatabaseConnection postgresConnection)
        {
            // COPY operations cannot be used within an active transaction for ExactlyOnce semantics
            // because PostgreSQL COPY cannot be properly rolled back once started.
            // Users should use Batch write strategy when ExactlyOnce semantics are required.
            if (_connection.CurrentTransaction != null)
            {
                throw new PostgresException(
                    "COPY operations cannot be used within an active transaction. " +
                    "Use Batch write strategy (PostgresWriteStrategy.Batch) when ExactlyOnce delivery semantics are required.");
            }

            return postgresConnection.UnderlyingConnection;
        }

        throw new PostgresException(
            $"Expected connection of type '{nameof(PostgresDatabaseConnection)}' but got '{_connection.GetType().Name}'. " +
            "COPY operations require access to the underlying NpgsqlConnection.");
    }

    /// <summary>
    ///     Executes binary COPY import for maximum performance.
    /// </summary>
    private async Task ExecuteBinaryCopyAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var copyCommand = BuildCopyCommand();

        await using var importer = await connection.BeginBinaryImportAsync(copyCommand, cancellationToken).ConfigureAwait(false);

        foreach (var item in _pendingRows)
        {
            var values = GetValues(item);
            await importer.StartRowAsync(cancellationToken).ConfigureAwait(false);

            for (var i = 0; i < values.Length; i++)
            {
                var value = values[i];
                var mapping = _mappings[i];

                if (value == null || value == DBNull.Value)
                    await importer.WriteNullAsync(cancellationToken).ConfigureAwait(false);
                else if (mapping.NpgsqlDbType.HasValue)
                    await importer.WriteAsync(value, mapping.NpgsqlDbType.Value, cancellationToken).ConfigureAwait(false);
                else
                    await importer.WriteAsync(value, cancellationToken).ConfigureAwait(false);
            }
        }

        await importer.CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Executes text COPY import.
    /// </summary>
    private async Task ExecuteTextCopyAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var copyCommand = BuildCopyCommand();
        var copyData = BuildTextCopyData();

        await using var writer = await connection.BeginTextImportAsync(copyCommand, cancellationToken).ConfigureAwait(false);

        foreach (var line in copyData)
        {
            await writer.WriteLineAsync(line).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Builds the COPY SQL command.
    /// </summary>
    private string BuildCopyCommand()
    {
        if (_mappings.Length == 0)
            throw new InvalidOperationException($"Type '{typeof(T).Name}' does not expose any writable properties to persist.");

        var quotedTableName = DatabaseIdentifierValidator.QuoteIdentifier($"{_schema}.{_tableName}");

        var quotedColumns = _mappings
            .Select(m => ValidateAndQuoteIdentifier(m.ColumnName, nameof(m.ColumnName)))
            .ToArray();

        var columnList = string.Join(", ", quotedColumns);

        return
            $"COPY {quotedTableName} ({columnList}) FROM STDIN (FORMAT {_configuration.UseBinaryCopy switch { true => "BINARY", _ => "CSV" }}, DELIMITER '\t', NULL '\\N')";
    }

    /// <summary>
    ///     Builds text COPY data lines.
    /// </summary>
    private IEnumerable<string> BuildTextCopyData()
    {
        foreach (var item in _pendingRows)
        {
            var values = GetValues(item);
            var sb = new StringBuilder();

            for (var i = 0; i < values.Length; i++)
            {
                if (i > 0)
                    sb.Append('\t');

                var value = values[i];

                sb.Append(value == null || value == DBNull.Value
                    ? "\\N"
                    : EscapeCopyValue(value.ToString() ?? string.Empty));
            }

            yield return sb.ToString();
        }
    }

    /// <summary>
    ///     Escapes a value for text COPY format.
    /// </summary>
    private static string EscapeCopyValue(string value)
    {
        return value
            .Replace("\\", "\\\\")
            .Replace("\t", "\\t")
            .Replace("\n", "\\n")
            .Replace("\r", "\\r");
    }

    /// <summary>
    ///     Gets values from an item using convention-based mapping.
    /// </summary>
    private object?[] GetValues(T item)
    {
        if (_parameterMapper == null)
            return _valueFactory(item);

        var mapped = _parameterMapper(item)?.ToArray() ?? Array.Empty<DatabaseParameter>();

        if (mapped.Length != _mappings.Length)
        {
            throw new InvalidOperationException(
                $"Custom parameter mapper for '{typeof(T).Name}' must return exactly {_mappings.Length} values to match the mapped columns.");
        }

        var values = new object?[_mappings.Length];

        for (var i = 0; i < _mappings.Length; i++)
        {
            values[i] = mapped[i].Value;
        }

        return values;
    }

    /// <summary>
    ///     Builds property mappings from type T.
    /// </summary>
    private PropertyMapping[] BuildMappings()
    {
        return
        [
            .. typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanWrite && !IsIgnored(p))
                .Select(p => new PropertyMapping(
                    GetColumnName(p),
                    BuildGetter(p),
                    p.GetCustomAttribute<PostgresColumnAttribute>()?.DbType)),
        ];
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<PostgresColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);
        return ignoredByAttribute || hasIgnoreMarker;
    }

    private static string GetColumnName(PropertyInfo property)
    {
        var columnAttr = property.GetCustomAttribute<PostgresColumnAttribute>();
        return columnAttr?.Name ?? ToSnakeCase(property.Name);
    }

    private static Func<T, object?> BuildGetter(PropertyInfo property)
    {
        var instanceParam = Expression.Parameter(typeof(T), "item");
        var propertyAccess = Expression.Property(instanceParam, property);
        var convert = Expression.Convert(propertyAccess, typeof(object));
        return Expression.Lambda<Func<T, object?>>(convert, instanceParam).Compile();
    }

    private static Func<T, object?[]> BuildValueFactory(IReadOnlyList<PropertyMapping> mappings)
    {
        return item =>
        {
            var values = new object?[mappings.Count];

            for (var i = 0; i < mappings.Count; i++)
            {
                values[i] = mappings[i].Getter(item);
            }

            return values;
        };
    }

    private string ValidateAndQuoteIdentifier(string identifier, string paramName)
    {
        if (_configuration.ValidateIdentifiers)
            DatabaseIdentifierValidator.ValidateIdentifier(identifier, paramName);

        return DatabaseIdentifierValidator.QuoteIdentifier(identifier);
    }

    private static string ToSnakeCase(string str)
    {
        return string.Concat(str.Select((x, i) => i > 0 && char.IsUpper(x)
            ? "_" + x
            : x.ToString())).ToLowerInvariant();
    }

    /// <summary>
    ///     Represents a property mapping for COPY operations.
    /// </summary>
    private sealed record PropertyMapping(
        string ColumnName,
        Func<T, object?> Getter,
        NpgsqlDbType? NpgsqlDbType = null);
}
