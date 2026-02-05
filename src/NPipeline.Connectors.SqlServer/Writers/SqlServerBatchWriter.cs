using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Exceptions;
using NPipeline.Connectors.SqlServer.Mapping;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.Connectors.Attributes;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.SqlServer.Writers;

/// <summary>
///     Batch write strategy for SQL Server.
///     Buffers rows and writes them in batches using multi-row INSERT statements.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal sealed class SqlServerBatchWriter<T> : IDatabaseWriter<T>
{
    private const int MaxParametersPerCommand = 2100;
    private readonly SqlServerConfiguration _configuration;
    private readonly IDatabaseConnection _connection;
    private readonly int _flushThreshold;
    private readonly string _insertSql;
    private readonly PropertyMapping[] _mappings;
    private readonly int _parameterCount;
    private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
    private readonly List<object?[]> _pendingRows;
    private readonly string _schema;
    private readonly string _tableName;
    private readonly Func<T, object?[]> _valueFactory;

    /// <summary>
    ///     Initializes a new instance of <see cref="SqlServerBatchWriter{T}" /> class.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="schema">The schema name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="parameterMapper">Optional parameter mapper function.</param>
    /// <param name="configuration">The configuration.</param>
    public SqlServerBatchWriter(
        IDatabaseConnection connection,
        string schema,
        string tableName,
        Func<T, IEnumerable<DatabaseParameter>>? parameterMapper,
        SqlServerConfiguration configuration)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _schema = schema ?? throw new ArgumentNullException(nameof(schema));
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();
        _parameterMapper = parameterMapper;
        _mappings = BuildMappings();
        _parameterCount = _mappings.Length;
        _valueFactory = BuildValueFactory(_mappings);

        var maxRowsPerBatch = _parameterCount == 0
            ? 1
            : Math.Max(1, MaxParametersPerCommand / _parameterCount);

        var maxBatchSize = Math.Min(_configuration.MaxBatchSize, maxRowsPerBatch);
        _flushThreshold = Math.Clamp(_configuration.BatchSize, 1, maxBatchSize);
        _pendingRows = new List<object?[]>(_flushThreshold);
        _insertSql = BuildInsertSql();
    }

    /// <summary>
    ///     Writes a single item to the batch buffer.
    ///     Flushes the buffer when it reaches the configured batch size.
    /// </summary>
    /// <param name="item">The item to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        _pendingRows.Add(GetValues(item));

        if (_pendingRows.Count >= _flushThreshold)
            await FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Writes a batch of items to the batch buffer.
    ///     Flushes the buffer after all items are added.
    /// </summary>
    /// <param name="items">The items to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task WriteBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        foreach (var item in items)
        {
            await WriteAsync(item, cancellationToken).ConfigureAwait(false);
        }

        if (_pendingRows.Count > 0)
            await FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Flushes the batch buffer to the database using a multi-row INSERT statement.
    ///     Supports transaction wrapping when UseTransaction is enabled.
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
                await ExecuteFlushAsync(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts - 1 && SqlServerExceptionHandler.ShouldRetry(ex, _configuration))
            {
                attempt++;
                var delay = SqlServerExceptionHandler.GetRetryDelay(ex, attempt, _configuration);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        // If we get here, all retries failed
        await ExecuteFlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Disposes the writer and flushes any remaining buffered items.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // Flush any buffered items before disposal
        await FlushAsync().ConfigureAwait(false);
    }

    /// <summary>
    ///     Executes the actual flush operation.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private async Task ExecuteFlushAsync(CancellationToken cancellationToken = default)
    {
        var valueClauses = new List<string>(_pendingRows.Count);
        var paramIndex = 0;

        await using var command = await _connection.CreateCommandAsync(cancellationToken).ConfigureAwait(false);
        command.CommandType = CommandType.Text;
        command.CommandTimeout = _configuration.CommandTimeout;

        foreach (var row in _pendingRows)
        {
            EnsureValueCount(row);

            var parameterNames = new string[_parameterCount];

            for (var i = 0; i < _parameterCount; i++)
            {
                var paramName = $"@p{paramIndex++}";
                parameterNames[i] = paramName;
                command.AddParameter(paramName, row[i] ?? DBNull.Value);
            }

            valueClauses.Add($"({string.Join(", ", parameterNames)})");
        }

        command.CommandText = _insertSql + string.Join(", ", valueClauses);

        _ = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        _pendingRows.Clear();
    }

    /// <summary>
    ///     Builds the INSERT SQL statement using square brackets for identifier quoting.
    /// </summary>
    /// <returns>The INSERT SQL statement.</returns>
    private string BuildInsertSql()
    {
        if (_parameterCount == 0)
            throw new InvalidOperationException($"Type '{typeof(T).Name}' does not expose any writable properties to persist.");

        var quotedTableName = QuoteIdentifier($"{_schema}.{_tableName}");

        var quotedColumns = _mappings
            .Select(m => QuoteIdentifier(m.ColumnName))
            .ToArray();

        return $"INSERT INTO {quotedTableName} ({string.Join(", ", quotedColumns)}) VALUES ";
    }

    /// <summary>
    ///     Quotes a SQL Server identifier using square brackets.
    /// </summary>
    /// <param name="identifier">The identifier to quote.</param>
    /// <returns>The quoted identifier.</returns>
    private static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("Identifier cannot be empty.", nameof(identifier));

        // Split schema.table and quote each part
        var parts = identifier.Split('.');
        var quotedParts = parts.Select(p => $"[{p}]");
        return string.Join(".", quotedParts);
    }

    /// <summary>
    ///     Gets parameters from an item using convention-based mapping or custom parameter mapper.
    /// </summary>
    /// <param name="item">The item.</param>
    /// <returns>The parameters.</returns>
    private object?[] GetValues(T item)
    {
        if (_parameterMapper == null)
            return _valueFactory(item);

        var mapped = _parameterMapper(item)?.ToArray() ?? Array.Empty<DatabaseParameter>();

        if (mapped.Length != _parameterCount)
        {
            throw new InvalidOperationException(
                $"Custom parameter mapper for '{typeof(T).Name}' must return exactly {_parameterCount} values to match the mapped columns.");
        }

        var values = new object?[_parameterCount];

        for (var i = 0; i < _parameterCount; i++)
        {
            values[i] = mapped[i].Value;
        }

        return values;
    }

    /// <summary>
    ///     Builds property mappings for the type, excluding identity columns.
    /// </summary>
    /// <returns>The property mappings.</returns>
    private static PropertyMapping[] BuildMappings()
    {
        return
        [
            .. typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanWrite && !IsIgnored(p) && !IsIdentity(p))
                .Select(p => new PropertyMapping(GetColumnName(p), BuildGetter(p))),
        ];
    }

    /// <summary>
    ///     Determines if a property should be ignored.
    /// </summary>
    /// <param name="property">The property.</param>
    /// <returns>True if the property should be ignored; otherwise, false.</returns>
    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var sqlServerAttribute = property.GetCustomAttribute<SqlServerColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true || sqlServerAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);
        return ignoredByAttribute || hasIgnoreMarker;
    }

    /// <summary>
    ///     Determines if a property is an identity column.
    /// </summary>
    /// <param name="property">The property.</param>
    /// <returns>True if the property is an identity column; otherwise, false.</returns>
    private static bool IsIdentity(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<SqlServerColumnAttribute>();
        return columnAttribute?.Identity == true;
    }

    /// <summary>
    ///     Gets the column name for a property based on attributes or convention.
    /// </summary>
    /// <param name="property">The property.</param>
    /// <returns>The column name.</returns>
    private static string GetColumnName(PropertyInfo property)
    {
        var sqlServerAttr = property.GetCustomAttribute<SqlServerColumnAttribute>();

        if (sqlServerAttr?.Name is { Length: > 0 } sqlName)
            return sqlName;

        var columnAttr = property.GetCustomAttribute<ColumnAttribute>();

        // SQL Server uses PascalCase by convention
        return columnAttr?.Name ?? property.Name;
    }

    /// <summary>
    ///     Builds a compiled getter function for a property.
    /// </summary>
    /// <param name="property">The property.</param>
    /// <returns>The getter function.</returns>
    private static Func<T, object?> BuildGetter(PropertyInfo property)
    {
        var instanceParam = Expression.Parameter(typeof(T), "item");
        var propertyAccess = Expression.Property(instanceParam, property);
        var convert = Expression.Convert(propertyAccess, typeof(object));
        return Expression.Lambda<Func<T, object?>>(convert, instanceParam).Compile();
    }

    /// <summary>
    ///     Builds a compiled value factory that extracts all property values.
    /// </summary>
    /// <param name="mappings">The property mappings.</param>
    /// <returns>The value factory.</returns>
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

    /// <summary>
    ///     Ensures that the value array has the expected number of elements.
    /// </summary>
    /// <param name="values">The value array.</param>
    /// <exception cref="SqlServerException">Thrown when the value count doesn't match.</exception>
    private void EnsureValueCount(object?[] values)
    {
        if (values.Length != _parameterCount)
        {
            throw new SqlServerException(
                $"Expected {_parameterCount} values for table '{_tableName}' but received {values.Length}.",
                null,
                false,
                new InvalidOperationException("Value count mismatch."));
        }
    }

    /// <summary>
    ///     Represents a property mapping for SQL Server.
    /// </summary>
    /// <param name="ColumnName">The column name.</param>
    /// <param name="Getter">The getter function.</param>
    private sealed record PropertyMapping(string ColumnName, Func<T, object?> Getter);
}
