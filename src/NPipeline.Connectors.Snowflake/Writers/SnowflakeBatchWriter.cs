using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Exceptions;
using NPipeline.Connectors.Snowflake.Mapping;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Snowflake.Writers;

/// <summary>
///     Batch write strategy for Snowflake.
///     Buffers rows and writes them in batches using multi-row INSERT statements or MERGE statements for upsert.
///     Snowflake supports up to 16,384 rows per multi-row INSERT.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal sealed class SnowflakeBatchWriter<T> : IDatabaseWriter<T>
{
    private readonly SnowflakeConfiguration _configuration;
    private readonly IDatabaseConnection _connection;
    private readonly int _flushThreshold;
    private readonly string _insertSql;
    private readonly PropertyMapping[] _mappings;
    private readonly string _mergeSqlTemplate;
    private readonly int _parameterCount;
    private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
    private readonly List<object?[]> _pendingRows;
    private readonly string _schema;
    private readonly string _tableName;
    private readonly Func<T, object?[]> _valueFactory;

    /// <summary>
    ///     Initializes a new instance of <see cref="SnowflakeBatchWriter{T}" /> class.
    /// </summary>
    public SnowflakeBatchWriter(
        IDatabaseConnection connection,
        string schema,
        string tableName,
        Func<T, IEnumerable<DatabaseParameter>>? parameterMapper,
        SnowflakeConfiguration configuration)
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

        // Snowflake limit: 16,384 rows per multi-row INSERT
        var maxBatchSize = Math.Min(_configuration.MaxBatchSize, 16384);
        _flushThreshold = Math.Clamp(_configuration.BatchSize, 1, maxBatchSize);
        _pendingRows = new List<object?[]>(_flushThreshold);
        _insertSql = BuildInsertSql();
        _mergeSqlTemplate = BuildMergeSqlTemplate();
    }

    /// <inheritdoc />
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        _pendingRows.Add(GetValues(item));

        if (_pendingRows.Count >= _flushThreshold)
            await FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task WriteBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        foreach (var item in items)
        {
            await WriteAsync(item, cancellationToken).ConfigureAwait(false);
        }

        if (_pendingRows.Count > 0)
            await FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
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
            catch (Exception ex) when (attempt < maxAttempts - 1 && SnowflakeExceptionHandler.ShouldRetry(ex, _configuration))
            {
                attempt++;
                var delay = SnowflakeExceptionHandler.GetRetryDelay(ex, attempt, _configuration);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        // All retries failed
        await ExecuteFlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await FlushAsync().ConfigureAwait(false);
    }

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

        command.CommandText = ShouldUseMerge()
            ? BuildMergeSql(valueClauses)
            : _insertSql + string.Join(", ", valueClauses);

        _ = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        _pendingRows.Clear();
    }

    private bool ShouldUseMerge()
    {
        return _configuration.UseUpsert
               && _configuration.UpsertKeyColumns != null
               && _configuration.UpsertKeyColumns.Length > 0;
    }

    /// <summary>
    ///     Builds the INSERT SQL statement using double-quote identifier quoting (Snowflake convention).
    /// </summary>
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
    ///     Builds the MERGE SQL template for upsert operations using Snowflake MERGE syntax.
    /// </summary>
    private string BuildMergeSqlTemplate()
    {
        if (!ShouldUseMerge())
            return string.Empty;

        if (_parameterCount == 0)
            throw new InvalidOperationException($"Type '{typeof(T).Name}' does not expose any writable properties to persist.");

        var quotedTableName = QuoteIdentifier($"{_schema}.{_tableName}");

        var quotedColumns = _mappings
            .Select(m => QuoteIdentifier(m.ColumnName))
            .ToArray();

        var keyColumns = _configuration.UpsertKeyColumns!
            .Select(QuoteIdentifier)
            .ToArray();

        var onClauses = keyColumns.Select(col => $"target.{col} = source.{col}");
        var onClause = string.Join(" AND ", onClauses);

        var sourceColumns = string.Join(", ", quotedColumns);

        var sb = new StringBuilder();
        sb.Append($"MERGE INTO {quotedTableName} AS target");
        sb.Append($" USING ({{0}}) AS source ({sourceColumns})");
        sb.Append($" ON {onClause}");

        switch (_configuration.OnMergeAction)
        {
            case OnMergeAction.Update:
                var updateColumns = quotedColumns
                    .Where(col => !keyColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                    .Select(col => $"{col} = source.{col}")
                    .ToArray();

                if (updateColumns.Length > 0)
                {
                    sb.Append(" WHEN MATCHED THEN UPDATE SET ");
                    sb.Append(string.Join(", ", updateColumns));
                }

                break;
            case OnMergeAction.Delete:
                sb.Append(" WHEN MATCHED THEN DELETE");
                break;
            case OnMergeAction.Ignore:
            default:
                break;
        }

        var insertColumns = string.Join(", ", quotedColumns);
        var insertValues = quotedColumns.Select(col => $"source.{col}");
        sb.Append($" WHEN NOT MATCHED THEN INSERT ({insertColumns}) VALUES ({string.Join(", ", insertValues)})");

        return sb.ToString();
    }

    private string BuildMergeSql(List<string> valueClauses)
    {
        var valuesSection = $"VALUES {string.Join(", ", valueClauses)}";
        return string.Format(_mergeSqlTemplate, valuesSection);
    }

    /// <summary>
    ///     Quotes a Snowflake identifier using double quotes.
    /// </summary>
    private static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("Identifier cannot be empty.", nameof(identifier));

        var parts = identifier.Split('.');
        var quotedParts = parts.Select(p => $"\"{p}\"");
        return string.Join(".", quotedParts);
    }

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

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var snowflakeAttribute = property.GetCustomAttribute<SnowflakeColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true || snowflakeAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);
        return ignoredByAttribute || hasIgnoreMarker;
    }

    private static bool IsIdentity(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<SnowflakeColumnAttribute>();
        return columnAttribute?.Identity == true;
    }

    private static string GetColumnName(PropertyInfo property)
    {
        var snowflakeAttr = property.GetCustomAttribute<SnowflakeColumnAttribute>();

        if (snowflakeAttr?.Name is { Length: > 0 } sfName)
            return sfName;

        var columnAttr = property.GetCustomAttribute<ColumnAttribute>();

        return columnAttr?.Name ?? SnowflakeNamingConvention.ToDefaultColumnName(property.Name);
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

    private void EnsureValueCount(object?[] values)
    {
        if (values.Length != _parameterCount)
        {
            throw new SnowflakeException(
                $"Expected {_parameterCount} values for table '{_tableName}' but received {values.Length}.",
                null,
                false,
                new InvalidOperationException("Value count mismatch."));
        }
    }

    private sealed record PropertyMapping(string ColumnName, Func<T, object?> Getter);
}
