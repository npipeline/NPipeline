using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Exceptions;
using NPipeline.Connectors.MySql.Mapping;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.MySql.Writers;

/// <summary>
///     Batch write strategy for MySQL.
///     Buffers rows and writes them in batches using multi-row INSERT statements.
///     Supports ON DUPLICATE KEY UPDATE, INSERT IGNORE, and REPLACE INTO for upsert operations.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal sealed class MySqlBatchWriter<T> : IDatabaseWriter<T>
{
    // MySQL max_allowed_packet guard: keep batches under ~64 MB.
    // We use parameter count as a simple proxy rather than byte estimation.
    private const int MaxParametersPerCommand = 65_000;

    private readonly MySqlConfiguration _configuration;
    private readonly IDatabaseConnection _connection;
    private readonly int _flushThreshold;
    private readonly string _insertPrefix;
    private readonly PropertyMapping[] _mappings;
    private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
    private readonly List<object?[]> _pendingRows;
    private readonly string _tableName;
    private readonly Func<T, object?[]> _valueFactory;

    public MySqlBatchWriter(
        IDatabaseConnection connection,
        string tableName,
        Func<T, IEnumerable<DatabaseParameter>>? parameterMapper,
        MySqlConfiguration configuration)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
        _configuration.Validate();
        _parameterMapper = parameterMapper;
        _mappings = BuildMappings();
        _valueFactory = BuildValueFactory(_mappings);

        var maxByParamLimit = _mappings.Length == 0 ? 1 : MaxParametersPerCommand / _mappings.Length;
        var maxBatch = Math.Min(_configuration.MaxBatchSize, maxByParamLimit);
        _flushThreshold = Math.Clamp(_configuration.BatchSize, 1, maxBatch);
        _pendingRows = new List<object?[]>(_flushThreshold);
        _insertPrefix = BuildInsertPrefix();
    }

    /// <inheritdoc />
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        _pendingRows.Add(GetValues(item));

        if (_pendingRows.Count >= _flushThreshold)
            await FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task WriteBatchAsync(IEnumerable<T> items,
        CancellationToken cancellationToken = default)
    {
        foreach (var item in items)
            await WriteAsync(item, cancellationToken).ConfigureAwait(false);

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

        while (attempt < maxAttempts - 1)
        {
            try
            {
                await ExecuteFlushAsync(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (MySqlExceptionHandler.ShouldRetry(ex, _configuration))
            {
                attempt++;
                var delay = MySqlExceptionHandler.GetRetryDelay(ex, attempt, _configuration);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        await ExecuteFlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await FlushAsync().ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private async Task ExecuteFlushAsync(CancellationToken ct)
    {
        var valueClauses = new List<string>(_pendingRows.Count);
        var paramIndex = 0;

        await using var command = await _connection.CreateCommandAsync(ct).ConfigureAwait(false);
        command.CommandType = CommandType.Text;
        command.CommandTimeout = _configuration.CommandTimeout;

        foreach (var row in _pendingRows)
        {
            var names = new string[_mappings.Length];
            for (var i = 0; i < _mappings.Length; i++)
            {
                var name = $"@p{paramIndex++}";
                names[i] = name;
                command.AddParameter(name, row[i] ?? DBNull.Value);
            }
            valueClauses.Add($"({string.Join(", ", names)})");
        }

        command.CommandText = BuildSql(valueClauses);
        _ = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        _pendingRows.Clear();
    }

    private string BuildSql(List<string> valueClauses)
    {
        var valuesBlock = string.Join(", ", valueClauses);

        if (!_configuration.UseUpsert)
            return $"{_insertPrefix} {valuesBlock}";

        return _configuration.OnDuplicateKeyAction switch
        {
            OnDuplicateKeyAction.Ignore => $"INSERT IGNORE INTO {QuoteIdentifier(_tableName)} ({BuildColumnList()}) VALUES {valuesBlock}",
            OnDuplicateKeyAction.Replace => $"REPLACE INTO {QuoteIdentifier(_tableName)} ({BuildColumnList()}) VALUES {valuesBlock}",
            OnDuplicateKeyAction.Update => BuildUpsertSql(valuesBlock),
            _ => $"{_insertPrefix} {valuesBlock}",
        };
    }

    private string BuildUpsertSql(string valuesBlock)
    {
        var sb = new StringBuilder();
        sb.Append($"INSERT INTO {QuoteIdentifier(_tableName)} ({BuildColumnList()}) VALUES {valuesBlock}");
        sb.Append(" ON DUPLICATE KEY UPDATE ");

        // Update all non-key columns
        var keySet = _configuration.UpsertKeyColumns?.ToHashSet(StringComparer.OrdinalIgnoreCase)
                     ?? new HashSet<string>();

        var updateClauses = _mappings
            .Where(m => !keySet.Contains(m.ColumnName))
            .Select(m => $"{QuoteIdentifier(m.ColumnName)}=VALUES({QuoteIdentifier(m.ColumnName)})");

        sb.Append(string.Join(", ", updateClauses));
        return sb.ToString();
    }

    private string BuildInsertPrefix() =>
        $"INSERT INTO {QuoteIdentifier(_tableName)} ({BuildColumnList()}) VALUES";

    private string BuildColumnList() =>
        string.Join(", ", _mappings.Select(m => QuoteIdentifier(m.ColumnName)));

    /// <summary>Quotes an identifier using MySQL backticks.</summary>
    private static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("Identifier cannot be empty.", nameof(identifier));

        return string.Join(".", identifier.Split('.').Select(p => $"`{p}`"));
    }

    private object?[] GetValues(T item)
    {
        if (_parameterMapper is null)
            return _valueFactory(item);

        var mapped = _parameterMapper(item)?.ToArray() ?? Array.Empty<DatabaseParameter>();

        if (mapped.Length != _mappings.Length)
            throw new InvalidOperationException(
                $"Custom parameter mapper for '{typeof(T).Name}' must return exactly {_mappings.Length} values.");

        return mapped.Select(p => p.Value).ToArray();
    }

    private static PropertyMapping[] BuildMappings() =>
    [
        .. typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && !IsIgnored(p) && !IsAutoIncrement(p))
            .Select(p => new PropertyMapping(GetColumnName(p), BuildGetter(p))),
    ];

    private static bool IsIgnored(PropertyInfo property)
    {
        var col = property.GetCustomAttribute<ColumnAttribute>();
        var mysqlCol = property.GetCustomAttribute<MySqlColumnAttribute>();
        return col?.Ignore == true || mysqlCol?.Ignore == true
               || property.IsDefined(typeof(IgnoreColumnAttribute), true);
    }

    private static bool IsAutoIncrement(PropertyInfo property)
    {
        var attr = property.GetCustomAttribute<MySqlColumnAttribute>();
        return attr?.AutoIncrement == true;
    }

    private static string GetColumnName(PropertyInfo property)
    {
        if (property.GetCustomAttribute<MySqlColumnAttribute>() is { } mySqlAttr
            && !string.IsNullOrEmpty(mySqlAttr.Name))
            return mySqlAttr.Name!;

        if (property.GetCustomAttribute<ColumnAttribute>() is { } colAttr
            && !string.IsNullOrEmpty(colAttr.Name))
            return colAttr.Name!;

        return property.Name;
    }

    private static Func<T, object?> BuildGetter(PropertyInfo property)
    {
        var param = Expression.Parameter(typeof(T), "item");
        var body = Expression.Convert(Expression.Property(param, property), typeof(object));
        return Expression.Lambda<Func<T, object?>>(body, param).Compile();
    }

    private static Func<T, object?[]> BuildValueFactory(IReadOnlyList<PropertyMapping> mappings) =>
        item =>
        {
            var values = new object?[mappings.Count];
            for (var i = 0; i < mappings.Count; i++)
                values[i] = mappings[i].Getter(item);
            return values;
        };

    private sealed record PropertyMapping(string ColumnName, Func<T, object?> Getter);
}
