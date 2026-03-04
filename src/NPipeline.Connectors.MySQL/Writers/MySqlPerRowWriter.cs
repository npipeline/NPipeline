using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Exceptions;
using NPipeline.Connectors.MySql.Mapping;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.MySql.Writers;

/// <summary>
///     Per-row write strategy for MySQL.
///     Writes one row at a time using individual INSERT statements.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal sealed class MySqlPerRowWriter<T> : IDatabaseWriter<T>
{
    private readonly MySqlConfiguration _configuration;
    private readonly IDatabaseConnection _connection;
    private readonly string _insertSql;
    private readonly PropertyMapping[] _mappings;
    private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
    private readonly string[] _parameterNames;
    private readonly string _tableName;
    private readonly Func<T, object?[]> _valueFactory;

    public MySqlPerRowWriter(
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
        _parameterNames = BuildParameterNames(_mappings.Length);
        _valueFactory = BuildValueFactory(_mappings);
        _insertSql = BuildInsertSql();
    }

    /// <inheritdoc />
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        var attempt = 0;
        var maxAttempts = _configuration.MaxRetryAttempts + 1;

        while (attempt < maxAttempts - 1)
        {
            try
            {
                await ExecuteWriteAsync(item, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (MySqlExceptionHandler.ShouldRetry(ex, _configuration))
            {
                attempt++;
                var delay = MySqlExceptionHandler.GetRetryDelay(ex, attempt, _configuration);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        // Final attempt — let exceptions propagate
        await ExecuteWriteAsync(item, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task WriteBatchAsync(IEnumerable<T> items,
        CancellationToken cancellationToken = default)
    {
        foreach (var item in items)
            await WriteAsync(item, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task FlushAsync(CancellationToken cancellationToken = default) =>
        Task.CompletedTask;

    /// <inheritdoc />
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private async Task ExecuteWriteAsync(T item, CancellationToken ct)
    {
        await using var command = await _connection.CreateCommandAsync(ct).ConfigureAwait(false);
        command.CommandText = _insertSql;
        command.CommandType = CommandType.Text;
        command.CommandTimeout = _configuration.CommandTimeout;

        var values = GetValues(item);
        for (var i = 0; i < values.Length; i++)
            command.AddParameter(_parameterNames[i], values[i] ?? DBNull.Value);

        _ = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private string BuildInsertSql()
    {
        if (_mappings.Length == 0)
            throw new InvalidOperationException(
                $"Type '{typeof(T).Name}' does not expose any writable properties to persist.");

        var quotedTable = QuoteIdentifier(_tableName);
        var columnList = string.Join(", ", _mappings.Select(m => QuoteIdentifier(m.ColumnName)));
        var paramList = string.Join(", ", _parameterNames);
        return $"INSERT INTO {quotedTable} ({columnList}) VALUES ({paramList})";
    }

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

    private static string[] BuildParameterNames(int count) =>
        Enumerable.Range(0, count).Select(i => $"@p{i}").ToArray();

    private sealed record PropertyMapping(string ColumnName, Func<T, object?> Getter);
}
