using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Exceptions;
using NPipeline.Connectors.Snowflake.Mapping;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Snowflake.Writers;

/// <summary>
///     Per-row write strategy for Snowflake.
///     Writes one row at a time using individual INSERT statements.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal sealed class SnowflakePerRowWriter<T> : IDatabaseWriter<T>
{
    private readonly SnowflakeConfiguration _configuration;
    private readonly IDatabaseConnection _connection;
    private readonly string _insertSql;
    private readonly PropertyMapping[] _mappings;
    private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
    private readonly string[] _parameterNames;
    private readonly string _schema;
    private readonly string _tableName;
    private readonly Func<T, object?[]> _valueFactory;

    /// <summary>
    ///     Initializes a new instance of <see cref="SnowflakePerRowWriter{T}" /> class.
    /// </summary>
    public SnowflakePerRowWriter(
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
        _parameterNames = BuildParameterNames(_mappings.Length);
        _valueFactory = BuildValueFactory(_mappings);
        _insertSql = BuildInsertSql();
    }

    /// <inheritdoc />
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        var attempt = 0;
        var maxAttempts = _configuration.MaxRetryAttempts + 1;

        while (attempt < maxAttempts)
        {
            try
            {
                await using var command = await _connection.CreateCommandAsync(cancellationToken);
                command.CommandText = _insertSql;
                command.CommandType = CommandType.Text;
                command.CommandTimeout = _configuration.CommandTimeout;

                var values = GetValues(item);

                for (var i = 0; i < values.Length; i++)
                {
                    command.AddParameter(_parameterNames[i], values[i] ?? DBNull.Value);
                }

                _ = await command.ExecuteNonQueryAsync(cancellationToken);
                return;
            }
            catch (Exception ex) when (attempt < maxAttempts - 1 && SnowflakeExceptionHandler.ShouldRetry(ex, _configuration))
            {
                attempt++;
                var delay = SnowflakeExceptionHandler.GetRetryDelay(ex, attempt, _configuration);
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }
        }

        // All retries failed - final attempt
        await using var finalCommand = await _connection.CreateCommandAsync(cancellationToken);
        finalCommand.CommandText = _insertSql;
        finalCommand.CommandType = CommandType.Text;
        finalCommand.CommandTimeout = _configuration.CommandTimeout;

        var finalValues = GetValues(item);

        for (var i = 0; i < finalValues.Length; i++)
        {
            finalCommand.AddParameter(_parameterNames[i], finalValues[i] ?? DBNull.Value);
        }

        _ = await finalCommand.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <inheritdoc />
    public async Task WriteBatchAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)
    {
        foreach (var item in items)
        {
            await WriteAsync(item, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await ValueTask.CompletedTask;
    }

    /// <summary>
    ///     Builds the INSERT SQL statement using double-quote identifier quoting (Snowflake convention).
    /// </summary>
    private string BuildInsertSql()
    {
        if (_mappings.Length == 0)
            throw new InvalidOperationException($"Type '{typeof(T).Name}' does not expose any writable properties to persist.");

        var quotedTableName = QuoteIdentifier($"{_schema}.{_tableName}");

        var quotedColumns = _mappings
            .Select(m => QuoteIdentifier(m.ColumnName))
            .ToArray();

        var paramList = string.Join(", ", _parameterNames);

        return $"INSERT INTO {quotedTableName} ({string.Join(", ", quotedColumns)}) VALUES ({paramList})";
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

    private static string[] BuildParameterNames(int count)
    {
        var names = new string[count];

        for (var i = 0; i < count; i++)
        {
            names[i] = $"@p{i}";
        }

        return names;
    }

    private sealed record PropertyMapping(string ColumnName, Func<T, object?> Getter);
}
