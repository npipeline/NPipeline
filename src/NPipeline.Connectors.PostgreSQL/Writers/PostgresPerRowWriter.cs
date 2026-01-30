using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Exceptions;
using NPipeline.Connectors.PostgreSQL.Configuration;
using NPipeline.Connectors.PostgreSQL.Mapping;
using NPipeline.Connectors.Utilities;

namespace NPipeline.Connectors.PostgreSQL.Writers;

/// <summary>
///     Per-row write strategy for PostgreSQL.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal sealed class PostgresPerRowWriter<T> : IDatabaseWriter<T>
{
    private readonly PostgresConfiguration _configuration;
    private readonly IDatabaseConnection _connection;
    private readonly string _insertSql;
    private readonly PropertyMapping[] _mappings;
    private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
    private readonly string[] _parameterNames;
    private readonly string _schema;
    private readonly string _tableName;
    private readonly Func<T, object?[]> _valueFactory;

    /// <summary>
    ///     Initializes a new instance of <see cref="PostgresPerRowWriter{T}" /> class.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="schema">The schema name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="parameterMapper">Optional parameter mapper function.</param>
    /// <param name="configuration">The configuration.</param>
    public PostgresPerRowWriter(
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
        _parameterNames = BuildParameterNames(_mappings.Length);
        _valueFactory = BuildValueFactory(_mappings);
        _insertSql = BuildInsertSql();
    }

    /// <summary>
    ///     Writes a single item to the database.
    /// </summary>
    /// <param name="item">The item to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
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
    }

    /// <summary>
    ///     Writes a batch of items to the database.
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
    }

    /// <summary>
    ///     Flushes any buffered data.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public Task FlushAsync(CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    ///     Disposes the writer.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // Connection is owned by the sink node, not the writer
        await ValueTask.CompletedTask;
    }

    /// <summary>
    ///     Builds the INSERT SQL statement.
    /// </summary>
    /// <returns>The INSERT SQL statement.</returns>
    private string BuildInsertSql()
    {
        if (_mappings.Length == 0)
            throw new InvalidOperationException($"Type '{typeof(T).Name}' does not expose any writable properties to persist.");

        var quotedTableName = DatabaseIdentifierValidator.QuoteIdentifier($"{_schema}.{_tableName}");

        var quotedColumns = _mappings
            .Select(m => ValidateAndQuoteIdentifier(m.ColumnName, nameof(m.ColumnName)))
            .ToArray();

        var paramList = string.Join(", ", _parameterNames);

        return $"INSERT INTO {quotedTableName} ({string.Join(", ", quotedColumns)}) VALUES ({paramList})";
    }

    private object?[] GetValues(T item)
    {
        if (_parameterMapper == null)
            return _valueFactory(item);

        var mapped = _parameterMapper(item)?.ToArray() ?? Array.Empty<DatabaseParameter>();

        if (mapped.Length != _mappings.Length)
            throw new InvalidOperationException(
                $"Custom parameter mapper for '{typeof(T).Name}' must return exactly {_mappings.Length} values to match the mapped columns.");

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
                .Where(p => p.CanWrite && !IsIgnored(p))
                .Select(p => new PropertyMapping(GetColumnName(p), BuildGetter(p))),
        ];
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<PostgresColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(PostgresIgnoreAttribute), true);
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

    private static string[] BuildParameterNames(int count)
    {
        var names = new string[count];

        for (var i = 0; i < count; i++)
        {
            names[i] = $"@p{i}";
        }

        return names;
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

    private sealed record PropertyMapping(string ColumnName, Func<T, object?> Getter);
}
