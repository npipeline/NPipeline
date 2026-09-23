using System.Diagnostics;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using MySqlConnector;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Connection;
using NPipeline.Connectors.MySql.Exceptions;
using NPipeline.Connectors.MySql.Mapping;
using NPipeline.Connectors.MySql.Reliability;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using MySqlException = NPipeline.Connectors.MySql.Exceptions.MySqlException;

namespace NPipeline.Connectors.MySql.Writers;

/// <summary>
///     High-performance bulk load write strategy for MySQL using <see cref="MySqlBulkLoader" />.
///     Uses the LOAD DATA LOCAL INFILE protocol for maximum throughput.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal sealed class MySqlBulkLoadWriter<T> : IDatabaseWriter<T>
{
    private readonly MySqlConfiguration _configuration;
    private readonly IDatabaseConnection _connection;
    private readonly int _flushThreshold;
    private readonly PropertyMapping[] _mappings;
    private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
    private readonly ConnectionResilience _resilience;
    private readonly List<T> _pendingRows;
    private readonly string _tableName;
    private readonly Func<T, object?[]> _valueFactory;

    public MySqlBulkLoadWriter(
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
        _flushThreshold = Math.Clamp(_configuration.BulkLoadBatchSize, 1, _configuration.MaxBatchSize);
        _pendingRows = new List<T>(_flushThreshold);
        _resilience = new ConnectionResilience(_configuration.Resilience, _connection);
    }

    /// <inheritdoc />
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        _pendingRows.Add(item);

        if (_pendingRows.Count >= _flushThreshold)
            await FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task WriteBatchAsync(IEnumerable<T> items,
        CancellationToken cancellationToken = default)
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

    /// <inheritdoc />
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        if (_pendingRows.Count == 0)
            return;

        try
        {
            // LOAD DATA is one statement, which on a transactional engine commits every row or none, so it is safe to retry.
            await _resilience.RunAsync(ExecuteBulkLoadAsync, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Written, or reported to the caller as failed: either way the rows must not ride along in the next flush
            // or be sent again when the writer is disposed.
            _pendingRows.Clear();
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        try
        {
            await FlushAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Debug.WriteLine(
                $"Warning: Failed to flush during disposal for MySqlBulkLoadWriter<{typeof(T).Name}>: {ex.Message}");
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private async Task ExecuteBulkLoadAsync(CancellationToken ct)
    {
        var mysqlConnection = GetMySqlConnection();

        // Build the CSV-like data stream in memory
        var stream = BuildDataStream();
        await using var streamScope = stream.ConfigureAwait(false);

        var loader = new MySqlBulkLoader(mysqlConnection)
        {
            TableName = _tableName,
            SourceStream = stream,
            Local = true,
            CharacterSet = _configuration.CharacterSet ?? "utf8mb4",
            NumberOfLinesToSkip = 0,
            FieldTerminator = _configuration.FieldTerminator.ToString(),
            LineTerminator = _configuration.LineTerminator.ToString(),
            EscapeCharacter = _configuration.EscapeCharacter,
        };

        // Map columns in order
        loader.Columns.AddRange(_mappings.Select(m => m.ColumnName));

        if (_configuration.BulkLoadTimeout > 0)
            loader.Timeout = _configuration.BulkLoadTimeout;

        _ = await loader.LoadAsync(ct).ConfigureAwait(false);
    }

    private MemoryStream BuildDataStream()
    {
        var sb = new StringBuilder();

        foreach (var item in _pendingRows)
        {
            var values = GetValues(item);
            var fields = values.Select(v => EscapeField(v));
            sb.Append(string.Join(_configuration.FieldTerminator, fields));
            sb.Append(_configuration.LineTerminator);
        }

        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
        return new MemoryStream(bytes);
    }

    private string EscapeField(object? value)
    {
        if (value is null || value == DBNull.Value)
            return "\\N"; // MySQL NULL in LOAD DATA

        var str = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
        var escape = _configuration.EscapeCharacter;
        var fieldTerminator = _configuration.FieldTerminator;
        var lineTerminator = _configuration.LineTerminator;

        return str
            .Replace(escape.ToString(), $"{escape}{escape}")
            .Replace(fieldTerminator.ToString(), $"{escape}{fieldTerminator}")
            .Replace(lineTerminator.ToString(), $"{escape}{lineTerminator}");
    }

    private MySqlConnection GetMySqlConnection()
    {
        if (_connection is MySqlDatabaseConnection mysqlConn)
            return mysqlConn.UnderlyingConnection;

        throw new MySqlException(
            $"Expected connection of type '{nameof(MySqlDatabaseConnection)}' but got '{_connection.GetType().Name}'. " +
            "BulkLoad operations require access to the underlying MySqlConnection.",
            null,
            false,
            new InvalidOperationException("Invalid connection type for bulk load operations."));
    }

    private object?[] GetValues(T item)
    {
        if (_parameterMapper is null)
            return _valueFactory(item);

        var mapped = _parameterMapper(item)?.ToArray() ?? Array.Empty<DatabaseParameter>();

        if (mapped.Length != _mappings.Length)
        {
            throw new InvalidOperationException(
                $"Custom parameter mapper for '{typeof(T).Name}' must return exactly {_mappings.Length} values.");
        }

        return mapped.Select(p => p.Value).ToArray();
    }

    private static PropertyMapping[] BuildMappings()
    {
        return
        [
            .. typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanWrite && !IsIgnored(p) && !IsAutoIncrement(p))
                .Select(p => new PropertyMapping(GetColumnName(p), BuildGetter(p))),
        ];
    }

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

    private sealed record PropertyMapping(string ColumnName, Func<T, object?> Getter);
}
