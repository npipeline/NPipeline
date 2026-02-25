using System.Data;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Data.SqlClient;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Connection;
using NPipeline.Connectors.SqlServer.Exceptions;
using NPipeline.Connectors.SqlServer.Mapping;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.SqlServer.Writers;

/// <summary>
///     High-performance bulk copy write strategy for SQL Server using SqlBulkCopy API.
///     Provides maximum throughput for large data imports.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal sealed class SqlServerBulkCopyWriter<T> : IDatabaseWriter<T>
{
    private readonly SqlServerConfiguration _configuration;
    private readonly IDatabaseConnection _connection;
    private readonly int _flushThreshold;
    private readonly PropertyMapping[] _mappings;
    private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
    private readonly List<T> _pendingRows;
    private readonly string _schema;
    private readonly string _tableName;
    private readonly Func<T, object?[]> _valueFactory;

    /// <summary>
    ///     Initializes a new instance of <see cref="SqlServerBulkCopyWriter{T}" /> class.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="schema">The schema name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="parameterMapper">Optional parameter mapper function.</param>
    /// <param name="configuration">The configuration.</param>
    public SqlServerBulkCopyWriter(
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
        _valueFactory = BuildValueFactory(_mappings);
        _flushThreshold = Math.Clamp(_configuration.BulkCopyBatchSize, 1, _configuration.MaxBatchSize);
        _pendingRows = new List<T>(_flushThreshold);
    }

    /// <summary>
    ///     Writes a single item to the buffer.
    ///     Flushes the buffer when it reaches the configured batch size.
    /// </summary>
    /// <param name="item">The item to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        _pendingRows.Add(item);

        if (_pendingRows.Count >= _flushThreshold)
        {
            await FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Writes a batch of items using SqlBulkCopy.
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
            {
                await FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        if (_pendingRows.Count > 0)
        {
            await FlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Flushes the buffer to the database using SqlBulkCopy.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        if (_pendingRows.Count == 0)
        {
            return;
        }

        var attempt = 0;
        var maxAttempts = _configuration.MaxRetryAttempts + 1;

        while (attempt < maxAttempts)
        {
            try
            {
                await ExecuteBulkCopyAsync(cancellationToken).ConfigureAwait(false);
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
        await ExecuteBulkCopyAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Disposes the writer and flushes any remaining buffered items.
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
            System.Diagnostics.Debug.WriteLine(
                $"Warning: Failed to flush during disposal for SqlServerBulkCopyWriter<{typeof(T).Name}>: {ex.Message}");
        }
    }

    /// <summary>
    ///     Gets the underlying SqlConnection from the database connection.
    /// </summary>
    private SqlConnection GetSqlConnection()
    {
        if (_connection is SqlServerDatabaseConnection sqlServerConnection)
        {
            return sqlServerConnection.UnderlyingConnection;
        }

        throw new SqlServerException(
            $"Expected connection of type '{nameof(SqlServerDatabaseConnection)}' but got '{_connection.GetType().Name}'. " +
            "SqlBulkCopy operations require access to the underlying SqlConnection.",
            null,
            false,
            new InvalidOperationException("Invalid connection type for bulk copy operations."));
    }

    /// <summary>
    ///     Executes the bulk copy operation using SqlBulkCopy API.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    private async Task ExecuteBulkCopyAsync(CancellationToken cancellationToken)
    {
        var sqlConnection = GetSqlConnection();
        var sqlTransaction = GetSqlTransaction();
        var dataTable = BuildDataTable();

        // Pass transaction to SqlBulkCopy to enlist in active transaction for ExactlyOnce semantics
        using var bulkCopy = sqlTransaction != null
            ? new SqlBulkCopy(sqlConnection, SqlBulkCopyOptions.Default, sqlTransaction)
            : new SqlBulkCopy(sqlConnection);

        ConfigureBulkCopy(bulkCopy);
        ConfigureColumnMappings(bulkCopy);

        await bulkCopy.WriteToServerAsync(dataTable, cancellationToken).ConfigureAwait(false);

        _pendingRows.Clear();
    }

    /// <summary>
    ///     Gets the underlying SqlTransaction from the database connection if one is active.
    /// </summary>
    /// <returns>The active SqlTransaction, or null if no transaction is active.</returns>
    private SqlTransaction? GetSqlTransaction()
    {
        if (_connection is SqlServerDatabaseConnection sqlServerConnection)
        {
            return sqlServerConnection.UnderlyingTransaction;
        }

        return null;
    }

    /// <summary>
    ///     Configures the SqlBulkCopy instance with settings from configuration.
    /// </summary>
    /// <param name="bulkCopy">The SqlBulkCopy instance to configure.</param>
    private void ConfigureBulkCopy(SqlBulkCopy bulkCopy)
    {
        bulkCopy.DestinationTableName = $"[{_schema}].[{_tableName}]";
        bulkCopy.BatchSize = _configuration.BulkCopyBatchSize;
        bulkCopy.BulkCopyTimeout = _configuration.BulkCopyTimeout;
        bulkCopy.EnableStreaming = _configuration.EnableStreaming;

        // Configure notification for progress tracking if specified
        if (_configuration.BulkCopyNotifyAfter > 0)
        {
            bulkCopy.NotifyAfter = _configuration.BulkCopyNotifyAfter;
            bulkCopy.SqlRowsCopied += (_, e) =>
            {
                // Event handler for rows copied - can be used for logging/progress
                // This is a placeholder for potential future progress reporting
            };
        }
    }

    /// <summary>
    ///     Configures column mappings for the bulk copy operation.
    /// </summary>
    /// <param name="bulkCopy">The SqlBulkCopy instance to configure.</param>
    private void ConfigureColumnMappings(SqlBulkCopy bulkCopy)
    {
        for (var i = 0; i < _mappings.Length; i++)
        {
            var mapping = _mappings[i];
            bulkCopy.ColumnMappings.Add(i, mapping.ColumnName);
        }
    }

    /// <summary>
    ///     Builds a DataTable from the pending rows for use with SqlBulkCopy.
    /// </summary>
    /// <returns>A DataTable containing all pending rows.</returns>
    private DataTable BuildDataTable()
    {
        var dataTable = new DataTable();

        // Define columns based on property mappings
        foreach (var mapping in _mappings)
        {
            var columnType = mapping.PropertyType;
            var dataColumn = new DataColumn(mapping.ColumnName, columnType)
            {
                AllowDBNull = !mapping.PropertyType.IsValueType || Nullable.GetUnderlyingType(mapping.PropertyType) != null
            };
            dataTable.Columns.Add(dataColumn);
        }

        // Add rows from pending items
        foreach (var item in _pendingRows)
        {
            var values = GetValues(item);
            dataTable.Rows.Add(values);
        }

        return dataTable;
    }

    /// <summary>
    ///     Gets values from an item using convention-based mapping or custom parameter mapper.
    /// </summary>
    /// <param name="item">The item.</param>
    /// <returns>The values array.</returns>
    private object?[] GetValues(T item)
    {
        if (_parameterMapper == null)
        {
            return _valueFactory(item);
        }

        var mapped = _parameterMapper(item)?.ToArray() ?? Array.Empty<DatabaseParameter>();

        if (mapped.Length != _mappings.Length)
        {
            throw new InvalidOperationException(
                $"Custom parameter mapper for '{typeof(T).Name}' must return exactly {_mappings.Length} values to match the mapped columns.");
        }

        var values = new object?[_mappings.Length];

        for (var i = 0; i < _mappings.Length; i++)
        {
            values[i] = mapped[i].Value ?? DBNull.Value;
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
                .Select(p => new PropertyMapping(
                    GetColumnName(p),
                    BuildGetter(p),
                    p.PropertyType)),
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
        {
            return sqlName;
        }

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
                var value = mappings[i].Getter(item);
                values[i] = value ?? DBNull.Value;
            }

            return values;
        };
    }

    /// <summary>
    ///     Represents a property mapping for SQL Server bulk copy operations.
    /// </summary>
    /// <param name="ColumnName">The column name.</param>
    /// <param name="Getter">The getter function.</param>
    /// <param name="PropertyType">The property type.</param>
    private sealed record PropertyMapping(string ColumnName, Func<T, object?> Getter, Type PropertyType);
}
