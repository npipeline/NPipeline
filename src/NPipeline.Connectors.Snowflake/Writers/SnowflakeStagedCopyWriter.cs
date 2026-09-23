using System.Data;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Mapping;
using NPipeline.Connectors.Snowflake.Reliability;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Snowflake.Writers;

/// <summary>
///     Staged copy write strategy for Snowflake.
///     Uses PUT to upload data files to a Snowflake internal stage,
///     then COPY INTO to load the data into the target table.
///     Provides the highest throughput for large data volumes.
/// </summary>
/// <typeparam name="T">The type of objects to write.</typeparam>
internal sealed class SnowflakeStagedCopyWriter<T> : IDatabaseWriter<T>
{
    private readonly SnowflakeConfiguration _configuration;
    private readonly IDatabaseConnection _connection;
    private readonly PropertyMapping[] _mappings;
    private readonly Func<T, IEnumerable<DatabaseParameter>>? _parameterMapper;
    private readonly ConnectionResilience _resilience;
    private readonly List<object?[]> _pendingRows;
    private readonly string _schema;
    private readonly string _tableName;
    private readonly Func<T, object?[]> _valueFactory;
    private int _fileCounter;

    /// <summary>
    ///     Initializes a new instance of <see cref="SnowflakeStagedCopyWriter{T}" /> class.
    /// </summary>
    public SnowflakeStagedCopyWriter(
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
        _valueFactory = BuildValueFactory(_mappings);
        _pendingRows = new List<object?[]>(_configuration.BatchSize);
        _resilience = new ConnectionResilience(_configuration.Resilience, _connection);
    }

    /// <inheritdoc />
    public async Task WriteAsync(T item, CancellationToken cancellationToken = default)
    {
        _pendingRows.Add(GetValues(item));

        if (_pendingRows.Count >= _configuration.BatchSize)
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

        // One file name per flush, chosen outside the retried steps, so every retry uploads and loads the same file.
        var fileName = $"{_configuration.StageFilePrefix}{DateTime.UtcNow:yyyyMMddHHmmss}_{_fileCounter++}.csv";
        var tempFilePath = Path.Combine(Path.GetTempPath(), fileName);

        var stagePath = _configuration.StageName == "~"
            ? $"@~/{fileName}"
            : $"@{_configuration.StageName}/{fileName}";

        try
        {
            // Step 1: Write CSV data to temp file
            await WriteCsvFileAsync(tempFilePath, cancellationToken).ConfigureAwait(false);

            // Step 2: PUT file to Snowflake internal stage. This writes nothing to the table, and OVERWRITE=TRUE lets a
            // retry replace a partial upload, so it is retried on its own.
            var putSql =
                $"PUT 'file://{tempFilePath.Replace("\\", "/")}' '{stagePath}' AUTO_COMPRESS={(_configuration.CopyCompression != "NONE" ? "TRUE" : "FALSE")} OVERWRITE=TRUE";

            await _resilience.RunAsync(ct => ExecuteNonQueryAsync(putSql, ct), cancellationToken).ConfigureAwait(false);

            // Step 3: COPY INTO target table from stage. Retried against the same staged file without uploading it again:
            // Snowflake's load metadata skips a file it has already loaded, so if an attempt loaded the rows and only its
            // reply was lost, the retry loads nothing twice. A new upload would be a new file and load them again.
            var copySql = BuildCopySql(stagePath);
            await _resilience.RunAsync(ct => ExecuteNonQueryAsync(copySql, ct), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            // Loaded, or reported to the caller as failed: either way the rows must not ride along in the next flush or
            // be sent again when the writer is disposed.
            _pendingRows.Clear();

            // Clean up temp file
            if (File.Exists(tempFilePath))
            {
                try
                {
                    File.Delete(tempFilePath);
                }
                catch
                {
                    // Best-effort cleanup
                }
            }
        }
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await FlushAsync().ConfigureAwait(false);
    }

    private async Task ExecuteNonQueryAsync(string sql, CancellationToken cancellationToken)
    {
        var command = await _connection.CreateCommandAsync(cancellationToken).ConfigureAwait(false);

        await using (command.ConfigureAwait(false))
        {
            command.CommandText = sql;
            command.CommandType = CommandType.Text;
            command.CommandTimeout = _configuration.CommandTimeout;
            _ = await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private string BuildCopySql(string stagePath)
    {
        var quotedTableName = QuoteIdentifier($"{_schema}.{_tableName}");
        var quotedColumns = _mappings.Select(m => QuoteIdentifier(m.ColumnName)).ToArray();

        var copySql = new StringBuilder();
        copySql.Append($"COPY INTO {quotedTableName} ({string.Join(", ", quotedColumns)})");
        copySql.Append($" FROM '{stagePath}'");
        copySql.Append($" FILE_FORMAT = (TYPE = '{_configuration.FileFormat}'");

        if (_configuration.FileFormat.Equals("CSV", StringComparison.OrdinalIgnoreCase))
            copySql.Append(" FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 0 ESCAPE_UNENCLOSED_FIELD = NONE");

        if (!string.IsNullOrWhiteSpace(_configuration.CopyCompression) &&
            !_configuration.CopyCompression.Equals("NONE", StringComparison.OrdinalIgnoreCase))
            copySql.Append($" COMPRESSION = '{_configuration.CopyCompression}'");

        copySql.Append(')');
        copySql.Append($" ON_ERROR = '{_configuration.OnErrorAction}'");
        copySql.Append($" PURGE = {(_configuration.PurgeAfterCopy ? "TRUE" : "FALSE")}");
        return copySql.ToString();
    }

    /// <summary>
    ///     Writes pending rows to a CSV file for PUT upload.
    /// </summary>
    private async Task WriteCsvFileAsync(string filePath, CancellationToken cancellationToken)
    {
        var writer = new StreamWriter(filePath, false, Encoding.UTF8);
        await using var writerScope = writer.ConfigureAwait(false);

        foreach (var row in _pendingRows)
        {
            var fields = new string[row.Length];

            for (var i = 0; i < row.Length; i++)
            {
                fields[i] = FormatCsvField(row[i]);
            }

            await writer.WriteLineAsync(string.Join(",", fields)).ConfigureAwait(false);
        }
    }

    /// <summary>
    ///     Formats a value as a CSV field with proper escaping.
    /// </summary>
    private static string FormatCsvField(object? value)
    {
        if (value is null or DBNull)
            return string.Empty;

        var str = value switch
        {
            DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss.fff", CultureInfo.InvariantCulture),
            DateTimeOffset dto => dto.ToString("yyyy-MM-dd HH:mm:ss.fff zzz", CultureInfo.InvariantCulture),
            decimal d => d.ToString(CultureInfo.InvariantCulture),
            double d => d.ToString(CultureInfo.InvariantCulture),
            float f => f.ToString(CultureInfo.InvariantCulture),
            bool b => b
                ? "TRUE"
                : "FALSE",
            byte[] bytes => Convert.ToBase64String(bytes),
            _ => value.ToString() ?? string.Empty,
        };

        // Escape CSV: if contains comma, quote, or newline, wrap in quotes and double any existing quotes
        if (str.Contains(',') || str.Contains('"') || str.Contains('\n') || str.Contains('\r'))
            return $"\"{str.Replace("\"", "\"\"")}\"";

        return str;
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

    private sealed record PropertyMapping(string ColumnName, Func<T, object?> Getter);
}
