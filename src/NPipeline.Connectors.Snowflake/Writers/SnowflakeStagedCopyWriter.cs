using System.Data;
using System.Globalization;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Exceptions;
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
    private readonly List<object?[]> _pendingRows;
    private readonly ConnectionResilience _resilience;
    private readonly string _schema;
    private readonly string _tableName;
    private readonly Func<T, object?[]> _valueFactory;
    private readonly string _writerId = Guid.NewGuid().ToString("N")[..12];
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

        // One file name per flush, chosen outside the retried steps, so every retry uploads and loads the same file. The
        // writer id keeps two writers flushing in the same second from overwriting each other's file, or from having
        // Snowflake skip one as already loaded.
        var fileName = $"{_configuration.StageFilePrefix}{DateTime.UtcNow:yyyyMMddHHmmss}_{_writerId}_{_fileCounter++}.csv";
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

            await _resilience.RunAsync(ct => PutAsync(putSql, ct), cancellationToken).ConfigureAwait(false);

            // Step 3: COPY INTO target table from stage. Retried against the same staged file without uploading it again:
            // Snowflake's load metadata skips a file it has already loaded, so if an attempt loaded the rows and only its
            // reply was lost, the retry loads nothing twice. A new upload would be a new file and load them again.
            var copySql = BuildCopySql(stagePath);
            var copyAttempt = 0;
            await _resilience.RunAsync(ct => CopyAsync(copySql, stagePath, copyAttempt++ == 0, ct), cancellationToken).ConfigureAwait(false);
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

    /// <summary>
    ///     Runs the PUT and checks each file's status. The driver reports a file it could not upload, even after its own
    ///     upload retries, as a result row with status ERROR rather than as an exception; without this check COPY INTO would
    ///     find no file, load nothing, and the flush would be lost without an error.
    /// </summary>
    private async Task PutAsync(string sql, CancellationToken cancellationToken)
    {
        var rows = await QueryAsync(sql, cancellationToken).ConfigureAwait(false);

        foreach (var row in rows)
        {
            var status = row.GetValueOrDefault("status");

            if (string.Equals(status, "UPLOADED", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(status, "SKIPPED", StringComparison.OrdinalIgnoreCase))
                continue;

            // Not retried: the driver's upload retries have already run (see SnowflakeTransientErrorDetector.IsRetriedByDriver).
            throw new SnowflakeException(
                $"PUT of '{row.GetValueOrDefault("source")}' to the stage reported status '{status}': {row.GetValueOrDefault("message")}");
        }
    }

    /// <summary>
    ///     Runs COPY INTO and checks that it processed the staged file.
    /// </summary>
    /// <remarks>
    ///     Snowflake answers a COPY INTO that found nothing to load with a single row,
    ///     <c>
    ///         Copy executed with 0 files
    ///         processed.
    ///     </c>
    ///     , and no <c>file</c> column. On the first attempt that means the file just uploaded is missing, so
    ///     the rows would be lost: it fails. On a retry it is the expected answer when an earlier attempt loaded the file and
    ///     only its reply was lost, because load metadata then skips the file.
    /// </remarks>
    private async Task CopyAsync(string sql, string stagePath, bool firstAttempt, CancellationToken cancellationToken)
    {
        var rows = await QueryAsync(sql, cancellationToken).ConfigureAwait(false);

        if (firstAttempt && !rows.Any(r => !string.IsNullOrEmpty(r.GetValueOrDefault("file"))))
        {
            throw new SnowflakeException(
                $"COPY INTO from '{stagePath}' processed no files, so the staged rows were not loaded: " +
                $"{rows.Select(r => r.GetValueOrDefault("status")).FirstOrDefault() ?? "no result"}");
        }
    }

    private async Task<List<Dictionary<string, string?>>> QueryAsync(string sql, CancellationToken cancellationToken)
    {
        var command = await _connection.CreateCommandAsync(cancellationToken).ConfigureAwait(false);

        await using (command.ConfigureAwait(false))
        {
            command.CommandText = sql;
            command.CommandType = CommandType.Text;
            command.CommandTimeout = _configuration.CommandTimeout;

            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            await using (reader.ConfigureAwait(false))
            {
                var rows = new List<Dictionary<string, string?>>();

                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    var row = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);

                    for (var i = 0; i < reader.FieldCount; i++)
                    {
                        row[reader.GetName(i)] = reader.IsDBNull(i)
                            ? null
                            : Convert.ToString(reader.GetFieldValue<object>(i), CultureInfo.InvariantCulture);
                    }

                    rows.Add(row);
                }

                return rows;
            }
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
