using NPipeline.Connectors.DuckDB.Exceptions;

namespace NPipeline.Connectors.DuckDB.Configuration;

/// <summary>
///     Configuration for DuckDB source and sink nodes.
/// </summary>
public sealed class DuckDBConfiguration
{
    // --- Connection ---

    /// <summary>
    ///     Path to the DuckDB database file, or null/empty for in-memory.
    ///     Used by DI factory methods to resolve connection details.
    /// </summary>
    public string? DatabasePath { get; set; }

    /// <summary>
    ///     Access mode for the database. Default: <see cref="DuckDBAccessMode.Automatic" />.
    /// </summary>
    public DuckDBAccessMode AccessMode { get; set; } = DuckDBAccessMode.Automatic;

    /// <summary>
    ///     Maximum memory DuckDB may use (e.g., "4GB"). Null uses the DuckDB default (80% of RAM).
    /// </summary>
    public string? MemoryLimit { get; set; }

    /// <summary>
    ///     Number of threads DuckDB uses for execution. 0 = auto-detect.
    /// </summary>
    public int Threads { get; set; }

    /// <summary>
    ///     Temp directory for spilling. Null uses the DuckDB default.
    /// </summary>
    public string? TempDirectory { get; set; }

    /// <summary>
    ///     DuckDB extensions to load before executing commands (e.g., "httpfs", "spatial").
    /// </summary>
    public string[]? Extensions { get; set; }

    /// <summary>
    ///     Session-level settings applied via SET key = value (e.g., S3 credentials, pragmas).
    /// </summary>
    public Dictionary<string, string>? Settings { get; set; }

    // --- Read options ---

    /// <summary>
    ///     Stream results row-by-row instead of materializing. Default: true.
    /// </summary>
    public bool StreamResults { get; set; } = true;

    /// <summary>
    ///     Number of rows to fetch per internal batch when streaming. Default: 2048 (one vector).
    /// </summary>
    public int FetchSize { get; set; } = 2048;

    /// <summary>
    ///     Columns to project (pushed down to query). Null = all columns.
    /// </summary>
    public string[]? ProjectedColumns { get; set; }

    /// <summary>
    ///     Command timeout in seconds. Default: 30.
    /// </summary>
    public int CommandTimeout { get; set; } = 30;

    /// <summary>
    ///     Enable DuckDB query progress tracking. Default: false.
    /// </summary>
    public bool EnableProgressTracking { get; set; }

    // --- Write options ---

    /// <summary>
    ///     Write strategy: Appender (bulk, fastest) or Sql (INSERT, supports upsert).
    ///     Default: <see cref="DuckDBWriteStrategy.Appender" />.
    /// </summary>
    public DuckDBWriteStrategy WriteStrategy { get; set; } = DuckDBWriteStrategy.Appender;

    /// <summary>
    ///     Batch size for SQL write strategy. Ignored for Appender. Default: 1000.
    /// </summary>
    public int BatchSize { get; set; } = 1000;

    /// <summary>
    ///     Auto-create the target table from the CLR type if it does not exist. Default: true.
    /// </summary>
    public bool AutoCreateTable { get; set; } = true;

    /// <summary>
    ///     Truncate target table before writing. Default: false.
    /// </summary>
    public bool TruncateBeforeWrite { get; set; }

    /// <summary>
    ///     Wrap the entire write in a transaction. Default: true.
    /// </summary>
    public bool UseTransaction { get; set; } = true;

    /// <summary>
    ///     File format options for COPY TO export (compression, delimiter, etc.).
    /// </summary>
    public DuckDBFileExportOptions? FileExportOptions { get; set; }

    // --- Mapping ---

    /// <summary>
    ///     Use case-insensitive column name matching. Default: true.
    /// </summary>
    public bool CaseInsensitiveMapping { get; set; } = true;

    /// <summary>
    ///     Cache compiled mapping delegates. Default: true.
    /// </summary>
    public bool CacheMappingMetadata { get; set; } = true;

    // --- Error handling ---

    /// <summary>
    ///     Called when a row fails to map. Return true to skip the row, false to throw.
    ///     Parameters: (exception, rowIndex).
    /// </summary>
    public Func<Exception, long, bool>? RowErrorHandler { get; set; }

    /// <summary>
    ///     Continue processing remaining rows on error. Default: false.
    /// </summary>
    public bool ContinueOnError { get; set; }

    // --- Observability ---

    /// <summary>
    ///     Observer for metrics and diagnostics callbacks.
    /// </summary>
    public IDuckDBConnectorObserver? Observer { get; set; }

    /// <summary>
    ///     Validates this configuration, throwing <see cref="DuckDBConnectorException" /> if invalid.
    /// </summary>
    public void Validate()
    {
        if (BatchSize < 1)
            throw new DuckDBConnectorException("BatchSize must be at least 1.");

        if (CommandTimeout < 0)
            throw new DuckDBConnectorException("CommandTimeout cannot be negative.");

        if (FetchSize < 1)
            throw new DuckDBConnectorException("FetchSize must be at least 1.");

        if (Threads < 0)
            throw new DuckDBConnectorException("Threads cannot be negative.");

        if (MemoryLimit is not null && !IsValidMemoryLimit(MemoryLimit))
        {
            throw new DuckDBConnectorException(
                $"Invalid MemoryLimit '{MemoryLimit}'. Use a format like '4GB', '512MB', or '1073741824'.");
        }
    }

    private static bool IsValidMemoryLimit(string value)
    {
        if (long.TryParse(value, out _))
            return true;

        var trimmed = value.Trim();
        var suffixes = new[] { "B", "KB", "MB", "GB", "TB" };

        foreach (var suffix in suffixes)
        {
            if (trimmed.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
            {
                var numPart = trimmed[..^suffix.Length].Trim();
                return double.TryParse(numPart, out var num) && num > 0;
            }
        }

        return false;
    }
}
