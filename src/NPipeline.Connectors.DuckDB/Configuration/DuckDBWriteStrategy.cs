namespace NPipeline.Connectors.DuckDB.Configuration;

/// <summary>
///     Write strategy for DuckDB sink nodes.
/// </summary>
public enum DuckDBWriteStrategy
{
    /// <summary>
    ///     Uses DuckDB's native Appender API for maximum throughput.
    ///     Does NOT support upsert/conflict resolution.
    ///     Rows are appended in bulk with minimal overhead.
    /// </summary>
    Appender,

    /// <summary>
    ///     Uses parameterized INSERT statements with optional batching.
    ///     Supports INSERT OR REPLACE for upsert scenarios.
    ///     Slower than Appender but more flexible.
    /// </summary>
    Sql,
}
