namespace NPipeline.Connectors.Snowflake.Configuration;

/// <summary>
///     Write strategies supported by the Snowflake sink.
/// </summary>
public enum SnowflakeWriteStrategy
{
    /// <summary>
    ///     Write each row individually using separate INSERT statements.
    /// </summary>
    PerRow,

    /// <summary>
    ///     Write rows using batched INSERT commands with multi-row VALUES clauses.
    ///     Up to 16,384 rows per statement (Snowflake limit).
    /// </summary>
    Batch,

    /// <summary>
    ///     Write rows using PUT file + COPY INTO for high-performance bulk loading.
    ///     Best for batches greater than 10,000 rows.
    /// </summary>
    StagedCopy,
}
