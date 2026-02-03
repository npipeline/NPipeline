namespace NPipeline.Connectors.SqlServer.Configuration;

/// <summary>
///     Write strategies supported by the SQL Server sink.
/// </summary>
public enum SqlServerWriteStrategy
{
    /// <summary>
    ///     Write each row individually using separate INSERT statements.
    /// </summary>
    PerRow,

    /// <summary>
    ///     Write rows using batched INSERT commands.
    /// </summary>
    Batch,

    /// <summary>
    ///     Write rows using SqlBulkCopy for high-performance bulk loading.
    ///     This feature is available in the commercial SQL Server connector.
    /// </summary>
    BulkCopy,
}
