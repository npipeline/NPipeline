namespace NPipeline.Connectors.PostgreSQL.Configuration;

/// <summary>
///     Write strategies supported by the PostgreSQL sink.
/// </summary>
public enum PostgresWriteStrategy
{
    /// <summary>
    ///     Write each row individually.
    /// </summary>
    PerRow,

    /// <summary>
    ///     Write rows using batched INSERT commands.
    /// </summary>
    Batch,

    /// <summary>
    ///     Write rows using the PostgreSQL COPY protocol.
    /// </summary>
    Copy,
}
