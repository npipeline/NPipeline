namespace NPipeline.Connectors.DeliverySemantics;

/// <summary>
///     Modes for implementing exactly-once semantics.
/// </summary>
public enum ExactlyOnceMode
{
    /// <summary>
    ///     Single-phase transactional writes within one database transaction.
    /// </summary>
    Transactional,

    /// <summary>
    ///     Two-phase commit across resources (reserved for future use).
    /// </summary>
    TwoPhaseCommit,

    /// <summary>
    ///     Idempotent operations using upsert or deduplication keys.
    /// </summary>
    Idempotent,
}
