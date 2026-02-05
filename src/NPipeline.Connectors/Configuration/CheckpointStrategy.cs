namespace NPipeline.Connectors.Configuration;

/// <summary>
///     Checkpoint strategies for recovery.
/// </summary>
public enum CheckpointStrategy
{
    /// <summary>
    ///     No checkpointing - data may be lost on failure.
    /// </summary>
    None,

    /// <summary>
    ///     Offset-based checkpointing.
    /// </summary>
    Offset,

    /// <summary>
    ///     Key-based checkpointing.
    /// </summary>
    KeyBased,

    /// <summary>
    ///     Cursor-based checkpointing.
    /// </summary>
    Cursor,

    /// <summary>
    ///     Change Data Capture checkpointing.
    /// </summary>
    CDC,

    /// <summary>
    ///     In-memory checkpointing - checkpoints are stored in memory and lost on process restart.
    ///     Provides recovery from transient failures during pipeline execution.
    /// </summary>
    InMemory,
}
