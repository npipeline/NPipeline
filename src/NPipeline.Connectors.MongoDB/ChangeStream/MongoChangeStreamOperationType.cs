namespace NPipeline.Connectors.MongoDB.ChangeStream;

/// <summary>
///     Represents the type of operation that triggered a change stream event.
/// </summary>
public enum MongoChangeStreamOperationType
{
    /// <summary>
    ///     Document was inserted.
    /// </summary>
    Insert,

    /// <summary>
    ///     Document was updated.
    /// </summary>
    Update,

    /// <summary>
    ///     Document was replaced.
    /// </summary>
    Replace,

    /// <summary>
    ///     Document was deleted.
    /// </summary>
    Delete,

    /// <summary>
    ///     Change stream was invalidated (e.g., collection dropped).
    /// </summary>
    Invalidate,

    /// <summary>
    ///     Collection was dropped.
    /// </summary>
    Drop,

    /// <summary>
    ///     Database was dropped.
    /// </summary>
    DropDatabase,

    /// <summary>
    ///     Collection was renamed.
    /// </summary>
    Rename,
}
