namespace NPipeline.Connectors.MongoDB.Configuration;

/// <summary>
///     Defines the action to take when a duplicate key is encountered during write operations.
/// </summary>
public enum OnDuplicateAction
{
    /// <summary>
    ///     Ignores the duplicate and continues with the next document.
    /// </summary>
    Ignore,

    /// <summary>
    ///     Overwrites the existing document with the new data.
    /// </summary>
    Overwrite,

    /// <summary>
    ///     Fails the entire batch operation when a duplicate is encountered.
    /// </summary>
    Fail,
}
