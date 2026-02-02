namespace NPipeline.Connectors.SqlServer.Configuration;

/// <summary>
///     Actions to take when a MERGE statement encounters a match.
///     This feature is available in the commercial SQL Server connector.
/// </summary>
public enum OnMergeAction
{
    /// <summary>
    ///     Insert when no match is found.
    /// </summary>
    Insert,

    /// <summary>
    ///     Update when a match is found.
    /// </summary>
    Update,

    /// <summary>
    ///     Perform both insert (when no match) and update (when match) operations.
    /// </summary>
    InsertOrUpdate,

    /// <summary>
    ///     Delete when a match is found.
    /// </summary>
    Delete,
}
