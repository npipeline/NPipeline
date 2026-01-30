namespace NPipeline.Connectors.PostgreSQL.Configuration;

/// <summary>
///     Actions to take when an INSERT encounters a conflict.
/// </summary>
public enum OnConflictAction
{
    /// <summary>
    ///     Update non-conflict columns using values from <c>EXCLUDED</c>.
    /// </summary>
    Update,

    /// <summary>
    ///     Ignore the conflicting row (no-op).
    /// </summary>
    Ignore,
}
