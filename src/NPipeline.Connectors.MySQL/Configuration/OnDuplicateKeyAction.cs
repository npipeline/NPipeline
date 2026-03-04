namespace NPipeline.Connectors.MySql.Configuration;

/// <summary>
///     Specifies the action to take when a MySQL upsert operation encounters a duplicate key.
/// </summary>
public enum OnDuplicateKeyAction
{
    /// <summary>
    ///     Update the existing row with new values.
    ///     Generates: <c>INSERT … ON DUPLICATE KEY UPDATE col=VALUES(col), …</c>
    /// </summary>
    Update,

    /// <summary>
    ///     Silently skip duplicate rows without modifying existing data.
    ///     Generates: <c>INSERT IGNORE INTO …</c>
    /// </summary>
    Ignore,

    /// <summary>
    ///     Delete the existing row and insert a new one.
    ///     Generates: <c>REPLACE INTO …</c>
    ///     Warning: this is a delete + re-insert and resets AUTO_INCREMENT identity.
    /// </summary>
    Replace,
}
