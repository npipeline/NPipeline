namespace NPipeline.Connectors.Snowflake.Configuration;

/// <summary>
///     Specifies the action to take when a MERGE statement finds a matching row in the target table.
/// </summary>
public enum OnMergeAction
{
    /// <summary>
    ///     Do not modify the existing row when a match is found. Only new rows will be inserted.
    /// </summary>
    Ignore,

    /// <summary>
    ///     Update the existing row with values from the source when a match is found.
    ///     This is the standard "upsert" behavior - insert if not exists, update if exists.
    /// </summary>
    Update,

    /// <summary>
    ///     Delete the existing row when a match is found.
    /// </summary>
    Delete,
}
