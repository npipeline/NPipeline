namespace NPipeline.Connectors.DuckDB.Configuration;

/// <summary>
///     Access mode for the DuckDB database.
/// </summary>
public enum DuckDBAccessMode
{
    /// <summary>DuckDB determines the mode based on usage.</summary>
    Automatic,

    /// <summary>Read-only access — allows concurrent readers.</summary>
    ReadOnly,

    /// <summary>Read-write access — exclusive lock.</summary>
    ReadWrite,
}
