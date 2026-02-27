namespace NPipeline.Connectors.Parquet;

/// <summary>
///     Controls compatibility behavior when the Parquet file schema and CLR model diverge.
/// </summary>
public enum SchemaCompatibilityMode
{
    /// <summary>
    ///     Strict mode: all mapped fields must exist in the file and types must match exactly.
    ///     Any schema mismatch results in an exception.
    /// </summary>
    Strict,

    /// <summary>
    ///     Additive mode: missing columns in the file map to default values for the CLR property.
    ///     Nullable properties may be set to null. Extra columns in the file are ignored.
    /// </summary>
    Additive,

    /// <summary>
    ///     Name-only mode: columns are matched by name only. Allows compatible type coercions
    ///     (e.g., int to long, float to double) with explicit conversion checks.
    /// </summary>
    NameOnly,
}
