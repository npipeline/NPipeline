namespace NPipeline.Connectors.Aws.Sqs.Configuration;

/// <summary>
///     Specifies the naming policy for JSON property names when reading or writing.
/// </summary>
public enum JsonPropertyNamingPolicy
{
    /// <summary>
    ///     Property names are converted to lowercase.
    /// </summary>
    LowerCase,

    /// <summary>
    ///     Property names are converted to camelCase.
    /// </summary>
    CamelCase,

    /// <summary>
    ///     Property names are converted to snake_case.
    /// </summary>
    SnakeCase,

    /// <summary>
    ///     Property names are converted to PascalCase.
    /// </summary>
    PascalCase,

    /// <summary>
    ///     Property names are used as-is without any transformation.
    /// </summary>
    AsIs,
}
