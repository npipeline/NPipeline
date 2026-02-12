namespace NPipeline.Connectors.Json;

/// <summary>
///     Specifies the naming policy for JSON property names when reading or writing.
/// </summary>
/// <remarks>
///     <para>
///         Different JSON APIs and conventions use different property naming styles.
///         The <see cref="JsonPropertyNamingPolicy" /> enum allows you to specify which
///         naming convention to use when mapping between JSON properties and .NET properties.
///     </para>
///     <para>
///         The default policy is <see cref="LowerCase" />, which aligns with the conventions
///         used by the CSV and Excel connectors for consistent behavior across different data formats.
///     </para>
/// </remarks>
public enum JsonPropertyNamingPolicy
{
    /// <summary>
    ///     Property names are converted to lowercase.
    ///     Example: <c>FirstName</c> → <c>firstname</c>
    /// </summary>
    /// <remarks>
    ///     This is the default policy for the JSON connector, providing consistency
    ///     with the CSV and Excel connectors which also use lowercase header/column names.
    /// </remarks>
    LowerCase,

    /// <summary>
    ///     Property names are converted to camelCase.
    ///     Example: <c>FirstName</c> → <c>firstName</c>
    /// </summary>
    /// <remarks>
    ///     This is the most common naming convention in JavaScript and JSON APIs.
    ///     The first letter is lowercase, and each subsequent word starts with an uppercase letter.
    /// </remarks>
    CamelCase,

    /// <summary>
    ///     Property names are converted to snake_case.
    ///     Example: <c>FirstName</c> → <c>first_name</c>
    /// </summary>
    /// <remarks>
    ///     This naming convention is commonly used in APIs and databases.
    ///     Words are separated by underscores and all letters are lowercase.
    /// </remarks>
    SnakeCase,

    /// <summary>
    ///     Property names are converted to PascalCase.
    ///     Example: <c>firstName</c> → <c>FirstName</c>
    /// </summary>
    /// <remarks>
    ///     This naming convention is commonly used in .NET and other C-style languages.
    ///     Each word starts with an uppercase letter and there are no separators.
    /// </remarks>
    PascalCase,

    /// <summary>
    ///     Property names are used as-is without any transformation.
    ///     Example: <c>FirstName</c> → <c>FirstName</c>
    /// </summary>
    /// <remarks>
    ///     This policy preserves the original property names exactly as they are defined
    ///     in the .NET type or as they appear in the JSON data.
    /// </remarks>
    AsIs,
}
