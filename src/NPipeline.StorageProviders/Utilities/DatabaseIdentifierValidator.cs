using System.Text.RegularExpressions;

namespace NPipeline.StorageProviders.Utilities;

/// <summary>
///     Provides validation for database identifiers.
/// </summary>
public static class DatabaseIdentifierValidator
{
    private static readonly Regex ValidIdentifierRegex = new(@"^[a-zA-Z_][a-zA-Z0-9_]*$",
        RegexOptions.Compiled);

    /// <summary>
    ///     Determines whether the specified identifier is valid.
    /// </summary>
    /// <param name="identifier">The identifier to validate.</param>
    /// <returns>True if the identifier is valid; otherwise, false.</returns>
    public static bool IsValidIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            return false;

        return ValidIdentifierRegex.IsMatch(identifier);
    }

    /// <summary>
    ///     Validates the specified identifier and throws an exception if invalid.
    /// </summary>
    /// <param name="identifier">The identifier to validate.</param>
    /// <param name="paramName">The parameter name.</param>
    /// <exception cref="ArgumentException">Thrown when the identifier is invalid.</exception>
    public static void ValidateIdentifier(string identifier, string paramName)
    {
        if (!IsValidIdentifier(identifier))
        {
            throw new ArgumentException(
                $"'{identifier}' is not a valid database identifier. " +
                "Identifiers must start with a letter or underscore and contain only letters, numbers, and underscores.",
                paramName);
        }
    }

    /// <summary>
    ///     Quotes the specified identifier with the specified quote character.
    /// </summary>
    /// <param name="identifier">The identifier to quote.</param>
    /// <param name="quoteChar">The quote character to use.</param>
    /// <returns>The quoted identifier.</returns>
    public static string QuoteIdentifier(string identifier, string quoteChar = "\"")
    {
        return $"{quoteChar}{identifier.Replace(quoteChar, quoteChar + quoteChar)}{quoteChar}";
    }
}
