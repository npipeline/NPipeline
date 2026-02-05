using System.Text;

namespace NPipeline.StorageProviders.Utilities;

/// <summary>
///     Builder for constructing database connection strings.
/// </summary>
public static class DatabaseConnectionStringBuilder
{
    /// <summary>
    ///     Builds connection string from parameters.
    /// </summary>
    /// <param name="parameters">The connection parameters.</param>
    /// <returns>The connection string.</returns>
    public static string BuildConnectionString(IDictionary<string, string> parameters)
    {
        var builder = new StringBuilder();

        foreach (var kvp in parameters)
        {
            if (builder.Length > 0)
                builder.Append(';');

            builder.Append($"{kvp.Key}={EscapeValue(kvp.Value)}");
        }

        return builder.ToString();
    }

    /// <summary>
    ///     Parses connection string into parameters.
    /// </summary>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>The connection parameters.</returns>
    public static IDictionary<string, string> ParseConnectionString(string connectionString)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var part in connectionString.Split(';'))
        {
            var idx = part.IndexOf('=');

            if (idx > 0)
            {
                var key = part.Substring(0, idx).Trim();
                var value = UnescapeValue(part.Substring(idx + 1).Trim());
                result[key] = value;
            }
        }

        return result;
    }

    /// <summary>
    ///     Escapes value for connection string.
    /// </summary>
    /// <param name="value">The value to escape.</param>
    /// <returns>The escaped value.</returns>
    private static string EscapeValue(string value)
    {
        // Simple escaping - database-specific implementations may need more
        return value.Replace(";", "\\;");
    }

    /// <summary>
    ///     Unescapes value from connection string.
    /// </summary>
    /// <param name="value">The value to unescape.</param>
    /// <returns>The unescaped value.</returns>
    private static string UnescapeValue(string value)
    {
        return value.Replace("\\;", ";");
    }
}
