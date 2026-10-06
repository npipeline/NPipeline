using System.Collections.Frozen;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Database;

/// <summary>
///     Contains parsed database connection information extracted from a <see cref="StorageUri" />.
/// </summary>
/// <param name="Host">The database server host name or IP address.</param>
/// <param name="Port">The database server port number (optional).</param>
/// <param name="Database">The database name.</param>
/// <param name="Username">The database username (optional).</param>
/// <param name="Password">The database password (optional).</param>
/// <param name="Parameters">Additional connection parameters as key-value pairs.</param>
public record DatabaseConnectionInfo(
    string Host,
    int? Port,
    string Database,
    string? Username,
    string? Password,
    IReadOnlyDictionary<string, string> Parameters);

/// <summary>
///     Provides utility methods for parsing <see cref="StorageUri" /> instances into database connection information.
/// </summary>
/// <remarks>
///     Parsing logic:
///     - Database name is extracted from <see cref="StorageUri.Path" /> by trimming the leading '/'.
///     - Username and password are preferred from query parameters ("username"/"user" and "password"/"pwd"),
///     falling back to <see cref="StorageUri.UserName" /> and <see cref="StorageUri.Password" />.
///     - Port is preferred from <see cref="StorageUri.Port" />, falling back to the "port" query parameter.
///     - Host is required and must be specified in <see cref="StorageUri.Host" />.
/// </remarks>
public static class DatabaseUriParser
{
    private static readonly FrozenSet<string> HandledParameterKeys = new[]
    {
        "username", "user", "password", "pwd", "port",
    }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Parses a <see cref="StorageUri" /> into a <see cref="DatabaseConnectionInfo" /> instance.
    /// </summary>
    /// <param name="uri">The storage URI to parse.</param>
    /// <returns>
    ///     A <see cref="DatabaseConnectionInfo" /> containing the parsed connection information.
    /// </returns>
    /// <exception cref="ArgumentNullException">If <paramref name="uri" /> is null.</exception>
    /// <exception cref="ArgumentException">
    ///     If the URI is missing required components (host or database name).
    /// </exception>
    public static DatabaseConnectionInfo Parse(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        // Validate required components
        if (string.IsNullOrWhiteSpace(uri.Host))
            throw new ArgumentException("Database URI must specify a host.", nameof(uri));

        // Extract database name from path (trim leading '/')
        var database = ExtractDatabaseName(uri.Path);

        if (string.IsNullOrWhiteSpace(database))
            throw new ArgumentException("Database URI must specify a database name in the path.", nameof(uri));

        // Extract username and password
        var (username, password) = ExtractCredentials(uri);

        // Extract port
        var port = ExtractPort(uri);

        return new DatabaseConnectionInfo(
            uri.Host,
            port,
            database,
            username,
            password,
            uri.Parameters);
    }

    /// <summary>
    ///     Determines whether the specified parameter key is already handled by <see cref="Parse" /> and should be
    ///     skipped when building a connection string from the remaining parameters.
    /// </summary>
    /// <param name="key">The parameter key.</param>
    /// <returns>True if the key is already handled; otherwise, false.</returns>
    public static bool IsHandledParameter(string key) => HandledParameterKeys.Contains(key);

    private static string ExtractDatabaseName(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return string.Empty;

        // Trim leading/trailing '/' and return the rest as the database name
        return path.Trim('/');
    }

    private static (string? Username, string? Password) ExtractCredentials(StorageUri uri)
    {
        // Prefer query parameters over the user information
        var username = GetParameter(uri, "username") ?? GetParameter(uri, "user");
        var password = GetParameter(uri, "password") ?? GetParameter(uri, "pwd");

        // Fall back to the user information if not found in parameters
        if (string.IsNullOrWhiteSpace(username) && !string.IsNullOrWhiteSpace(uri.UserName))
        {
            username = uri.UserName;

            if (uri.Password is not null)
                password = uri.Password;
        }

        return (username, password);
    }

    private static int? ExtractPort(StorageUri uri)
    {
        // Prefer Port property, fall back to "port" parameter
        if (uri.Port.HasValue)
            return uri.Port.Value;

        var portParam = GetParameter(uri, "port");

        if (!string.IsNullOrWhiteSpace(portParam) && int.TryParse(portParam, out var port))
            return port;

        return null;
    }

    private static string? GetParameter(StorageUri uri, string key)
    {
        if (uri.Parameters.TryGetValue(key, out var value))
            return value;

        return null;
    }
}
