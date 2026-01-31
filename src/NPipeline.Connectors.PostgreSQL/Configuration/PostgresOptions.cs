namespace NPipeline.Connectors.PostgreSQL.Configuration;

/// <summary>
///     Options for configuring PostgreSQL connector in dependency injection.
/// </summary>
public class PostgresOptions
{
    /// <summary>
    ///     Gets or sets the default connection string. Optional when only named connections are used.
    /// </summary>
    public string DefaultConnectionString { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets named connection strings for multiple databases.
    ///     Keys are case-insensitive.
    /// </summary>
    public IDictionary<string, string> NamedConnections { get; set; } = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Gets or sets the default configuration for PostgreSQL operations.
    /// </summary>
    public PostgresConfiguration DefaultConfiguration { get; set; } = new();

    /// <summary>
    ///     Gets a connection string by name.
    /// </summary>
    /// <param name="name">The connection name. If null or empty, returns the default connection string.</param>
    /// <returns>The connection string.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the named connection is not found.</exception>
    public string GetConnectionString(string? name = null)
    {
        return string.IsNullOrWhiteSpace(name)
            ? DefaultConnectionString
            : NamedConnections.TryGetValue(name, out var connectionString)
                ? connectionString
                : throw new InvalidOperationException($"Named connection '{name}' not found.");
    }

    /// <summary>
    ///     Checks if a named connection exists.
    /// </summary>
    public bool HasConnection(string name)
    {
        return string.IsNullOrWhiteSpace(name)
            ? !string.IsNullOrWhiteSpace(DefaultConnectionString)
            : NamedConnections.ContainsKey(name);
    }

    /// <summary>
    ///     Adds or updates a named connection string.
    /// </summary>
    public void AddOrUpdateConnection(string name, string connectionString)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Connection name cannot be empty.", nameof(name));

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string cannot be empty.", nameof(connectionString));

        NamedConnections[name] = connectionString;
    }

    /// <summary>
    ///     Removes a named connection string.
    /// </summary>
    public bool RemoveConnection(string name)
    {
        return NamedConnections.Remove(name);
    }

    /// <summary>
    ///     Gets all configured connection names ("default" plus any named connections).
    /// </summary>
    public IEnumerable<string> GetConnectionNames()
    {
        if (!string.IsNullOrWhiteSpace(DefaultConnectionString))
            yield return "default";

        foreach (var key in NamedConnections.Keys)
        {
            yield return key;
        }
    }
}
