using NPipeline.Connectors.MongoDB.Configuration;

namespace NPipeline.Connectors.MongoDB.DependencyInjection;

/// <summary>
///     Configuration options for the MongoDB connector.
/// </summary>
public class MongoConnectorOptions
{
    /// <summary>
    ///     Gets or sets the default connection string.
    /// </summary>
    public string? DefaultConnectionString { get; set; }

    /// <summary>
    ///     Gets the named connections dictionary (key = name, value = connection string).
    /// </summary>
    public Dictionary<string, string> Connections { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Gets or sets the default configuration for source/sink nodes.
    /// </summary>
    public MongoConfiguration? DefaultConfiguration { get; set; }

    /// <summary>
    ///     Gets a connection string by name.
    /// </summary>
    /// <param name="name">The connection name. If null or empty, returns the default connection string.</param>
    /// <returns>The connection string.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the named connection is not found.</exception>
    public string? GetConnectionString(string? name = null)
    {
        return string.IsNullOrWhiteSpace(name)
            ? DefaultConnectionString
            : Connections.TryGetValue(name, out var connectionString)
                ? connectionString
                : throw new InvalidOperationException($"Named connection '{name}' not found.");
    }

    /// <summary>
    ///     Checks if a named connection exists.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <returns>True if the named connection exists; otherwise, false.</returns>
    public bool HasConnection(string name)
    {
        return string.IsNullOrWhiteSpace(name)
            ? !string.IsNullOrWhiteSpace(DefaultConnectionString)
            : Connections.ContainsKey(name);
    }

    /// <summary>
    ///     Adds or updates a named connection.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <param name="connectionString">The connection string.</param>
    public void AddOrUpdateConnection(string name, string connectionString)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Connection name cannot be empty.", nameof(name));

        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string cannot be empty.", nameof(connectionString));

        Connections[name] = connectionString;
    }

    /// <summary>
    ///     Removes a named connection.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <returns>True if the connection was removed; otherwise, false.</returns>
    public bool RemoveConnection(string name)
    {
        return Connections.Remove(name);
    }

    /// <summary>
    ///     Gets all configured connection names ("default" plus any named connections).
    /// </summary>
    /// <returns>An enumerable of connection names.</returns>
    public IEnumerable<string> GetConnectionNames()
    {
        if (!string.IsNullOrWhiteSpace(DefaultConnectionString))
            yield return "default";

        foreach (var key in Connections.Keys)
        {
            yield return key;
        }
    }
}
