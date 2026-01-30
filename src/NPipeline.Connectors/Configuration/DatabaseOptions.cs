namespace NPipeline.Connectors.Configuration;

/// <summary>
///     Options for configuring database connectors in dependency injection.
/// </summary>
public class DatabaseOptions
{
    /// <summary>
    ///     Gets or sets the default connection string.
    /// </summary>
    public string DefaultConnectionString { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets named connection strings.
    /// </summary>
    public IDictionary<string, string> NamedConnections { get; set; } =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Gets a connection string by name.
    /// </summary>
    /// <param name="name">The name of the connection, or null for default.</param>
    /// <returns>The connection string.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the named connection is not found.</exception>
    public string GetConnectionString(string? name = null)
    {
        if (string.IsNullOrWhiteSpace(name))
            return DefaultConnectionString;

        if (NamedConnections.TryGetValue(name, out var connectionString))
            return connectionString;

        throw new InvalidOperationException($"Named connection '{name}' not found");
    }
}
