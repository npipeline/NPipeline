namespace NPipeline.Connectors.DuckDB.Configuration;

/// <summary>
///     Options for configuring DuckDB connector in dependency injection.
/// </summary>
public sealed class DuckDBOptions
{
    /// <summary>
    ///     Named database configurations (name → configuration).
    ///     Keys are case-insensitive.
    /// </summary>
    public Dictionary<string, DuckDBConfiguration> NamedDatabases { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Default configuration applied to all nodes unless overridden.
    /// </summary>
    public DuckDBConfiguration DefaultConfiguration { get; set; } = new();

    /// <summary>
    ///     Gets a configuration by database name.
    /// </summary>
    /// <param name="name">The database name. If null or empty, returns the default configuration.</param>
    /// <returns>The configuration for the named database.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the named database is not found.</exception>
    public DuckDBConfiguration GetConfiguration(string? name = null)
    {
        if (string.IsNullOrWhiteSpace(name))
            return DefaultConfiguration;

        return NamedDatabases.TryGetValue(name, out var config)
            ? config
            : throw new InvalidOperationException($"Named database '{name}' not found.");
    }

    /// <summary>
    ///     Checks if a named database exists.
    /// </summary>
    public bool HasDatabase(string name)
    {
        return !string.IsNullOrWhiteSpace(name) && NamedDatabases.ContainsKey(name);
    }

    /// <summary>
    ///     Adds or updates a named database configuration.
    /// </summary>
    public void AddOrUpdateDatabase(string name, DuckDBConfiguration configuration)
    {
        if (string.IsNullOrWhiteSpace(name))
            throw new ArgumentException("Database name cannot be empty.", nameof(name));

        NamedDatabases[name] = configuration ?? throw new ArgumentNullException(nameof(configuration));
    }
}
