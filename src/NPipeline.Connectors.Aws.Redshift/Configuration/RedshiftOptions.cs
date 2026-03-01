namespace NPipeline.Connectors.Aws.Redshift.Configuration;

/// <summary>Extended options with named connection support for DI scenarios.</summary>
public class RedshiftOptions : RedshiftConfiguration
{
    private readonly Dictionary<string, string> _namedConnections = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Gets or sets the default connection string.</summary>
    public string? DefaultConnectionString { get; set; }

    /// <summary>Gets or sets the default configuration.</summary>
    public RedshiftConfiguration DefaultConfiguration { get; set; } = new();

    /// <summary>Gets the dictionary of named connections.</summary>
    public IReadOnlyDictionary<string, string> NamedConnections => _namedConnections;

    /// <summary>Adds or updates a named connection string.</summary>
    /// <param name="name">The name of the connection.</param>
    /// <param name="connectionString">The connection string.</param>
    public void AddOrUpdateConnection(string name, string connectionString)
    {
        _namedConnections[name] = connectionString;
    }

    /// <summary>Tries to get a named connection string.</summary>
    /// <param name="name">The name of the connection.</param>
    /// <param name="connectionString">The connection string if found.</param>
    /// <returns>True if the connection was found; otherwise, false.</returns>
    public bool TryGetConnection(string name, out string? connectionString)
    {
        return _namedConnections.TryGetValue(name, out connectionString);
    }

    /// <summary>Gets the names of all configured connections.</summary>
    /// <returns>An enumerable of connection names.</returns>
    public IEnumerable<string> GetConnectionNames()
    {
        return _namedConnections.Keys;
    }
}
