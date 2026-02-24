using System.Collections.Concurrent;

namespace NPipeline.Connectors.Azure.Configuration;

/// <summary>
///     Manages named connections for Azure services.
/// </summary>
public class AzureConnectionOptions
{
    private readonly ConcurrentDictionary<string, string> _namedConnections = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, AzureEndpointOptions> _namedEndpoints = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Gets or sets the default connection string.
    /// </summary>
    public string? DefaultConnectionString { get; set; }

    /// <summary>
    ///     Gets or sets the default endpoint with credential.
    /// </summary>
    public AzureEndpointOptions? DefaultEndpoint { get; set; }

    /// <summary>
    ///     Gets a named connection string by name.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <returns>The connection string, or null if not found.</returns>
    public string? GetConnectionString(string name)
    {
        return _namedConnections.TryGetValue(name, out var connectionString)
            ? connectionString
            : null;
    }

    /// <summary>
    ///     Adds or updates a named connection string.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <param name="connectionString">The connection string.</param>
    public void AddOrUpdateConnection(string name, string connectionString)
    {
        _namedConnections[name] = connectionString;
    }

    /// <summary>
    ///     Gets a named endpoint by name.
    /// </summary>
    /// <param name="name">The endpoint name.</param>
    /// <returns>The endpoint options, or null if not found.</returns>
    public AzureEndpointOptions? GetEndpoint(string name)
    {
        return _namedEndpoints.TryGetValue(name, out var endpoint)
            ? endpoint
            : null;
    }

    /// <summary>
    ///     Adds or updates a named endpoint.
    /// </summary>
    /// <param name="name">The endpoint name.</param>
    /// <param name="endpoint">The endpoint options.</param>
    public void AddOrUpdateEndpoint(string name, AzureEndpointOptions endpoint)
    {
        _namedEndpoints[name] = endpoint;
    }

    /// <summary>
    ///     Gets all named connection strings.
    /// </summary>
    /// <returns>A dictionary of named connections.</returns>
    public IReadOnlyDictionary<string, string> GetAllConnections()
    {
        return new Dictionary<string, string>(_namedConnections, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    ///     Gets all named endpoints.
    /// </summary>
    /// <returns>A dictionary of named endpoints.</returns>
    public IReadOnlyDictionary<string, AzureEndpointOptions> GetAllEndpoints()
    {
        return new Dictionary<string, AzureEndpointOptions>(_namedEndpoints, StringComparer.OrdinalIgnoreCase);
    }
}
