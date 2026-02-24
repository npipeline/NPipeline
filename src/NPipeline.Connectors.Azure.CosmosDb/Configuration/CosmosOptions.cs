using Azure.Core;
using NPipeline.Connectors.Azure.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Configuration;

/// <summary>
///     Configuration options for the Cosmos DB connector.
///     Supports multiple connection configurations including connection strings and Azure AD authentication.
/// </summary>
public class CosmosOptions
{
    /// <summary>
    ///     Gets the Azure connection options that manage named connections and endpoints.
    /// </summary>
    public AzureConnectionOptions AzureConnections { get; } = new();

    /// <summary>
    ///     Gets or sets the default connection string.
    /// </summary>
    public string? DefaultConnectionString
    {
        get => AzureConnections.DefaultConnectionString;
        set => AzureConnections.DefaultConnectionString = value;
    }

    /// <summary>
    ///     Gets or sets the default Mongo API connection string.
    /// </summary>
    public string? DefaultMongoConnectionString { get; set; }

    /// <summary>
    ///     Gets or sets the default Cassandra connection options.
    /// </summary>
    public CassandraConnectionOptions? DefaultCassandraConnection { get; set; }

    /// <summary>
    ///     Gets or sets the default endpoint for Azure AD authentication.
    /// </summary>
    public Uri? DefaultEndpoint
    {
        get => AzureConnections.DefaultEndpoint?.Endpoint;
        set => AzureConnections.DefaultEndpoint = value != null
            ? new AzureEndpointOptions { Endpoint = value }
            : null;
    }

    /// <summary>
    ///     Gets or sets the default credential for Azure AD authentication.
    /// </summary>
    public TokenCredential? DefaultCredential
    {
        get => AzureConnections.DefaultEndpoint?.Credential;
        set
        {
            AzureConnections.DefaultEndpoint ??= new AzureEndpointOptions();
            AzureConnections.DefaultEndpoint.Credential = value;
        }
    }

    /// <summary>
    ///     Gets or sets the default configuration settings applied to all connections.
    /// </summary>
    public CosmosConfiguration? DefaultConfiguration { get; set; }

    /// <summary>
    ///     Gets the dictionary of named connection strings.
    /// </summary>
    public Dictionary<string, string> NamedConnections { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Gets the dictionary of named endpoints with Azure AD credentials.
    /// </summary>
    public Dictionary<string, CosmosEndpointOptions> NamedEndpoints { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Gets the dictionary of named Mongo API connection strings.
    /// </summary>
    public Dictionary<string, string> NamedMongoConnections { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Gets the dictionary of named Cassandra connection options.
    /// </summary>
    public Dictionary<string, CassandraConnectionOptions> NamedCassandraConnections { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    ///     Adds or updates a named connection string.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <param name="connectionString">The connection string.</param>
    /// <returns>This instance for fluent chaining.</returns>
    public CosmosOptions AddOrUpdateConnection(string name, string connectionString)
    {
        NamedConnections[name] = connectionString;
        AzureConnections.AddOrUpdateConnection(name, connectionString);
        return this;
    }

    /// <summary>
    ///     Adds or updates a named Mongo API connection string.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <param name="connectionString">The Mongo API connection string.</param>
    /// <returns>This instance for fluent chaining.</returns>
    public CosmosOptions AddOrUpdateMongoConnection(string name, string connectionString)
    {
        NamedMongoConnections[name] = connectionString;
        return this;
    }

    /// <summary>
    ///     Adds or updates a named Cassandra connection.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <param name="connection">The Cassandra connection options.</param>
    /// <returns>This instance for fluent chaining.</returns>
    public CosmosOptions AddOrUpdateCassandraConnection(string name, CassandraConnectionOptions connection)
    {
        NamedCassandraConnections[name] = connection;
        return this;
    }

    /// <summary>
    ///     Adds or updates a named endpoint with Azure AD credential.
    /// </summary>
    /// <param name="name">The connection name.</param>
    /// <param name="endpoint">The Cosmos DB endpoint.</param>
    /// <param name="credential">The token credential for authentication.</param>
    /// <returns>This instance for fluent chaining.</returns>
    public CosmosOptions AddOrUpdateEndpoint(string name, Uri endpoint, TokenCredential credential)
    {
        NamedEndpoints[name] = new CosmosEndpointOptions { Endpoint = endpoint, Credential = credential };
        AzureConnections.AddOrUpdateEndpoint(name, new AzureEndpointOptions { Endpoint = endpoint, Credential = credential });
        return this;
    }

    /// <summary>
    ///     Validates the options and throws if invalid.
    /// </summary>
    public void Validate()
    {
        if (string.IsNullOrEmpty(DefaultConnectionString) &&
            string.IsNullOrEmpty(DefaultMongoConnectionString) &&
            DefaultCassandraConnection == null &&
            DefaultEndpoint == null &&
            NamedConnections.Count == 0 &&
            NamedMongoConnections.Count == 0 &&
            NamedCassandraConnections.Count == 0 &&
            NamedEndpoints.Count == 0)
        {
            throw new InvalidOperationException(
                "At least one connection must be configured via SQL, Mongo, Cassandra, or endpoint options.");
        }
    }
}

/// <summary>
///     Options for Cosmos DB endpoint with Azure AD authentication.
/// </summary>
public class CosmosEndpointOptions
{
    /// <summary>
    ///     Gets or sets the Cosmos DB endpoint.
    /// </summary>
    public Uri? Endpoint { get; set; }

    /// <summary>
    ///     Gets or sets the token credential for authentication.
    /// </summary>
    public TokenCredential? Credential { get; set; }
}

/// <summary>
///     Options for Cosmos Cassandra API connections.
/// </summary>
public class CassandraConnectionOptions
{
    /// <summary>
    ///     Gets or sets the Cassandra contact point host.
    /// </summary>
    public string ContactPoint { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the Cassandra port.
    /// </summary>
    public int Port { get; set; } = 10350;

    /// <summary>
    ///     Gets or sets the Cassandra keyspace.
    /// </summary>
    public string Keyspace { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the Cassandra username.
    /// </summary>
    public string? Username { get; set; }

    /// <summary>
    ///     Gets or sets the Cassandra password.
    /// </summary>
    public string? Password { get; set; }
}
