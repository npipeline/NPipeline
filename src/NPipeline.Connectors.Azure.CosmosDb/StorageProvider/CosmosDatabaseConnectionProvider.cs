using Microsoft.Azure.Cosmos;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;
using NPipeline.Connectors.Azure.CosmosDb.Connection;
using NPipeline.Connectors.Database;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Azure.CosmosDb.StorageProvider;

/// <summary>
///     Cosmos DB connection provider implementation.
///     Provides database connection management for Cosmos DB via StorageUri.
/// </summary>
/// <remarks>
///     This provider supports "cosmosdb" and "cosmos" URI schemes.
/// </remarks>
public sealed class CosmosDatabaseConnectionProvider : IDatabaseConnectionProvider
{
    private static readonly IReadOnlyList<StorageScheme> SupportedSchemeList = [new StorageScheme("cosmosdb"), new StorageScheme("cosmos"), new StorageScheme("cosmos-mongo"), new StorageScheme("cosmos-cassandra")];

    /// <summary>The URI schemes this provider handles.</summary>
    public IReadOnlyList<StorageScheme> Schemes => SupportedSchemeList;

    /// <summary>
    ///     Generates a Cosmos DB connection string from the specified <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing connection information.</param>
    /// <returns>A Cosmos DB connection string.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="uri" /> is null.</exception>
    /// <exception cref="ArgumentException">
    ///     If the URI is missing required components (e.g., account or key).
    /// </exception>
    public string GetConnectionString(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        if (uri.Scheme.Value == "cosmos-mongo")
        {
            var auth = string.IsNullOrWhiteSpace(uri.UserName)
                ? string.Empty
                : uri.Password is null
                    ? $"{Uri.EscapeDataString(uri.UserName)}@"
                    : $"{Uri.EscapeDataString(uri.UserName)}:{Uri.EscapeDataString(uri.Password)}@";

            var port = uri.Port.HasValue
                ? $":{uri.Port}"
                : string.Empty;

            var db = uri.Path.Trim('/');
            return $"mongodb://{auth}{uri.Host}{port}/{db}";
        }

        if (uri.Scheme.Value == "cosmos-cassandra")
        {
            var port = uri.Port.HasValue
                ? uri.Port.Value
                : 10350;

            var keyspace = uri.Path.Trim('/');
            return $"Contact Points={uri.Host};Port={port};Default Keyspace={keyspace};";
        }

        var info = ParseCosmosUri(uri);

        if (string.IsNullOrWhiteSpace(info.AccountKey) && string.IsNullOrWhiteSpace(info.Password))
        {
            throw new ArgumentException(
                "Cosmos DB URI must include account key in query parameter 'key' or password in userinfo.",
                nameof(uri));
        }

        return BuildConnectionString(new CosmosUriInfo
        {
            Endpoint = info.Endpoint,
            AccountKey = info.AccountKey ?? info.Password,
        });
    }

    /// <summary>
    ///     Opens a Cosmos DB database connection from the specified <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing connection information.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>
    ///     A task producing an <see cref="IDatabaseConnection" /> that can be used to interact with the database.
    /// </returns>
    /// <exception cref="ArgumentNullException">If <paramref name="uri" /> is null.</exception>
    /// <exception cref="ArgumentException">
    ///     If the URI is missing required components (e.g., account, database, or container).
    /// </exception>
    /// <exception cref="DatabaseConnectionException">
    ///     If the connection cannot be established due to network, authentication, or other database-specific errors.
    /// </exception>
    public async Task<IDatabaseConnection> OpenConnectionAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        if (uri.Scheme.Value is "cosmos-mongo" or "cosmos-cassandra")
        {
            throw new NotSupportedException(
                $"Scheme '{uri.Scheme.Value}' is supported by API adapters but not by {nameof(IDatabaseConnection)}-based nodes. " +
                "Use the registered ICosmosApiAdapterResolver with Mongo/Cassandra adapters for these APIs.");
        }

        var info = ParseCosmosUri(uri);

        var clientOptions = new CosmosClientOptions
        {
            ConnectionMode = ConnectionMode.Direct,
            SerializerOptions = new CosmosSerializationOptions
            {
                PropertyNamingPolicy = CosmosPropertyNamingPolicy.CamelCase,
            },
        };

        CosmosClient? client = null;

        try
        {
            if (!string.IsNullOrWhiteSpace(info.AccountKey))
            {
                // Use account key authentication
                var connectionString = BuildConnectionString(info);
                client = new CosmosClient(connectionString, clientOptions);
            }
            else if (!string.IsNullOrWhiteSpace(info.Username) && !string.IsNullOrWhiteSpace(info.Password))
            {
                // Use username/password as account/key
                var connectionString = $"AccountEndpoint={info.Endpoint};AccountKey={info.Password};";
                client = new CosmosClient(connectionString, clientOptions);
            }
            else
            {
                // Use Azure AD authentication (assumes Azure.Identity is configured)
                throw new NotSupportedException(
                    "Azure AD authentication requires explicit credential configuration. " +
                    "Use the connection pool with TokenCredential instead.");
            }

            // Verify the connection by reading database
            var database = client.GetDatabase(info.Database);
            await database.ReadAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

            var container = database.GetContainer(info.Container);

            return new CosmosDatabaseConnection(database, container, new CosmosConfiguration());
        }
        catch (CosmosException ex)
        {
            client?.Dispose();

            throw new DatabaseConnectionException(
                $"Failed to establish Cosmos DB connection to {uri.Host}/{uri.Path}. Error: {ex.Message}", ex);
        }
        catch (Exception ex) when (ex is not OperationCanceledException and not DatabaseConnectionException)
        {
            client?.Dispose();

            throw new DatabaseConnectionException(
                $"Failed to establish Cosmos DB connection to {uri.Host}/{uri.Path}.", ex);
        }
    }

    private static CosmosUriInfo ParseCosmosUri(StorageUri uri)
    {
        // Expected formats:
        // SQL: cosmosdb://account.documents.azure.com:443/database/container?key=...
        // Mongo: cosmos-mongo://user:pass@account.mongo.cosmos.azure.com:10255/database
        // Cassandra: cosmos-cassandra://account.cassandra.cosmos.azure.com:10350/keyspace

        if (string.IsNullOrWhiteSpace(uri.Host))
            throw new ArgumentException("Cosmos DB URI must include the account endpoint host.", nameof(uri));

        var pathParts = uri.Path.TrimStart('/').Split('/', StringSplitOptions.RemoveEmptyEntries);

        var requiresContainer = uri.Scheme.Value is "cosmosdb" or "cosmos";

        if ((requiresContainer && pathParts.Length < 2) || (!requiresContainer && pathParts.Length < 1))
        {
            throw new ArgumentException(
                requiresContainer
                    ? "Cosmos DB URI path must include database and container. Format: cosmosdb://account/database/container"
                    : "URI path must include database/keyspace. Format: cosmos-mongo://host/database or cosmos-cassandra://host/keyspace",
                nameof(uri));
        }

        var database = pathParts[0];

        var container = pathParts.Length > 1
            ? pathParts[1]
            : string.Empty;

        var info = new CosmosUriInfo
        {
            Endpoint = uri.Port.HasValue
                ? $"https://{uri.Host}:{uri.Port}"
                : $"https://{uri.Host}",
            Database = database,
            Container = container,
        };

        // Extract account key from query parameters
        if (uri.Parameters != null)
        {
            if (uri.Parameters.TryGetValue("key", out var key))
                info.AccountKey = key;
        }

        // Extract from userinfo if present (user:password format)
        if (!string.IsNullOrWhiteSpace(uri.UserName) && uri.Password is not null)
        {
            info.Username = uri.UserName;
            info.Password = uri.Password;
        }

        return info;
    }

    private static string BuildConnectionString(CosmosUriInfo info) => $"AccountEndpoint={info.Endpoint};AccountKey={info.AccountKey};";

    private sealed class CosmosUriInfo
    {
        public string Endpoint { get; set; } = string.Empty;
        public string Database { get; set; } = string.Empty;
        public string Container { get; set; } = string.Empty;
        public string? AccountKey { get; set; }
        public string? Username { get; set; }
        public string? Password { get; set; }
    }
}
