using Cassandra;
using NPipeline.Connectors.Azure.CosmosDb.Abstractions;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Api.Cassandra;

/// <summary>
///     Cassandra API adapter for Cosmos Cassandra-compatible endpoints.
/// </summary>
public sealed class CosmosCassandraApiAdapter : ICosmosApiAdapter
{
    /// <inheritdoc />
    public CosmosApiType ApiType => CosmosApiType.Cassandra;

    /// <inheritdoc />
    public IReadOnlyCollection<string> SupportedSchemes { get; } = ["cosmos-cassandra"];

    /// <inheritdoc />
    public async Task<object> CreateClientAsync(CosmosConfiguration configuration, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var contactPoint = configuration.CassandraContactPoint;

        if (string.IsNullOrWhiteSpace(contactPoint))
        {
            if (Uri.TryCreate(configuration.AccountEndpoint, UriKind.Absolute, out var endpointUri))
                contactPoint = endpointUri.Host;
            else
                contactPoint = configuration.AccountEndpoint;
        }

        if (string.IsNullOrWhiteSpace(contactPoint))
            throw new InvalidOperationException("CassandraContactPoint or AccountEndpoint is required for Cassandra adapter.");

        if (string.IsNullOrWhiteSpace(configuration.DatabaseId))
            throw new InvalidOperationException("DatabaseId (keyspace) is required for Cassandra adapter.");

        var builder = Cluster.Builder()
            .AddContactPoint(contactPoint)
            .WithPort(configuration.CassandraPort);

        var username = configuration.CassandraUsername;
        var password = configuration.CassandraPassword;

        if (string.IsNullOrWhiteSpace(username) && !string.IsNullOrWhiteSpace(configuration.ConnectionString))
        {
            var parsed = ParseCassandraConnectionString(configuration.ConnectionString);
            username = parsed.Username ?? username;
            password = parsed.Password ?? password;
        }

        if (!string.IsNullOrWhiteSpace(username))
            builder.WithCredentials(username, password ?? string.Empty);

        var cluster = builder.Build();
        var session = await cluster.ConnectAsync(configuration.DatabaseId).WaitAsync(cancellationToken);
        return new CassandraClientContext(cluster, session);
    }

    /// <inheritdoc />
    public ICosmosSourceExecutor CreateSourceExecutor(object client, CosmosConfiguration configuration)
    {
        return new CosmosCassandraSourceExecutor(((CassandraClientContext)client).Session);
    }

    /// <inheritdoc />
    public ICosmosSinkExecutor<T> CreateSinkExecutor<T>(
        object client,
        CosmosConfiguration configuration,
        Func<T, string>? idSelector = null)
    {
        return new CosmosCassandraSinkExecutor<T>(((CassandraClientContext)client).Session, configuration);
    }

    private static (string? Username, string? Password) ParseCassandraConnectionString(string connectionString)
    {
        string? username = null;
        string? password = null;

        foreach (var segment in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var kv = segment.Split('=', 2, StringSplitOptions.TrimEntries);

            if (kv.Length != 2)
                continue;

            if (kv[0].Equals("username", StringComparison.OrdinalIgnoreCase) ||
                kv[0].Equals("user id", StringComparison.OrdinalIgnoreCase))
                username = kv[1];

            if (kv[0].Equals("password", StringComparison.OrdinalIgnoreCase))
                password = kv[1];
        }

        return (username, password);
    }
}
