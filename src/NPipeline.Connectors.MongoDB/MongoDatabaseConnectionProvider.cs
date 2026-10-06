using System.Collections.Frozen;
using MongoDB.Driver;
using NPipeline.Connectors.Database;
using NPipeline.Connectors.MongoDB.Connection;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.MongoDB;

/// <summary>
///     MongoDB connection provider implementation.
/// </summary>
public class MongoDatabaseConnectionProvider : IDatabaseConnectionProvider
{
    /// <summary>
    ///     The MongoDB URI schemes supported by this provider.
    /// </summary>
    public static readonly string[] SupportedSchemes = ["mongodb", "mongodb+srv"];

    /// <summary>
    ///     NPipeline-specific URI parameters that carry pipeline metadata and must not be
    ///     forwarded to the MongoDB driver as connection-string options.
    /// </summary>
    private static readonly FrozenSet<string> NPipelineParams =
        new[] { "collection", "table", "database" }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    private static readonly IReadOnlyList<StorageScheme> SupportedSchemeList = [new StorageScheme("mongodb"), new StorageScheme("mongodb+srv")];

    /// <summary>The URI schemes this provider handles.</summary>
    public IReadOnlyList<StorageScheme> Schemes => SupportedSchemeList;

    /// <summary>
    ///     Determines whether this provider can handle the specified storage URI.
    /// </summary>
    /// <param name="uri">The storage URI to check.</param>
    /// <returns>True if the URI uses a MongoDB scheme; otherwise, false.</returns>
    private static bool CanHandle(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        return SupportedSchemes.Any(s => string.Equals(uri.Scheme.Value, s, StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    ///     Generates a MongoDB connection string from the specified storage URI.
    /// </summary>
    /// <param name="uri">The storage URI containing connection information.</param>
    /// <returns>A MongoDB connection string.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="uri" /> is null.</exception>
    /// <exception cref="ArgumentException">If the URI scheme is not supported.</exception>
    public string GetConnectionString(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        if (!CanHandle(uri))
            throw new ArgumentException($"Unsupported storage scheme '{uri.Scheme.Value}'. Expected one of: {string.Join(", ", SupportedSchemes)}");

        // Reconstruct the connection string, stripping NPipeline-specific parameters
        // (e.g. 'collection') that the MongoDB driver would reject as unknown options.
        var scheme = uri.Scheme.Value;

        var userInfo = string.IsNullOrWhiteSpace(uri.UserName)
            ? ""
            : uri.Password is null
                ? $"{Uri.EscapeDataString(uri.UserName)}@"
                : $"{Uri.EscapeDataString(uri.UserName)}:{Uri.EscapeDataString(uri.Password)}@";

        var host = uri.Host ?? "localhost";

        var port = uri.Port > 0
            ? $":{uri.Port}"
            : "";

        var path = uri.Path ?? "";

        var mongoParams = uri.Parameters
            .Where(p => !NPipelineParams.Contains(p.Key))
            .ToDictionary(p => p.Key, p => p.Value);

        var query = mongoParams.Count > 0
            ? $"?{SerializeParameters(mongoParams)}"
            : "";

        return $"{scheme}://{userInfo}{host}{port}{path}{query}";
    }

    /// <summary>
    ///     Opens a MongoDB database connection from the specified storage URI.
    /// </summary>
    /// <param name="uri">The storage URI containing connection information.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing an <see cref="IDatabaseConnection" /> for MongoDB operations.</returns>
    public Task<IDatabaseConnection> OpenConnectionAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        if (!CanHandle(uri))
            throw new ArgumentException($"Unsupported storage scheme '{uri.Scheme.Value}'. Expected one of: {string.Join(", ", SupportedSchemes)}");

        var connectionString = GetConnectionString(uri);
        var client = MongoConnectionFactory.CreateClient(connectionString);
        var connection = new MongoDatabaseConnection(client);

        return Task.FromResult<IDatabaseConnection>(connection);
    }

    private static string SerializeParameters(IReadOnlyDictionary<string, string> parameters)
    {
        if (parameters.Count == 0)
            return string.Empty;

        var parts = new List<string>(parameters.Count);

        foreach (var kvp in parameters)
        {
            var k = Uri.EscapeDataString(kvp.Key);
            var v = Uri.EscapeDataString(kvp.Value);
            parts.Add($"{k}={v}");
        }

        return string.Join("&", parts);
    }

    /// <summary>
    ///     MongoDB database connection wrapper implementing <see cref="IDatabaseConnection" />.
    /// </summary>
    private sealed class MongoDatabaseConnection : IDatabaseConnection
    {
        private readonly IMongoClient _client;
        private bool _disposed;

        public MongoDatabaseConnection(IMongoClient client)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
        }

        public bool IsOpen => true; // MongoDB client is always "open" - connections are managed internally

        public IDatabaseTransaction? CurrentTransaction => null; // MongoDB transactions are handled differently

        public async Task OpenAsync(CancellationToken cancellationToken = default)
        {
            // MongoDB client doesn't require explicit open - just verify connectivity
            using var cursor = await _client.ListDatabaseNamesAsync(cancellationToken).ConfigureAwait(false);

            // Just enumerate to verify connection works
            while (await cursor.MoveNextAsync(cancellationToken).ConfigureAwait(false))
            {
            }
        }

        public Task CloseAsync(CancellationToken cancellationToken = default) =>

            // MongoDB client doesn't require explicit close
            Task.CompletedTask;

        public Task<IDatabaseTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default) =>
            throw new NotSupportedException(
                "MongoDB transactions require a session. Use the IMongoClient directly for transaction support.");

        public Task<IDatabaseCommand> CreateCommandAsync(CancellationToken cancellationToken = default) =>
            throw new NotSupportedException(
                "MongoDB commands are database-specific. Use the IMongoDatabase/IMongoCollection APIs directly.");

        public ValueTask DisposeAsync()
        {
            if (_disposed)
                return ValueTask.CompletedTask;

            _disposed = true;
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }
}
