using MongoDB.Driver;
using NPipeline.Connectors.MongoDB.Connection;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.MongoDB;

/// <summary>
///     MongoDB storage provider that implements the database storage provider interface.
///     Allows MongoDB connectors to work with the storage provider abstraction.
/// </summary>
public class MongoDatabaseStorageProvider : IDatabaseStorageProvider
{
    /// <summary>
    ///     The MongoDB URI schemes supported by this provider.
    /// </summary>
    public static readonly string[] SupportedSchemes = ["mongodb", "mongodb+srv"];

    /// <summary>
    ///     NPipeline-specific URI parameters that carry pipeline metadata and must not be
    ///     forwarded to the MongoDB driver as connection-string options.
    /// </summary>
    private static readonly HashSet<string> _npipelineParams =
        new(StringComparer.OrdinalIgnoreCase) { "collection", "table", "database" };

    /// <summary>
    ///     Gets the primary URI scheme for this provider.
    /// </summary>
    public StorageScheme Scheme => new("mongodb");

    /// <summary>
    ///     Determines whether this provider can handle the specified storage URI.
    /// </summary>
    /// <param name="uri">The storage URI to check.</param>
    /// <returns>True if the URI uses a MongoDB scheme; otherwise, false.</returns>
    public bool CanHandle(StorageUri uri)
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

        var userInfo = !string.IsNullOrWhiteSpace(uri.UserInfo)
            ? $"{uri.UserInfo}@"
            : "";

        var host = uri.Host ?? "localhost";

        var port = uri.Port > 0
            ? $":{uri.Port}"
            : "";

        var path = uri.Path ?? "";

        var mongoParams = uri.Parameters
            .Where(p => !_npipelineParams.Contains(p.Key))
            .ToDictionary(p => p.Key, p => p.Value);

        var query = mongoParams.Count > 0
            ? $"?{SerializeParameters(mongoParams)}"
            : "";

        return $"{scheme}://{userInfo}{host}{port}{path}{query}";
    }

    /// <summary>
    ///     Creates a MongoDB database connection from the specified storage URI.
    /// </summary>
    /// <param name="uri">The storage URI containing connection information.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing an <see cref="IDatabaseConnection" /> for MongoDB operations.</returns>
    public Task<IDatabaseConnection> GetConnectionAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        if (!CanHandle(uri))
            throw new ArgumentException($"Unsupported storage scheme '{uri.Scheme.Value}'. Expected one of: {string.Join(", ", SupportedSchemes)}");

        var connectionString = GetConnectionString(uri);
        var client = MongoConnectionFactory.CreateClient(connectionString);
        var connection = new MongoDatabaseConnection(client);

        return Task.FromResult<IDatabaseConnection>(connection);
    }

    /// <summary>
    ///     Opens a readable stream for the specified storage URI.
    ///     Note: This is not typically used for MongoDB operations.
    /// </summary>
    /// <param name="uri">The storage location to read from.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a readable stream.</returns>
    /// <exception cref="NotSupportedException">Always thrown as MongoDB doesn't support stream-based reads.</exception>
    public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException(
            "MongoDB does not support stream-based reads. Use GetConnectionAsync() to obtain a database connection.");
    }

    /// <summary>
    ///     Opens a writable stream for the specified storage URI.
    ///     Note: This is not typically used for MongoDB operations.
    /// </summary>
    /// <param name="uri">The storage location to write to.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a writable stream.</returns>
    /// <exception cref="NotSupportedException">Always thrown as MongoDB doesn't support stream-based writes.</exception>
    public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException(
            "MongoDB does not support stream-based writes. Use GetConnectionAsync() to obtain a database connection.");
    }

    /// <summary>
    ///     Checks whether a resource exists at the specified storage URI.
    /// </summary>
    /// <param name="uri">The storage location to check.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>True if the database/collection exists; otherwise, false.</returns>
    public async Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        if (!CanHandle(uri))
            return false;

        var connectionString = GetConnectionString(uri);
        var client = MongoConnectionFactory.CreateClient(connectionString);

        // Extract database name from path
        var databaseName = ExtractDatabaseName(uri.Path);

        if (string.IsNullOrEmpty(databaseName))
            return false;

        // Check if database exists by listing databases
        using var cursor = await client.ListDatabaseNamesAsync(cancellationToken);

        while (await cursor.MoveNextAsync(cancellationToken))
        {
            if (cursor.Current.Contains(databaseName))
                return true;
        }

        return false;
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

    private static string? ExtractDatabaseName(string? path)
    {
        if (string.IsNullOrEmpty(path))
            return null;

        // Remove leading slash and get the first segment
        var trimmed = path.TrimStart('/');
        var slashIndex = trimmed.IndexOf('/');

        return slashIndex >= 0
            ? trimmed.Substring(0, slashIndex)
            : trimmed;
    }

    /// <summary>
    ///     MongoDB database connection wrapper implementing <see cref="IDatabaseConnection" />.
    /// </summary>
    private class MongoDatabaseConnection : IDatabaseConnection
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
            using var cursor = await _client.ListDatabaseNamesAsync(cancellationToken);

            // Just enumerate to verify connection works
            while (await cursor.MoveNextAsync(cancellationToken))
            {
            }
        }

        public Task CloseAsync(CancellationToken cancellationToken = default)
        {
            // MongoDB client doesn't require explicit close
            return Task.CompletedTask;
        }

        public Task<IDatabaseTransaction> BeginTransactionAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException(
                "MongoDB transactions require a session. Use the IMongoClient directly for transaction support.");
        }

        public Task<IDatabaseCommand> CreateCommandAsync(CancellationToken cancellationToken = default)
        {
            throw new NotSupportedException(
                "MongoDB commands are database-specific. Use the IMongoDatabase/IMongoCollection APIs directly.");
        }

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
