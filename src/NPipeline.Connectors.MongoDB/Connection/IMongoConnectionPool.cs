using MongoDB.Driver;

namespace NPipeline.Connectors.MongoDB.Connection;

/// <summary>
///     Manages a pool of MongoDB client connections.
/// </summary>
public interface IMongoConnectionPool : IDisposable
{
    /// <summary>
    ///     Gets a client for the specified named connection.
    /// </summary>
    /// <param name="connectionName">The name of the connection. If null, returns the default client.</param>
    /// <returns>An <see cref="IMongoClient" /> instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the named connection is not found.</exception>
    IMongoClient GetClient(string? connectionName = null);

    /// <summary>
    ///     Gets a client for the specified connection string.
    /// </summary>
    /// <param name="connectionString">The MongoDB connection string.</param>
    /// <returns>An <see cref="IMongoClient" /> instance.</returns>
    IMongoClient GetClientForUri(string connectionString);

    /// <summary>
    ///     Checks if a named connection exists.
    /// </summary>
    /// <param name="name">The name of the connection.</param>
    /// <returns>True if the named connection exists; otherwise, false.</returns>
    bool HasNamedConnection(string name);

    /// <summary>
    ///     Gets all named connection names.
    /// </summary>
    /// <returns>A collection of named connection names.</returns>
    IEnumerable<string> GetNamedConnectionNames();
}
