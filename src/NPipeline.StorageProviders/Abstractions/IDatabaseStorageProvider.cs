using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Abstractions;

/// <summary>
///     Abstraction for database storage providers that accept a <see cref="StorageUri" /> for configuration.
///     Extends <see cref="IStorageProvider" /> with database-specific methods for connection management.
/// </summary>
/// <remarks>
///     Design goals:
///     - Environment-aware configuration via <see cref="StorageUri" />
///     - Decoupled from specific database implementations (PostgreSQL, SQL Server, etc.)
///     - Support for both connection string generation and direct connection creation
/// </remarks>
public interface IDatabaseStorageProvider : IStorageProvider
{
    /// <summary>
    ///     Generates a connection string from the specified <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing connection information.</param>
    /// <returns>
    ///     A database-specific connection string (e.g., "Host=localhost;Port=5432;Database=mydb;Username=user;Password=pass").
    /// </returns>
    /// <exception cref="ArgumentNullException">If <paramref name="uri" /> is null.</exception>
    /// <exception cref="ArgumentException">
    ///     If the URI is missing required components (e.g., host or database name).
    /// </exception>
    string GetConnectionString(StorageUri uri);

    /// <summary>
    ///     Creates a database connection from the specified <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing connection information.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>
    ///     A task producing an <see cref="IDatabaseConnection" /> that can be used to interact with the database.
    /// </returns>
    /// <exception cref="ArgumentNullException">If <paramref name="uri" /> is null.</exception>
    /// <exception cref="ArgumentException">
    ///     If the URI is missing required components (e.g., host or database name).
    /// </exception>
    /// <exception cref="DatabaseConnectionException">
    ///     If the connection cannot be established due to network, authentication, or other database-specific errors.
    /// </exception>
    Task<IDatabaseConnection> GetConnectionAsync(StorageUri uri, CancellationToken cancellationToken = default);
}
