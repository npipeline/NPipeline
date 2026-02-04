using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.PostgreSQL.Connection;
using NPipeline.Connectors.Utilities;
using Npgsql;

namespace NPipeline.Connectors.PostgreSQL;

/// <summary>
///     PostgreSQL storage provider implementation.
///     Provides database connection management for PostgreSQL databases via StorageUri.
/// </summary>
/// <remarks>
///     This provider supports both "postgres" and "postgresql" URI schemes.
///     Connection strings are built using NpgsqlConnectionStringBuilder.
///     Stream-based operations (OpenReadAsync, OpenWriteAsync, ExistsAsync) are not supported
///     as database providers are intended for connection management only.
/// </remarks>
public sealed class PostgresDatabaseStorageProvider : IDatabaseStorageProvider, IStorageProviderMetadataProvider
{
    private static readonly StorageProviderMetadata Metadata = new()
    {
        Name = "PostgreSQL",
        SupportedSchemes = ["postgres", "postgresql"],
        SupportsRead = false,
        SupportsWrite = false,
        SupportsDelete = false,
        SupportsListing = false,
        SupportsMetadata = false,
        SupportsHierarchy = false
    };

    /// <summary>
    ///     Gets the primary URI scheme this provider targets.
    /// </summary>
    public StorageScheme Scheme => StorageScheme.Postgres;

    /// <summary>
    ///     Indicates whether this provider can handle the specified <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage location to evaluate.</param>
    /// <returns>True if the provider can handle the given uri; otherwise false.</returns>
    public bool CanHandle(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        return uri.Scheme == StorageScheme.Postgres || uri.Scheme == StorageScheme.Postgresql;
    }

    /// <summary>
    ///     Generates a PostgreSQL connection string from the specified <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing connection information.</param>
    /// <returns>
    ///     A PostgreSQL connection string (e.g., "Host=localhost;Port=5432;Database=mydb;Username=user;Password=pass").
    /// </returns>
    /// <exception cref="ArgumentNullException">If <paramref name="uri" /> is null.</exception>
    /// <exception cref="ArgumentException">
    ///     If the URI is missing required components (e.g., host or database name).
    /// </exception>
    public string GetConnectionString(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var info = DatabaseUriParser.Parse(uri);

        var builder = new NpgsqlConnectionStringBuilder
        {
            Host = info.Host,
            Database = info.Database
        };

        if (info.Port.HasValue)
        {
            builder.Port = info.Port.Value;
        }

        if (!string.IsNullOrWhiteSpace(info.Username))
        {
            builder.Username = info.Username;
        }

        if (!string.IsNullOrWhiteSpace(info.Password))
        {
            builder.Password = info.Password;
        }

        // Add additional parameters from the URI
        foreach (var kvp in info.Parameters)
        {
            // Skip parameters that were already handled by the builder
            if (IsHandledParameter(kvp.Key))
                continue;

            builder[kvp.Key] = kvp.Value;
        }

        return builder.ToString();
    }

    /// <summary>
    ///     Creates a PostgreSQL database connection from the specified <see cref="StorageUri" />.
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
    /// <exception cref="NPipeline.Connectors.Exceptions.DatabaseConnectionException">
    ///     If the connection cannot be established due to network, authentication, or other database-specific errors.
    /// </exception>
    public async Task<IDatabaseConnection> GetConnectionAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var connectionString = GetConnectionString(uri);
        var connection = new NpgsqlConnection(connectionString);

        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw new NPipeline.Connectors.Exceptions.DatabaseConnectionException(
                $"Failed to establish PostgreSQL connection to {uri.Host}/{uri.Path.TrimStart('/')}.", ex);
        }

        return new PostgresDatabaseConnection(connection);
    }

    /// <summary>
    ///     Opens a readable stream for the specified <see cref="StorageUri" />.
    ///     Not supported for database providers.
    /// </summary>
    /// <param name="uri">The storage location to read from.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a readable <see cref="System.IO.Stream" />.</returns>
    /// <exception cref="NotSupportedException">Always thrown for database providers.</exception>
    public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException(
            $"OpenReadAsync is not supported by {nameof(PostgresDatabaseStorageProvider)}. " +
            $"Database providers are intended for connection management only. " +
            $"Use {nameof(IDatabaseConnection)} and {nameof(IDatabaseCommand)} for database operations.");
    }

    /// <summary>
    ///     Opens a writable stream for the specified <see cref="StorageUri" />.
    ///     Not supported for database providers.
    /// </summary>
    /// <param name="uri">The storage location to write to.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a writable <see cref="System.IO.Stream" />.</returns>
    /// <exception cref="NotSupportedException">Always thrown for database providers.</exception>
    public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException(
            $"OpenWriteAsync is not supported by {nameof(PostgresDatabaseStorageProvider)}. " +
            $"Database providers are intended for connection management only. " +
            $"Use {nameof(IDatabaseConnection)} and {nameof(IDatabaseCommand)} for database operations.");
    }

    /// <summary>
    ///     Checks whether a resource exists at the specified <see cref="StorageUri" />.
    ///     Not supported for database providers.
    /// </summary>
    /// <param name="uri">The storage location to check.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>True if the resource exists; otherwise false.</returns>
    /// <exception cref="NotSupportedException">Always thrown for database providers.</exception>
    public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException(
            $"ExistsAsync is not supported by {nameof(PostgresDatabaseStorageProvider)}. " +
            $"Database providers are intended for connection management only. " +
            $"Use {nameof(IDatabaseConnection)} and {nameof(IDatabaseCommand)} for database operations.");
    }

    /// <summary>
    ///     Returns metadata describing the provider's capabilities and supported schemes.
    /// </summary>
    /// <returns>A <see cref="StorageProviderMetadata" /> instance describing the provider.</returns>
    public StorageProviderMetadata GetMetadata() => Metadata;

    private static bool IsHandledParameter(string key)
    {
        var handledKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "host", "port", "database", "username", "user", "password", "pwd"
        };

        return handledKeys.Contains(key);
    }
}
