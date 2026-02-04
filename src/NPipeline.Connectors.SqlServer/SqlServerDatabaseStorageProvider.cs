using Microsoft.Data.SqlClient;
using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.SqlServer.Connection;
using NPipeline.Connectors.Utilities;

namespace NPipeline.Connectors.SqlServer;

/// <summary>
///     SQL Server storage provider implementation.
///     Provides database connection management for SQL Server databases via StorageUri.
/// </summary>
/// <remarks>
///     This provider supports both "mssql" and "sqlserver" URI schemes.
///     Connection strings are built using SqlConnectionStringBuilder.
///     Stream-based operations (OpenReadAsync, OpenWriteAsync, ExistsAsync) are not supported
///     as database providers are intended for connection management only.
/// </remarks>
public sealed class SqlServerDatabaseStorageProvider : IDatabaseStorageProvider, IStorageProviderMetadataProvider
{
    private static readonly StorageProviderMetadata Metadata = new()
    {
        Name = "SQL Server",
        SupportedSchemes = ["mssql", "sqlserver"],
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
    public StorageScheme Scheme => StorageScheme.Mssql;

    /// <summary>
    ///     Indicates whether this provider can handle the specified <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage location to evaluate.</param>
    /// <returns>True if the provider can handle the given uri; otherwise false.</returns>
    public bool CanHandle(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        return uri.Scheme == StorageScheme.Mssql || uri.Scheme == StorageScheme.SqlServer;
    }

    /// <summary>
    ///     Generates a SQL Server connection string from the specified <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing connection information.</param>
    /// <returns>
    ///     A SQL Server connection string (e.g., "Server=localhost,1433;Database=mydb;User Id=user;Password=pass").
    /// </returns>
    /// <exception cref="ArgumentNullException">If <paramref name="uri" /> is null.</exception>
    /// <exception cref="ArgumentException">
    ///     If the URI is missing required components (e.g., host or database name).
    /// </exception>
    public string GetConnectionString(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var info = DatabaseUriParser.Parse(uri);

        var builder = new SqlConnectionStringBuilder
        {
            DataSource = info.Port.HasValue
                ? $"{info.Host},{info.Port.Value}"
                : info.Host,
            InitialCatalog = info.Database
        };

        if (!string.IsNullOrWhiteSpace(info.Username))
        {
            builder.UserID = info.Username;
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
    ///     Creates a SQL Server database connection from specified <see cref="StorageUri" />.
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
        var connection = new SqlConnection(connectionString);

        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw new NPipeline.Connectors.Exceptions.DatabaseConnectionException(
                $"Failed to establish SQL Server connection to {uri.Host}/{uri.Path.TrimStart('/')}.", ex);
        }

        return new SqlServerDatabaseConnection(connection);
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
            $"OpenReadAsync is not supported by {nameof(SqlServerDatabaseStorageProvider)}. " +
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
            $"OpenWriteAsync is not supported by {nameof(SqlServerDatabaseStorageProvider)}. " +
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
            $"ExistsAsync is not supported by {nameof(SqlServerDatabaseStorageProvider)}. " +
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
            "server", "data source", "addr", "address", "network address",
            "database", "initial catalog",
            "user id", "uid", "user", "username",
            "password", "pwd"
        };

        return handledKeys.Contains(key);
    }
}
