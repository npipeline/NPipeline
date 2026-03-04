using MySqlConnector;
using NPipeline.Connectors.MySql.Connection;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;
using NPipeline.StorageProviders.Utilities;

namespace NPipeline.Connectors.MySql;

/// <summary>
///     MySQL storage provider implementation.
///     Provides database connection management for MySQL and MariaDB databases via <see cref="StorageUri" />.
/// </summary>
/// <remarks>
///     Supports <c>mysql</c> and <c>mariadb</c> URI schemes.
///     Connection strings are built with <see cref="MySqlConnectionStringBuilder" />.
///     Stream-based operations are not supported; use <see cref="IDatabaseConnection" /> instead.
/// </remarks>
public sealed class MySqlDatabaseStorageProvider : IDatabaseStorageProvider, IStorageProviderMetadataProvider
{
    private static readonly StorageProviderMetadata ProviderMetadata = new()
    {
        Name = "MySQL",
        SupportedSchemes = ["mysql", "mariadb"],
        SupportsRead = false,
        SupportsWrite = false,
        SupportsListing = false,
        SupportsMetadata = false,
        SupportsHierarchy = false,
    };

    /// <summary>Gets the primary URI scheme targeted by this provider.</summary>
    public StorageScheme Scheme => StorageScheme.MySql;

    /// <inheritdoc />
    public bool CanHandle(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        return uri.Scheme == StorageScheme.MySql || uri.Scheme == StorageScheme.MariaDb;
    }

    /// <summary>
    ///     Builds a MySQL connection string from the specified <see cref="StorageUri" />.
    /// </summary>
    /// <remarks>
    ///     URI format: <c>mysql://[user[:password]@]host[:port]/database[?param=value...]</c>
    /// </remarks>
    public string GetConnectionString(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var info = DatabaseUriParser.Parse(uri);

        var builder = new MySqlConnectionStringBuilder
        {
            Server = info.Host,
            Database = info.Database,
        };

        if (info.Port.HasValue)
            builder.Port = (uint)info.Port.Value;

        if (!string.IsNullOrWhiteSpace(info.Username))
            builder.UserID = info.Username;

        if (!string.IsNullOrWhiteSpace(info.Password))
            builder.Password = info.Password;

        // Pass through additional query-string parameters
        foreach (var kvp in info.Parameters)
        {
            if (!IsHandledParameter(kvp.Key))
                builder[kvp.Key] = kvp.Value;
        }

        return builder.ConnectionString;
    }

    /// <inheritdoc />
    public async Task<IDatabaseConnection> GetConnectionAsync(
        StorageUri uri,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var connectionString = GetConnectionString(uri);
        var connection = new MySqlConnection(connectionString);

        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await connection.DisposeAsync().ConfigureAwait(false);

            throw new DatabaseConnectionException(
                $"Failed to establish MySQL connection to {uri.Host}/{uri.Path.TrimStart('/')}.",
                ex);
        }

        return new MySqlDatabaseConnection(connection);
    }

    /// <inheritdoc />
    public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            $"OpenReadAsync is not supported by {nameof(MySqlDatabaseStorageProvider)}. " +
            "Use IDatabaseConnection for data access.");

    /// <inheritdoc />
    public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            $"OpenWriteAsync is not supported by {nameof(MySqlDatabaseStorageProvider)}. " +
            "Use IDatabaseConnection for data access.");

    /// <inheritdoc />
    public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
        => throw new NotSupportedException(
            $"ExistsAsync is not supported by {nameof(MySqlDatabaseStorageProvider)}. " +
            "Use IDatabaseConnection for data access.");

    /// <inheritdoc />
    public StorageProviderMetadata GetMetadata() => ProviderMetadata;

    private static bool IsHandledParameter(string key)
    {
        var handled = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "server", "host", "data source", "datasource", "addr", "address",
            "database", "initial catalog",
            "user id", "uid", "user", "username",
            "password", "pwd",
            "port",
        };

        return handled.Contains(key);
    }
}
