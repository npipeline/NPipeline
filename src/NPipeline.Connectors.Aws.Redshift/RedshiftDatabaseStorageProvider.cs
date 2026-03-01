using Npgsql;
using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Connection;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;
using NPipeline.StorageProviders.Utilities;

namespace NPipeline.Connectors.Aws.Redshift;

/// <summary>
///     AWS Redshift storage provider implementation.
///     Provides database connection management for Redshift clusters via StorageUri.
/// </summary>
/// <remarks>
///     This provider supports the "redshift" URI scheme.
///     Connection strings are built using NpgsqlConnectionStringBuilder for compatibility
///     with the Npgsql driver used to connect to Redshift.
///     Stream-based operations (OpenReadAsync, OpenWriteAsync, ExistsAsync) are not supported
///     as database providers are intended for connection management only.
/// </remarks>
public sealed class RedshiftDatabaseStorageProvider : IDatabaseStorageProvider, IStorageProviderMetadataProvider
{
    private const int DefaultRedshiftPort = 5439;

    private static readonly StorageProviderMetadata Metadata = new()
    {
        Name = "AWS Redshift",
        SupportedSchemes = ["redshift"],
        SupportsRead = false,
        SupportsWrite = false,
        SupportsListing = false,
        SupportsMetadata = false,
        SupportsHierarchy = false,
    };

    /// <summary>
    ///     Gets the primary URI scheme this provider targets.
    /// </summary>
    public StorageScheme Scheme => new("redshift");

    /// <summary>
    ///     Indicates whether this provider can handle the specified <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage location to evaluate.</param>
    /// <returns>True if the provider can handle the given uri; otherwise false.</returns>
    public bool CanHandle(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        return string.Equals(uri.Scheme.Value, "redshift", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    ///     Generates a Redshift connection string from the specified <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing connection information.</param>
    /// <returns>
    ///     A Npgsql-compatible connection string (e.g., "Host=cluster.example.com;Port=5439;Database=mydb;Username=user;Password=pass").
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
            Database = info.Database,
            Port = info.Port ?? DefaultRedshiftPort,
        };

        if (!string.IsNullOrWhiteSpace(info.Username))
            builder.Username = info.Username;

        if (!string.IsNullOrWhiteSpace(info.Password))
            builder.Password = info.Password;

        // SSL is required for Redshift connections
        builder.SslMode = SslMode.Require;

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
    ///     Creates a Redshift database connection from the specified <see cref="StorageUri" />.
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

            throw new DatabaseConnectionException(
                $"Failed to establish Redshift connection to {uri.Host}/{uri.Path.TrimStart('/')}.", ex);
        }

        return new RedshiftDatabaseConnection(connection);
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
            $"OpenReadAsync is not supported by {nameof(RedshiftDatabaseStorageProvider)}. " +
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
            $"OpenWriteAsync is not supported by {nameof(RedshiftDatabaseStorageProvider)}. " +
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
            $"ExistsAsync is not supported by {nameof(RedshiftDatabaseStorageProvider)}. " +
            $"Database providers are intended for connection management only. " +
            $"Use {nameof(IDatabaseConnection)} and {nameof(IDatabaseCommand)} for database operations.");
    }

    /// <summary>
    ///     Returns metadata describing the provider's capabilities and supported schemes.
    /// </summary>
    /// <returns>A <see cref="StorageProviderMetadata" /> instance describing the provider.</returns>
    public StorageProviderMetadata GetMetadata()
    {
        return Metadata;
    }

    /// <summary>
    ///     Creates a <see cref="RedshiftConfiguration" /> from the specified <see cref="StorageUri" />.
    /// </summary>
    /// <param name="uri">The storage URI containing connection information.</param>
    /// <returns>A <see cref="RedshiftConfiguration" /> instance populated from the URI.</returns>
    /// <exception cref="ArgumentNullException">If <paramref name="uri" /> is null.</exception>
    public RedshiftConfiguration CreateConfiguration(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var info = DatabaseUriParser.Parse(uri);

        var config = new RedshiftConfiguration
        {
            Host = info.Host,
            Port = info.Port ?? DefaultRedshiftPort,
            Database = info.Database,
            Username = info.Username ?? string.Empty,
            Password = info.Password ?? string.Empty,
            ConnectionString = GetConnectionString(uri),
        };

        // Apply schema parameter if present
        if (info.Parameters.TryGetValue("schema", out var schema))
            config.Schema = schema;

        // Apply timeout parameter if present
        if (info.Parameters.TryGetValue("timeout", out var timeoutStr) &&
            int.TryParse(timeoutStr, out var timeout))
            config.CommandTimeout = timeout;

        // Apply fetch size parameter if present
        if (info.Parameters.TryGetValue("fetchSize", out var fetchSizeStr) &&
            int.TryParse(fetchSizeStr, out var fetchSize))
            config.FetchSize = fetchSize;

        return config;
    }

    private static bool IsHandledParameter(string key)
    {
        // Skip parameters that either:
        //   (a) are explicitly set on NpgsqlConnectionStringBuilder above (host, port, database, username, password), or
        //   (b) are NPipeline-only parameters consumed at the CreateConfiguration level and not valid
        //       NpgsqlConnectionStringBuilder keys (schema, fetchSize).
        // All other query parameters (e.g. Timeout) are forwarded via the generic builder[key] = value path.
        var handledKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "host", "port", "database", "username", "user", "password", "pwd",
            "schema", "fetchSize",
        };

        return handledKeys.Contains(key);
    }
}
