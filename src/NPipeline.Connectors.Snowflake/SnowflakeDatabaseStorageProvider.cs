using Snowflake.Data.Client;
using NPipeline.Connectors.Snowflake.Connection;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;
using NPipeline.StorageProviders.Utilities;

namespace NPipeline.Connectors.Snowflake;

/// <summary>
///     Snowflake storage provider implementation.
///     Provides database connection management for Snowflake databases via StorageUri.
/// </summary>
/// <remarks>
///     This provider supports the "snowflake" URI scheme.
///     Stream-based operations (OpenReadAsync, OpenWriteAsync, ExistsAsync) are not supported
///     as database providers are intended for connection management only.
/// </remarks>
public sealed class SnowflakeDatabaseStorageProvider : IDatabaseStorageProvider, IStorageProviderMetadataProvider
{
    private static readonly StorageProviderMetadata Metadata = new()
    {
        Name = "Snowflake",
        SupportedSchemes = ["snowflake"],
        SupportsRead = false,
        SupportsWrite = false,
        SupportsListing = false,
        SupportsMetadata = false,
        SupportsHierarchy = false,
    };

    /// <summary>
    ///     Gets the primary URI scheme this provider targets.
    /// </summary>
    public StorageScheme Scheme => new("snowflake");

    /// <summary>
    ///     Indicates whether this provider can handle the specified <see cref="StorageUri" />.
    /// </summary>
    public bool CanHandle(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        return uri.Scheme == new StorageScheme("snowflake");
    }

    /// <summary>
    ///     Generates a Snowflake connection string from the specified <see cref="StorageUri" />.
    /// </summary>
    public string GetConnectionString(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var info = DatabaseUriParser.Parse(uri);

        var parts = new List<string>();

        if (!string.IsNullOrWhiteSpace(info.Host))
            parts.Add($"account={info.Host}");

        if (!string.IsNullOrWhiteSpace(info.Username))
            parts.Add($"user={info.Username}");

        if (!string.IsNullOrWhiteSpace(info.Password))
            parts.Add($"password={info.Password}");

        if (!string.IsNullOrWhiteSpace(info.Database))
            parts.Add($"db={info.Database}");

        // Add additional parameters from the URI
        foreach (var kvp in info.Parameters)
        {
            if (IsHandledParameter(kvp.Key))
                continue;

            parts.Add($"{kvp.Key}={kvp.Value}");
        }

        return string.Join(";", parts);
    }

    /// <summary>
    ///     Creates a Snowflake database connection from the specified <see cref="StorageUri" />.
    /// </summary>
    public async Task<IDatabaseConnection> GetConnectionAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var connectionString = GetConnectionString(uri);
        var connection = new SnowflakeDbConnection(connectionString);

        try
        {
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await connection.DisposeAsync().ConfigureAwait(false);

            throw new DatabaseConnectionException(
                $"Failed to establish Snowflake connection to {uri.Host}/{uri.Path.TrimStart('/')}.", ex);
        }

        return new SnowflakeDatabaseConnection(connection);
    }

    /// <inheritdoc />
    public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException(
            $"OpenReadAsync is not supported by {nameof(SnowflakeDatabaseStorageProvider)}. " +
            $"Database providers are intended for connection management only. " +
            $"Use {nameof(IDatabaseConnection)} and {nameof(IDatabaseCommand)} for database operations.");
    }

    /// <inheritdoc />
    public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException(
            $"OpenWriteAsync is not supported by {nameof(SnowflakeDatabaseStorageProvider)}. " +
            $"Database providers are intended for connection management only. " +
            $"Use {nameof(IDatabaseConnection)} and {nameof(IDatabaseCommand)} for database operations.");
    }

    /// <inheritdoc />
    public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        throw new NotSupportedException(
            $"ExistsAsync is not supported by {nameof(SnowflakeDatabaseStorageProvider)}. " +
            $"Database providers are intended for connection management only. " +
            $"Use {nameof(IDatabaseConnection)} and {nameof(IDatabaseCommand)} for database operations.");
    }

    /// <inheritdoc />
    public StorageProviderMetadata GetMetadata()
    {
        return Metadata;
    }

    private static bool IsHandledParameter(string key)
    {
        var handledKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "account", "host",
            "database", "db",
            "user", "username",
            "password", "pwd",
        };

        return handledKeys.Contains(key);
    }
}
