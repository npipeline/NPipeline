using System.Collections.Frozen;
using System.Data.Common;
using NPipeline.Connectors.Database;
using NPipeline.Connectors.Snowflake.Connection;
using NPipeline.StorageProviders.Models;
using Snowflake.Data.Client;

namespace NPipeline.Connectors.Snowflake;

/// <summary>
///     Snowflake connection provider implementation.
///     Provides database connection management for Snowflake databases via StorageUri.
/// </summary>
/// <remarks>
///     This provider supports the "snowflake" URI scheme.
/// </remarks>
public sealed class SnowflakeDatabaseConnectionProvider : IDatabaseConnectionProvider
{
    private static readonly IReadOnlyList<StorageScheme> SupportedSchemeList = [new StorageScheme("snowflake")];

    private static readonly FrozenSet<string> HandledParameters = new[]
    {
        "account", "host",
        "database", "db",
        "user", "username",
        "password", "pwd",
    }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    /// <summary>The URI schemes this provider handles.</summary>
    public IReadOnlyList<StorageScheme> Schemes => SupportedSchemeList;

    /// <summary>
    ///     Generates a Snowflake connection string from the specified <see cref="StorageUri" />.
    /// </summary>
    public string GetConnectionString(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        var info = DatabaseUriParser.Parse(uri);

        // DbConnectionStringBuilder quotes any value containing ';', '=' or quotes, which the Snowflake driver parses back
        // verbatim. Concatenating raw values let a password such as "p;host=evil" add or override connection keys.
        var builder = new DbConnectionStringBuilder();

        if (!string.IsNullOrWhiteSpace(info.Host))
            builder["account"] = info.Host;

        if (!string.IsNullOrWhiteSpace(info.Username))
            builder["user"] = info.Username;

        if (!string.IsNullOrWhiteSpace(info.Password))
            builder["password"] = info.Password;

        if (!string.IsNullOrWhiteSpace(info.Database))
            builder["db"] = info.Database;

        // Add additional parameters from the URI
        foreach (var kvp in info.Parameters)
        {
            if (HandledParameters.Contains(kvp.Key))
                continue;

            builder[kvp.Key] = kvp.Value;
        }

        return builder.ConnectionString;
    }

    /// <summary>
    ///     Opens a Snowflake database connection from the specified <see cref="StorageUri" />.
    /// </summary>
    public async Task<IDatabaseConnection> OpenConnectionAsync(StorageUri uri, CancellationToken cancellationToken = default)
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
}
