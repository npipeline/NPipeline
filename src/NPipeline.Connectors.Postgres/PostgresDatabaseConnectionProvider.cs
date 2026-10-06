using System.Collections.Frozen;
using Npgsql;
using NPipeline.Connectors.Database;
using NPipeline.Connectors.Postgres.Connection;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Postgres;

/// <summary>
///     PostgreSQL connection provider implementation.
///     Provides database connection management for PostgreSQL databases via StorageUri.
/// </summary>
/// <remarks>
///     This provider supports both "postgres" and "postgresql" URI schemes.
///     Connection strings are built using NpgsqlConnectionStringBuilder.
/// </remarks>
public sealed class PostgresDatabaseConnectionProvider : IDatabaseConnectionProvider
{
    private static readonly FrozenSet<string> HandledParameterKeys = new[]
    {
        "host", "port", "database", "username", "user", "password", "pwd",
    }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    private static readonly IReadOnlyList<StorageScheme> SupportedSchemeList = [new StorageScheme("postgres"), new StorageScheme("postgresql")];

    /// <summary>The URI schemes this provider handles.</summary>
    public IReadOnlyList<StorageScheme> Schemes => SupportedSchemeList;

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
            Database = info.Database,
        };

        if (info.Port.HasValue)
            builder.Port = info.Port.Value;

        if (!string.IsNullOrWhiteSpace(info.Username))
            builder.Username = info.Username;

        if (!string.IsNullOrWhiteSpace(info.Password))
            builder.Password = info.Password;

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
    ///     Opens a PostgreSQL database connection from the specified <see cref="StorageUri" />.
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
    public async Task<IDatabaseConnection> OpenConnectionAsync(StorageUri uri, CancellationToken cancellationToken = default)
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
                $"Failed to establish PostgreSQL connection to {uri.Host}/{uri.Path.TrimStart('/')}.", ex);
        }

        return new PostgresDatabaseConnection(connection);
    }

    private static bool IsHandledParameter(string key) => HandledParameterKeys.Contains(key);
}
