using Microsoft.Data.SqlClient;
using NPipeline.Connectors.SqlServer.Connection;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;
using NPipeline.StorageProviders.Utilities;

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
public sealed class SqlServerDatabaseStorageProvider : StorageProvider, IDatabaseStorageProvider
{
    private static readonly IReadOnlyList<StorageScheme> SupportedSchemeList = [new StorageScheme("mssql"), new StorageScheme("sqlserver")];

    /// <inheritdoc />
    public override string Name => "SQL Server";

    /// <inheritdoc />
    public override IReadOnlyList<StorageScheme> Schemes => SupportedSchemeList;

    /// <inheritdoc />
    public override StorageCapabilities Capabilities => StorageCapabilities.None;

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
            InitialCatalog = info.Database,
        };

        if (!string.IsNullOrWhiteSpace(info.Username))
            builder.UserID = info.Username;

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
    /// <exception cref="DatabaseConnectionException">
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

            throw new DatabaseConnectionException(
                $"Failed to establish SQL Server connection to {uri.Host}/{uri.Path.TrimStart('/')}.", ex);
        }

        return new SqlServerDatabaseConnection(connection);
    }

    private static bool IsHandledParameter(string key)
    {
        var handledKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "server", "data source", "addr", "address", "network address",
            "database", "initial catalog",
            "user id", "uid", "user", "username",
            "password", "pwd",
        };

        return handledKeys.Contains(key);
    }
}
