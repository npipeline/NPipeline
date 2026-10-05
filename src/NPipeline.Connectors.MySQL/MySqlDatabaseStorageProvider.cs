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
public sealed class MySqlDatabaseStorageProvider : StorageProvider, IDatabaseStorageProvider
{
    private static readonly IReadOnlyList<StorageScheme> SupportedSchemeList = [new StorageScheme("mysql"), new StorageScheme("mariadb")];

    /// <inheritdoc />
    public override string Name => "MySQL";

    /// <inheritdoc />
    public override IReadOnlyList<StorageScheme> Schemes => SupportedSchemeList;

    /// <inheritdoc />
    public override StorageCapabilities Capabilities => StorageCapabilities.None;

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
