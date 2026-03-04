using MySqlConnector;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Exceptions;

namespace NPipeline.Connectors.MySql.Connection;

/// <summary>
///     Manages MySQL connections using MySqlConnector's built-in connection pooling.
///     Supports a default connection and any number of named connections.
/// </summary>
internal sealed class MySqlConnectionPool : IMySqlConnectionPool
{
    private readonly string? _defaultConnectionString;
    private readonly Dictionary<string, string> _namedConnectionStrings;
    private bool _disposed;

    /// <summary>
    ///     Creates a pool backed by a single connection string.
    /// </summary>
    public MySqlConnectionPool(string connectionString)
    {
        _defaultConnectionString = BuildConnectionString(connectionString, null);
        _namedConnectionStrings = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    ///     Creates a pool backed by named connection strings.
    /// </summary>
    public MySqlConnectionPool(IDictionary<string, string> namedConnections)
    {
        _defaultConnectionString = null;
        _namedConnectionStrings = namedConnections
            .ToDictionary(kvp => kvp.Key, kvp => BuildConnectionString(kvp.Value, null),
                StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    ///     Creates a pool from a <see cref="MySqlOptions"/> instance, injecting configuration overrides.
    /// </summary>
    public MySqlConnectionPool(MySqlOptions options, MySqlConfiguration? configuration = null)
    {
        _defaultConnectionString = options.DefaultConnectionString is not null
            ? BuildConnectionString(options.DefaultConnectionString, configuration)
            : null;

        _namedConnectionStrings = options.NamedConnections
            .ToDictionary(
                kvp => kvp.Key,
                kvp => BuildConnectionString(kvp.Value, configuration),
                StringComparer.OrdinalIgnoreCase);
    }

    /// <inheritdoc />
    public string? ConnectionString => _defaultConnectionString;

    /// <inheritdoc />
    public async Task<MySqlConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (_defaultConnectionString is null)
            throw new MySqlConnectionException(
                "No default connection string is configured. Use a named connection instead.");

        var connection = new MySqlConnection(_defaultConnectionString);
        await OpenAsync(connection, cancellationToken).ConfigureAwait(false);
        return connection;
    }

    /// <inheritdoc />
    public async Task<MySqlConnection> GetConnectionAsync(string name,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_namedConnectionStrings.TryGetValue(name, out var cs))
            throw new MySqlConnectionException(
                $"No connection with name '{name}' is configured.");

        var connection = new MySqlConnection(cs);
        await OpenAsync(connection, cancellationToken).ConfigureAwait(false);
        return connection;
    }

    /// <inheritdoc />
    public bool HasNamedConnection(string name) =>
        _namedConnectionStrings.ContainsKey(name);

    /// <inheritdoc />
    public IEnumerable<string> GetNamedConnectionNames() =>
        _namedConnectionStrings.Keys;

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _disposed = true;
        // MySqlConnector manages pool lifecycle via static state; no instance cleanup needed here.
        return ValueTask.CompletedTask;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static async Task OpenAsync(MySqlConnection connection, CancellationToken ct)
    {
        try
        {
            await connection.OpenAsync(ct).ConfigureAwait(false);
        }
        catch (MySqlConnector.MySqlException ex)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw MySqlExceptionFactory.CreateConnection(
                "Failed to open a MySQL connection.", ex);
        }
        catch (OperationCanceledException)
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>
    ///     Builds a connection string by overlaying <see cref="MySqlConfiguration"/> settings
    ///     on top of the raw connection string supplied by the user.
    /// </summary>
    private static string BuildConnectionString(string rawConnectionString,
        MySqlConfiguration? cfg)
    {
        var builder = new MySqlConnectionStringBuilder(rawConnectionString);

        if (cfg is null)
            return builder.ConnectionString;

        if (!string.IsNullOrWhiteSpace(cfg.DefaultDatabase))
            builder.Database = cfg.DefaultDatabase;

        if (!string.IsNullOrWhiteSpace(cfg.CharacterSet))
            builder.CharacterSet = cfg.CharacterSet;

        if (cfg.ConnectionTimeout > 0)
            builder.ConnectionTimeout = (uint)cfg.ConnectionTimeout;

        if (cfg.MinPoolSize > 0)
            builder.MinimumPoolSize = (uint)cfg.MinPoolSize;

        if (cfg.MaxPoolSize > 0)
            builder.MaximumPoolSize = (uint)cfg.MaxPoolSize;

        builder.AllowUserVariables = cfg.AllowUserVariables;
        builder.ConvertZeroDateTime = cfg.ConvertZeroDateTime;
        builder.AllowLoadLocalInfile = cfg.AllowLoadLocalInfile;

        return builder.ConnectionString;
    }
}
