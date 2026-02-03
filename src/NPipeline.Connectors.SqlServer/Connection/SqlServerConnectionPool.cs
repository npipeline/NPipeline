using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using NPipeline.Connectors.SqlServer.Configuration;

namespace NPipeline.Connectors.SqlServer.Connection;

/// <summary>
///     SQL Server connection pool implementation using SqlConnection.
///     Provides efficient connection pooling and support for named connections.
/// </summary>
public class SqlServerConnectionPool : ISqlServerConnectionPool
{
    private readonly SqlServerConfiguration _configuration;
    private readonly string? _defaultConnectionString;
    private readonly ConcurrentDictionary<string, string> _namedConnectionStrings;

    /// <summary>
    ///     Initializes a new instance of the SqlServerConnectionPool with a single connection string.
    /// </summary>
    /// <param name="connectionString">The connection string for SQL Server.</param>
    public SqlServerConnectionPool(string connectionString)
        : this(new SqlServerOptions { DefaultConnectionString = connectionString })
    {
    }

    /// <summary>
    ///     Initializes a new instance of the SqlServerConnectionPool with named connections only.
    /// </summary>
    /// <param name="namedConnections">Dictionary of named connection strings.</param>
    public SqlServerConnectionPool(IDictionary<string, string> namedConnections)
        : this(new SqlServerOptions { NamedConnections = new Dictionary<string, string>(namedConnections, StringComparer.OrdinalIgnoreCase) })
    {
    }

    /// <summary>
    ///     Initializes a new instance of the SqlServerConnectionPool using configured options.
    /// </summary>
    /// <param name="options">The connector options.</param>
    public SqlServerConnectionPool(SqlServerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _namedConnectionStrings = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        _configuration = options.DefaultConfiguration ?? new SqlServerConfiguration();
        _configuration.ValidateConnectionSettings();

        var hasDefault = !string.IsNullOrWhiteSpace(options.DefaultConnectionString);

        if (hasDefault)
        {
            _defaultConnectionString = BuildConnectionString(options.DefaultConnectionString, _configuration);
            ConnectionString = _defaultConnectionString;
        }

        foreach (var kvp in options.NamedConnections)
        {
            if (string.IsNullOrWhiteSpace(kvp.Value))
                throw new ArgumentException($"Connection string for '{kvp.Key}' cannot be empty.", nameof(options));

            var connectionString = BuildConnectionString(kvp.Value, _configuration);
            _ = _namedConnectionStrings.TryAdd(kvp.Key, connectionString);
            ConnectionString ??= connectionString;
            _defaultConnectionString ??= connectionString;
        }

        if (_defaultConnectionString == null)
        {
            throw new ArgumentException("At least one SQL Server connection string must be configured via DefaultConnectionString or NamedConnections.",
                nameof(options));
        }
    }

    /// <summary>
    ///     Gets a connection from the pool asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An open SqlConnection.</returns>
    public async Task<SqlConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connectionString = _defaultConnectionString ?? throw new InvalidOperationException("No default SQL Server connection string configured.");
        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return connection;
    }

    /// <summary>
    ///     Gets a connection for a named connection string.
    /// </summary>
    /// <param name="name">The name of the connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An open SqlConnection.</returns>
    /// <exception cref="InvalidOperationException">Thrown when named connection is not found.</exception>
    public async Task<SqlConnection> GetConnectionAsync(string name, CancellationToken cancellationToken = default)
    {
        if (!_namedConnectionStrings.TryGetValue(name, out var connectionString))
            throw new InvalidOperationException($"Named connection '{name}' not found.");

        var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return connection;
    }

    /// <summary>
    ///     Gets the connection string used by this pool.
    /// </summary>
    public string? ConnectionString { get; }

    /// <summary>
    ///     Checks if a named connection exists.
    /// </summary>
    /// <param name="name">The name of the connection.</param>
    /// <returns>True if the named connection exists; otherwise, false.</returns>
    public bool HasNamedConnection(string name)
    {
        return _namedConnectionStrings.ContainsKey(name);
    }

    /// <summary>
    ///     Gets all named connection names.
    /// </summary>
    /// <returns>A collection of named connection names.</returns>
    public IEnumerable<string> GetNamedConnectionNames()
    {
        return _namedConnectionStrings.Keys;
    }

    /// <summary>
    ///     Disposes the connection pool.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        // SqlConnection instances are managed by the caller and disposed separately.
        // This pool only manages connection string configuration.
        await Task.CompletedTask.ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private static string BuildConnectionString(string connectionString, SqlServerConfiguration configuration)
    {
        var builder = new SqlConnectionStringBuilder(connectionString)
        {
            ConnectTimeout = configuration.ConnectTimeout,
            CommandTimeout = configuration.CommandTimeout,
            MinPoolSize = configuration.MinPoolSize,
            MaxPoolSize = configuration.MaxPoolSize,
        };

        if (configuration.EnableMARS)
            builder.MultipleActiveResultSets = true;

        if (!string.IsNullOrWhiteSpace(configuration.ApplicationName))
            builder.ApplicationName = configuration.ApplicationName;

        return builder.ConnectionString;
    }
}
