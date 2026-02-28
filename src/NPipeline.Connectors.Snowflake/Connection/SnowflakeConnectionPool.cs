using System.Collections.Concurrent;
using NPipeline.Connectors.Snowflake.Configuration;
using Snowflake.Data.Client;

namespace NPipeline.Connectors.Snowflake.Connection;

/// <summary>
///     Snowflake connection pool implementation using SnowflakeDbConnection.
///     Provides efficient connection pooling and support for named connections.
/// </summary>
public class SnowflakeConnectionPool : ISnowflakeConnectionPool
{
    private readonly SnowflakeConfiguration _configuration;
    private readonly string? _defaultConnectionString;
    private readonly ConcurrentDictionary<string, string> _namedConnectionStrings;

    /// <summary>
    ///     Initializes a new instance of the SnowflakeConnectionPool with a single connection string.
    /// </summary>
    /// <param name="connectionString">The connection string for Snowflake.</param>
    public SnowflakeConnectionPool(string connectionString)
        : this(new SnowflakeOptions { DefaultConnectionString = connectionString })
    {
    }

    /// <summary>
    ///     Initializes a new instance of the SnowflakeConnectionPool with named connections only.
    /// </summary>
    /// <param name="namedConnections">Dictionary of named connection strings.</param>
    public SnowflakeConnectionPool(IDictionary<string, string> namedConnections)
        : this(new SnowflakeOptions { NamedConnections = new Dictionary<string, string>(namedConnections, StringComparer.OrdinalIgnoreCase) })
    {
    }

    /// <summary>
    ///     Initializes a new instance of the SnowflakeConnectionPool using configured options.
    /// </summary>
    /// <param name="options">The connector options.</param>
    public SnowflakeConnectionPool(SnowflakeOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        _namedConnectionStrings = new ConcurrentDictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        _configuration = options.DefaultConfiguration ?? new SnowflakeConfiguration();
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
            throw new ArgumentException("At least one Snowflake connection string must be configured via DefaultConnectionString or NamedConnections.",
                nameof(options));
        }
    }

    /// <summary>
    ///     Gets a connection from the pool asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An open SnowflakeDbConnection.</returns>
    public async Task<SnowflakeDbConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        var connectionString = _defaultConnectionString ?? throw new InvalidOperationException("No default Snowflake connection string configured.");
        var connection = new SnowflakeDbConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
        return connection;
    }

    /// <summary>
    ///     Gets a connection for a named connection string.
    /// </summary>
    /// <param name="name">The name of the connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An open SnowflakeDbConnection.</returns>
    /// <exception cref="InvalidOperationException">Thrown when named connection is not found.</exception>
    public async Task<SnowflakeDbConnection> GetConnectionAsync(string name, CancellationToken cancellationToken = default)
    {
        if (!_namedConnectionStrings.TryGetValue(name, out var connectionString))
            throw new InvalidOperationException($"Named connection '{name}' not found.");

        var connection = new SnowflakeDbConnection(connectionString);
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
        await Task.CompletedTask.ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }

    private static string BuildConnectionString(string connectionString, SnowflakeConfiguration configuration)
    {
        // Snowflake connection strings are semicolon-delimited key=value pairs
        // We enrich the provided connection string with configuration defaults
        var parts = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        // Parse existing connection string
        foreach (var segment in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var kvp = segment.Split('=', 2);

            if (kvp.Length == 2)
                parts[kvp[0].Trim()] = kvp[1].Trim();
        }

        // Apply configuration defaults only if not already present
        if (!parts.ContainsKey("TIMEOUT") && configuration.ConnectionTimeout > 0)
            parts["TIMEOUT"] = configuration.ConnectionTimeout.ToString();

        if (!parts.ContainsKey("MAXPOOLSIZE") && configuration.MaxPoolSize > 0)
            parts["MAXPOOLSIZE"] = configuration.MaxPoolSize.ToString();

        if (!parts.ContainsKey("MINPOOLSIZE") && configuration.MinPoolSize >= 0)
            parts["MINPOOLSIZE"] = configuration.MinPoolSize.ToString();

        return string.Join(";", parts.Select(kvp => $"{kvp.Key}={kvp.Value}"));
    }
}
