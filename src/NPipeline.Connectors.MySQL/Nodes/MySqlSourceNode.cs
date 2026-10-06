using System.Data.Common;
using MySqlConnector;
using NPipeline.Connectors.Database;
using NPipeline.Connectors.Sql;
using NPipeline.Connectors.MySql.Configuration;

namespace NPipeline.Connectors.MySql.Nodes;

/// <summary>
///     Runs a query against MySQL and streams its rows as records. Create one with <see cref="MySqlNodes" />.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public sealed class MySqlSourceNode<T> : SqlSourceNode<T>
{
    private static readonly Lazy<IDatabaseConnectionProvider> Provider = new(() => new MySqlDatabaseConnectionProvider());

    private readonly MySqlReadOptions _options;

    /// <summary>Creates a source that maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <exception cref="NotSupportedException">A mapped member is not a single value.</exception>
    public MySqlSourceNode(MySqlReadOptions options)
        : this(options, null)
    {
    }

    /// <summary>Creates a source that builds each record from a <see cref="SqlRow" /> with <paramref name="map" />.</summary>
    /// <param name="options">The source's options.</param>
    /// <param name="map">Builds a record from a row. An exception it throws is a row error.</param>
    public MySqlSourceNode(MySqlReadOptions options, Func<SqlRow, T>? map)
        : base(options, map, MySqlShape.Read((options ?? throw new ArgumentNullException(nameof(options))).Naming))
    {
        _options = options;
    }

    /// <inheritdoc />
    protected override string ConnectorName => MySqlDialect.Instance.Name;

    /// <inheritdoc />
    protected override IDatabaseConnectionProvider DefaultProvider => Provider.Value;

    /// <inheritdoc />
    protected override DbConnection CreateConnection(string connectionString) => new MySqlConnection(connectionString);

    /// <inheritdoc />
    protected override async Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken) =>
        _options.ConnectionPool is { } pool
            ? _options.ConnectionName is { Length: > 0 } name
                ? await pool.GetConnectionAsync(name, cancellationToken).ConfigureAwait(false)
                : await pool.GetConnectionAsync(cancellationToken).ConfigureAwait(false)
            : await base.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);

    /// <inheritdoc />
    protected override async Task<TResult> OpenWithRetryAsync<TResult>(Func<CancellationToken, Task<TResult>> open, CancellationToken cancellationToken) =>
        await _options.Resilience.RunAsync(open, cancellationToken).ConfigureAwait(false);
}
