using System.Data.Common;
using Microsoft.Data.SqlClient;
using NPipeline.Connectors.Database;
using NPipeline.Connectors.Sql;
using NPipeline.Connectors.SqlServer.Configuration;

namespace NPipeline.Connectors.SqlServer.Nodes;

/// <summary>
///     Runs a query against SQL Server and streams its rows as records. Create one with <see cref="SqlServerConnector" />.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public sealed class SqlServerSourceNode<T> : SqlSourceNode<T>
{
    private static readonly Lazy<IDatabaseConnectionProvider> Provider = new(() => new SqlServerDatabaseConnectionProvider());

    private readonly SqlServerReadOptions _options;

    /// <summary>Creates a source that maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <exception cref="NotSupportedException">A mapped member is not a single value.</exception>
    public SqlServerSourceNode(SqlServerReadOptions options)
        : this(options, null)
    {
    }

    /// <summary>Creates a source that builds each record from a <see cref="SqlRow" /> with <paramref name="map" />.</summary>
    /// <param name="options">The source's options.</param>
    /// <param name="map">Builds a record from a row. An exception it throws is a row error.</param>
    public SqlServerSourceNode(SqlServerReadOptions options, Func<SqlRow, T>? map)
        : base(options, map, SqlServerShape.Read((options ?? throw new ArgumentNullException(nameof(options))).Naming))
    {
        _options = options;
    }

    /// <inheritdoc />
    protected override string ConnectorName => SqlServerDialect.Instance.Name;

    /// <inheritdoc />
    protected override IDatabaseConnectionProvider DefaultProvider => Provider.Value;

    /// <inheritdoc />
    protected override DbConnection CreateConnection(string connectionString) => new SqlConnection(connectionString);

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
