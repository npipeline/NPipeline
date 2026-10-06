using System.Data.Common;
using NPipeline.Connectors.Database;
using NPipeline.Connectors.Postgres.Configuration;
using NPipeline.Connectors.Sql;
using Npgsql;

namespace NPipeline.Connectors.Postgres.Nodes;

/// <summary>
///     Runs a query against PostgreSQL and streams its rows as records. Create one with <see cref="PostgresConnector" />.
///     Members map to snake_case columns unless <see cref="SqlNodeOptions.Naming" /> says otherwise.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public sealed class PostgresSourceNode<T> : SqlSourceNode<T>
{
    private static readonly Lazy<IDatabaseConnectionProvider> Provider = new(() => new PostgresDatabaseConnectionProvider());

    private readonly PostgresReadOptions _options;

    /// <summary>Creates a source that maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <exception cref="NotSupportedException">A mapped member is not a single value.</exception>
    public PostgresSourceNode(PostgresReadOptions options)
        : this(options, null)
    {
    }

    /// <summary>Creates a source that builds each record from a <see cref="SqlRow" /> with <paramref name="map" />.</summary>
    /// <param name="options">The source's options.</param>
    /// <param name="map">Builds a record from a row. An exception it throws is a row error.</param>
    public PostgresSourceNode(PostgresReadOptions options, Func<SqlRow, T>? map)
        : base(options, map, PostgresShape.For((options ?? throw new ArgumentNullException(nameof(options))).Naming))
    {
        _options = options;
    }

    /// <inheritdoc />
    protected override string ConnectorName => PostgresDialect.Instance.Name;

    /// <inheritdoc />
    protected override IDatabaseConnectionProvider DefaultProvider => Provider.Value;

    /// <inheritdoc />
    protected override DbConnection CreateConnection(string connectionString) => new NpgsqlConnection(connectionString);

    /// <inheritdoc />
    protected override void BindParameter(DbParameter parameter, object? value) =>
        parameter.Value = PostgresDialect.Normalize(value, null).Value ?? DBNull.Value;

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
