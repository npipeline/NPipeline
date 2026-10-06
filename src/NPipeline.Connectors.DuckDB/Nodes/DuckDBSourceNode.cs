using System.Data.Common;
using DuckDB.NET.Data;
using NPipeline.Connectors.Database;
using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.Sql;

namespace NPipeline.Connectors.DuckDB.Nodes;

/// <summary>
///     Runs a DuckDB query, over a database or over files (<c>read_parquet</c>, <c>read_csv</c>, <c>read_json</c>), and
///     streams its rows as records. Create one with <see cref="DuckDBConnector" />.
/// </summary>
/// <typeparam name="T">The record type.</typeparam>
public sealed class DuckDBSourceNode<T> : SqlSourceNode<T>
{
    private readonly DuckDBReadOptions _options;

    /// <summary>Creates a source that maps columns to <typeparamref name="T" />'s members by name.</summary>
    /// <exception cref="NotSupportedException">A mapped member is not a single value.</exception>
    public DuckDBSourceNode(DuckDBReadOptions options)
        : this(options, null)
    {
    }

    /// <summary>Creates a source that builds each record from a <see cref="SqlRow" /> with <paramref name="map" />.</summary>
    /// <param name="options">The source's options.</param>
    /// <param name="map">Builds a record from a row. An exception it throws is a row error.</param>
    public DuckDBSourceNode(DuckDBReadOptions options, Func<SqlRow, T>? map)
        : base(options, map, DuckDBShape.For((options ?? throw new ArgumentNullException(nameof(options))).Naming))
    {
        _options = options;
    }

    /// <inheritdoc />
    protected override string ConnectorName => DuckDBDialect.Instance.Name;

    /// <inheritdoc />
    protected override IDatabaseConnectionProvider DefaultProvider { get; } = new DuckDBDatabaseConnectionProvider();

    /// <inheritdoc />
    protected override DbConnection CreateConnection(string connectionString) => new DuckDBConnection(connectionString);

    /// <inheritdoc />
    protected override Task<DbConnection> OpenConnectionAsync(CancellationToken cancellationToken) =>
        _options.ConnectionString is null && _options.Uri is null
            ? _options.Database.OpenAsync(cancellationToken)
            : base.OpenConnectionAsync(cancellationToken);
}
