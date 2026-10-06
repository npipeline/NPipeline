using NPipeline.Connectors.Database;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DuckDB;

/// <summary>
///     Resolves a <see cref="StorageUri" /> naming a local DuckDB database file into a connection string. DuckDB has no
///     network protocol of its own, so the URI's path is the file to open.
/// </summary>
internal sealed class DuckDBDatabaseConnectionProvider : IDatabaseConnectionProvider
{
    /// <inheritdoc />
    public string GetConnectionString(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        return $"Data Source={uri.Path}";
    }

    /// <inheritdoc />
    public Task<IDatabaseConnection> OpenConnectionAsync(StorageUri uri, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException($"{nameof(DuckDBDatabaseConnectionProvider)} only builds connection strings; DuckDB nodes open their own {nameof(System.Data.Common.DbConnection)}.");
}
