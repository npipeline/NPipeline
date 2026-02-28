using DuckDB.NET.Data;
using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Exceptions;

namespace NPipeline.Connectors.DuckDB.Connection;

/// <summary>
///     Default implementation of <see cref="IDuckDBConnectionFactory" />.
///     Creates DuckDB connections to in-memory or file-based databases.
/// </summary>
public sealed class DuckDBConnectionFactory : IDuckDBConnectionFactory
{
    private readonly string _connectionString;

    /// <summary>
    ///     Creates a factory for the specified database path.
    /// </summary>
    /// <param name="databasePath">
    ///     Path to a .duckdb file, or null/empty for an in-memory database.
    /// </param>
    /// <param name="accessMode">Optional access mode override.</param>
    public DuckDBConnectionFactory(string? databasePath = null, DuckDBAccessMode accessMode = DuckDBAccessMode.Automatic)
    {
        _connectionString = BuildConnectionString(databasePath, accessMode);
    }

    /// <summary>
    ///     Creates a factory from an existing connection string.
    /// </summary>
    /// <param name="connectionString">A DuckDB connection string.</param>
    internal DuckDBConnectionFactory(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    /// <inheritdoc />
    public DuckDBConnection CreateConnection()
    {
        try
        {
            return new DuckDBConnection(_connectionString);
        }
        catch (Exception ex)
        {
            throw new DuckDBConnectionException(
                "Failed to create DuckDB connection.",
                _connectionString,
                ex);
        }
    }

    internal static string BuildConnectionString(string? databasePath, DuckDBAccessMode accessMode)
    {
        if (string.IsNullOrWhiteSpace(databasePath))
            return "DataSource=:memory:";

        var builder = $"DataSource={databasePath}";

        return accessMode switch
        {
            DuckDBAccessMode.ReadOnly => builder + ";access_mode=READ_ONLY",
            DuckDBAccessMode.ReadWrite => builder + ";access_mode=READ_WRITE",
            _ => builder,
        };
    }
}
