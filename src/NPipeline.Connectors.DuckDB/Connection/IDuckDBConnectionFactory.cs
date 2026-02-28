using DuckDB.NET.Data;

namespace NPipeline.Connectors.DuckDB.Connection;

/// <summary>
///     Factory abstraction for creating DuckDB connections.
///     Enables testability and connection lifecycle management.
/// </summary>
public interface IDuckDBConnectionFactory
{
    /// <summary>
    ///     Creates a new <see cref="DuckDBConnection" /> instance.
    ///     The connection is not opened — call Open or OpenAsync before use.
    /// </summary>
    /// <returns>A new DuckDB connection.</returns>
    DuckDBConnection CreateConnection();
}
