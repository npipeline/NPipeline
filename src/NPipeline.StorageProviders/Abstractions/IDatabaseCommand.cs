using System.Data;

namespace NPipeline.StorageProviders.Abstractions;

/// <summary>
///     Database command abstraction for executing database operations.
/// </summary>
public interface IDatabaseCommand : IAsyncDisposable
{
    /// <summary>
    ///     Gets or sets the command text (SQL query or stored procedure name).
    /// </summary>
    string CommandText { get; set; }

    /// <summary>
    ///     Gets or sets the command timeout in seconds.
    /// </summary>
    int CommandTimeout { get; set; }

    /// <summary>
    ///     Gets or sets the command type (Text, StoredProcedure, TableDirect).
    /// </summary>
    CommandType CommandType { get; set; }

    /// <summary>
    ///     Adds a parameter to the command.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The parameter value.</param>
    void AddParameter(string name, object? value);

    /// <summary>
    ///     Executes the command and returns a data reader.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task<IDatabaseReader> ExecuteReaderAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Executes the command and returns the number of rows affected.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken = default);
}
