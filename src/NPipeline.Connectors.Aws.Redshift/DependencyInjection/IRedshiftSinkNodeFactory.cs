using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Nodes;

namespace NPipeline.Connectors.Aws.Redshift.DependencyInjection;

/// <summary>
///     Factory contract for creating Redshift sink nodes from DI.
/// </summary>
public interface IRedshiftSinkNodeFactory
{
    /// <summary>
    ///     Creates a Redshift sink node with named connection and table name.
    /// </summary>
    /// <typeparam name="T">Row type to write.</typeparam>
    /// <param name="connectionName">Named connection identifier in the connection pool.</param>
    /// <param name="tableName">Target table name (will use default schema).</param>
    /// <param name="writeStrategy">Write strategy to use (default: Batch).</param>
    /// <param name="configuration">Optional Redshift configuration overrides.</param>
    /// <param name="schema">Optional schema name (overrides configuration schema).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Configured Redshift sink node.</returns>
    Task<RedshiftSinkNode<T>> CreateSinkAsync<T>(
        string connectionName,
        string tableName,
        RedshiftWriteStrategy writeStrategy = RedshiftWriteStrategy.Batch,
        RedshiftConfiguration? configuration = null,
        string? schema = null,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    ///     Creates a Redshift sink node with default connection and table name.
    /// </summary>
    /// <typeparam name="T">Row type to write.</typeparam>
    /// <param name="tableName">Target table name (will use default schema).</param>
    /// <param name="writeStrategy">Write strategy to use (default: Batch).</param>
    /// <param name="configuration">Optional Redshift configuration overrides.</param>
    /// <param name="schema">Optional schema name (overrides configuration schema).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Configured Redshift sink node.</returns>
    Task<RedshiftSinkNode<T>> CreateSinkAsync<T>(
        string tableName,
        RedshiftWriteStrategy writeStrategy = RedshiftWriteStrategy.Batch,
        RedshiftConfiguration? configuration = null,
        string? schema = null,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    ///     Creates a Redshift sink node with named connection, schema, and table name.
    /// </summary>
    /// <typeparam name="T">Row type to write.</typeparam>
    /// <param name="connectionName">Named connection identifier in the connection pool.</param>
    /// <param name="schema">Target schema name.</param>
    /// <param name="tableName">Target table name.</param>
    /// <param name="writeStrategy">Write strategy to use (default: Batch).</param>
    /// <param name="configuration">Optional Redshift configuration overrides.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Configured Redshift sink node.</returns>
    Task<RedshiftSinkNode<T>> CreateSinkAsync<T>(
        string connectionName,
        string schema,
        string tableName,
        RedshiftWriteStrategy writeStrategy = RedshiftWriteStrategy.Batch,
        RedshiftConfiguration? configuration = null,
        CancellationToken cancellationToken = default)
        where T : class;
}
