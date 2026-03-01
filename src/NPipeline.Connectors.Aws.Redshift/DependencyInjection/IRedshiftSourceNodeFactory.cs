using NPipeline.Connectors.Aws.Redshift.Configuration;
using NPipeline.Connectors.Aws.Redshift.Mapping;
using NPipeline.Connectors.Aws.Redshift.Nodes;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Aws.Redshift.DependencyInjection;

/// <summary>
///     Factory contract for creating Redshift source nodes from DI.
/// </summary>
public interface IRedshiftSourceNodeFactory
{
    /// <summary>
    ///     Creates a Redshift source node with named connection and custom mapper.
    /// </summary>
    /// <typeparam name="T">Row type to read into.</typeparam>
    /// <param name="connectionName">Named connection identifier in the connection pool.</param>
    /// <param name="query">SQL query to execute.</param>
    /// <param name="rowMapper">Optional custom row mapper function (uses attribute-based mapping if null).</param>
    /// <param name="configuration">Optional Redshift configuration overrides.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Configured Redshift source node.</returns>
    Task<RedshiftSourceNode<T>> CreateSourceAsync<T>(
        string connectionName,
        string query,
        Func<RedshiftRow, T>? rowMapper = null,
        RedshiftConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    ///     Creates a Redshift source node with default connection and custom mapper.
    /// </summary>
    /// <typeparam name="T">Row type to read into.</typeparam>
    /// <param name="query">SQL query to execute.</param>
    /// <param name="rowMapper">Optional custom row mapper function (uses attribute-based mapping if null).</param>
    /// <param name="configuration">Optional Redshift configuration overrides.</param>
    /// <param name="parameters">Optional query parameters.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Configured Redshift source node.</returns>
    Task<RedshiftSourceNode<T>> CreateSourceAsync<T>(
        string query,
        Func<RedshiftRow, T>? rowMapper = null,
        RedshiftConfiguration? configuration = null,
        DatabaseParameter[]? parameters = null,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    ///     Creates a Redshift source node with named connection and attribute-based mapping.
    /// </summary>
    /// <typeparam name="T">Row type with mapping attributes.</typeparam>
    /// <param name="connectionName">Named connection identifier in the connection pool.</param>
    /// <param name="query">SQL query to execute.</param>
    /// <param name="configuration">Optional Redshift configuration overrides.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Configured Redshift source node with attribute-based mapping.</returns>
    Task<RedshiftSourceNode<T>> CreateSourceWithAttributesAsync<T>(
        string connectionName,
        string query,
        RedshiftConfiguration? configuration = null,
        CancellationToken cancellationToken = default)
        where T : class;
}
