using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Abstractions;

/// <summary>
///     Adapter contract for Cosmos API-specific client, source, and sink execution.
/// </summary>
public interface ICosmosApiAdapter
{
    /// <summary>
    ///     Gets the API type implemented by this adapter.
    /// </summary>
    CosmosApiType ApiType { get; }

    /// <summary>
    ///     Gets URI schemes handled by this adapter.
    /// </summary>
    IReadOnlyCollection<string> SupportedSchemes { get; }

    /// <summary>
    ///     Creates an API-specific client object.
    /// </summary>
    /// <param name="configuration">Connector configuration.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Client instance for the selected API.</returns>
    Task<object> CreateClientAsync(CosmosConfiguration configuration, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Creates a source executor backed by the provided API client.
    /// </summary>
    ICosmosSourceExecutor CreateSourceExecutor(object client, CosmosConfiguration configuration);

    /// <summary>
    ///     Creates a sink executor backed by the provided API client.
    /// </summary>
    ICosmosSinkExecutor<T> CreateSinkExecutor<T>(
        object client,
        CosmosConfiguration configuration,
        Func<T, string>? idSelector = null);
}
