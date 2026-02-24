using NPipeline.Connectors.Azure.CosmosDb.Abstractions;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.DependencyInjection;

/// <summary>
///     Default implementation for resolving Cosmos API adapters.
/// </summary>
internal sealed class CosmosApiAdapterResolver : ICosmosApiAdapterResolver
{
    private readonly IReadOnlyDictionary<CosmosApiType, ICosmosApiAdapter> _byApiType;
    private readonly IReadOnlyDictionary<string, ICosmosApiAdapter> _byScheme;

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosApiAdapterResolver" />.
    /// </summary>
    /// <param name="adapters">Registered adapters.</param>
    public CosmosApiAdapterResolver(IEnumerable<ICosmosApiAdapter> adapters)
    {
        var adapterArray = adapters.ToArray();
        _byApiType = adapterArray.ToDictionary(a => a.ApiType, a => a);

        _byScheme = adapterArray
            .SelectMany(a => a.SupportedSchemes.Select(s => new KeyValuePair<string, ICosmosApiAdapter>(s, a)))
            .ToDictionary(k => k.Key, v => v.Value, StringComparer.OrdinalIgnoreCase);
    }

    /// <inheritdoc />
    public ICosmosApiAdapter GetAdapter(CosmosApiType apiType)
    {
        return _byApiType.TryGetValue(apiType, out var adapter)
            ? adapter
            : throw new InvalidOperationException($"No adapter registered for API type '{apiType}'.");
    }

    /// <inheritdoc />
    public ICosmosApiAdapter GetAdapter(string scheme)
    {
        if (string.IsNullOrWhiteSpace(scheme))
            throw new ArgumentNullException(nameof(scheme));

        return _byScheme.TryGetValue(scheme, out var adapter)
            ? adapter
            : throw new InvalidOperationException($"No adapter registered for URI scheme '{scheme}'.");
    }
}
