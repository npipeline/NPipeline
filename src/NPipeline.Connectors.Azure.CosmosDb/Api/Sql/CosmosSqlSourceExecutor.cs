using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using NPipeline.Connectors.Azure.CosmosDb.Abstractions;
using NPipeline.Connectors.Azure.CosmosDb.Configuration;

namespace NPipeline.Connectors.Azure.CosmosDb.Api.Sql;

/// <summary>
///     SQL source executor using Cosmos query APIs.
/// </summary>
internal sealed class CosmosSqlSourceExecutor : ICosmosSourceExecutor
{
    private readonly Container _container;
    private readonly int _maxItemCount;

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosSqlSourceExecutor" />.
    /// </summary>
    public CosmosSqlSourceExecutor(CosmosClient client, CosmosConfiguration configuration)
    {
        if (string.IsNullOrWhiteSpace(configuration.DatabaseId))
            throw new InvalidOperationException("DatabaseId is required for SQL source execution.");

        if (string.IsNullOrWhiteSpace(configuration.ContainerId))
            throw new InvalidOperationException("ContainerId is required for SQL source execution.");

        _container = client.GetContainer(configuration.DatabaseId, configuration.ContainerId);
        _maxItemCount = configuration.MaxItemCount;
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<IDictionary<string, object?>>> QueryAsync(
        string query,
        CancellationToken cancellationToken = default)
    {
        var results = new List<IDictionary<string, object?>>();

        var iterator = _container.GetItemQueryStreamIterator(
            new QueryDefinition(query),
            requestOptions: new QueryRequestOptions { MaxItemCount = _maxItemCount });

        while (iterator.HasMoreResults)
        {
            using var response = await iterator.ReadNextAsync(cancellationToken);
            using var reader = new StreamReader(response.Content);
            var payload = await reader.ReadToEndAsync(cancellationToken);

            var json = JObject.Parse(payload);
            var documents = json["Documents"] as JArray;

            if (documents == null)
                continue;

            foreach (var document in documents.OfType<JObject>())
            {
                var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

                foreach (var property in document.Properties())
                {
                    row[property.Name] = property.Value.ToObject<object>();
                }

                results.Add(row);
            }
        }

        return results;
    }
}
