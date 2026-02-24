using System.Collections.Concurrent;
using NPipeline.Connectors.Azure.CosmosDb.Mapping;

namespace NPipeline.Connectors.Azure.CosmosDb.Nodes;

internal static class CosmosAdapterNodeMapper<T>
{
    private static readonly ConcurrentDictionary<Type, Func<CosmosRow, T>> MapperCache = new();

    public static T? Map(
        IDictionary<string, object?> row,
        Func<CosmosRow, T>? mapper,
        bool continueOnError)
    {
        try
        {
            if (typeof(T) == typeof(Dictionary<string, object?>))
                return (T)(object)new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase);

            if (typeof(T) == typeof(IDictionary<string, object?>))
                return (T)(object)new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase);

            var cosmosRow = new CosmosRow(new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase));

            if (typeof(T) == typeof(CosmosRow))
                return (T)(object)cosmosRow;

            if (mapper != null)
                return mapper(cosmosRow);

            var defaultMapper = MapperCache.GetOrAdd(typeof(T), _ => CosmosMapperBuilder.Build<T>());
            return defaultMapper(cosmosRow);
        }
        catch
        {
            if (!continueOnError)
                throw;

            return default;
        }
    }
}
