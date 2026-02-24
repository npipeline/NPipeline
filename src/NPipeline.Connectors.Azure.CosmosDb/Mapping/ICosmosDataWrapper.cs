namespace NPipeline.Connectors.Azure.CosmosDb.Mapping;

/// <summary>
///     Unified data wrapper abstraction for SQL (JSON), Mongo (BSON), and Cassandra rows.
/// </summary>
public interface ICosmosDataWrapper
{
    /// <summary>
    ///     Gets a typed value by name with default fallback.
    /// </summary>
    T Get<T>(string name, T defaultValue = default!);

    /// <summary>
    ///     Gets a raw value by name.
    /// </summary>
    object? GetValue(string name);

    /// <summary>
    ///     Determines whether a field exists.
    /// </summary>
    bool HasColumn(string name);

    /// <summary>
    ///     Converts the wrapped row into a dictionary.
    /// </summary>
    Dictionary<string, object?> ToDictionary();
}
