using MongoDB.Bson;

namespace NPipeline.Connectors.Azure.CosmosDb.Mapping;

/// <summary>
///     BSON-backed row wrapper for Cosmos Mongo API documents.
/// </summary>
public sealed class CosmosBsonRow : ICosmosDataWrapper
{
    private readonly BsonDocument _document;

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosBsonRow" />.
    /// </summary>
    /// <param name="document">The BSON document.</param>
    public CosmosBsonRow(BsonDocument document)
    {
        _document = document ?? throw new ArgumentNullException(nameof(document));
    }

    /// <inheritdoc />
    public T Get<T>(string name, T defaultValue = default!)
    {
        if (!TryGet(name, out T? value))
            return defaultValue;

        return value!;
    }

    /// <inheritdoc />
    public object? GetValue(string name)
    {
        if (!TryGetBsonValue(name, out var bsonValue))
            return null;

        return BsonTypeMapper.MapToDotNetValue(bsonValue);
    }

    /// <inheritdoc />
    public bool HasColumn(string name)
    {
        return TryGetBsonValue(name, out _);
    }

    /// <inheritdoc />
    public Dictionary<string, object?> ToDictionary()
    {
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        foreach (var element in _document.Elements)
        {
            result[element.Name] = BsonTypeMapper.MapToDotNetValue(element.Value);
        }

        return result;
    }

    private bool TryGet<T>(string name, out T? value)
    {
        value = default;

        if (!TryGetBsonValue(name, out var bsonValue))
            return false;

        try
        {
            var dotNetValue = BsonTypeMapper.MapToDotNetValue(bsonValue);

            if (dotNetValue is T typed)
            {
                value = typed;
                return true;
            }

            if (dotNetValue == null)
                return true;

            value = (T?)Convert.ChangeType(dotNetValue, typeof(T));
            return true;
        }
        catch
        {
            return false;
        }
    }

    private bool TryGetBsonValue(string name, out BsonValue value)
    {
        if (_document.TryGetValue(name, out value))
            return true;

        // Mongo uses _id while SQL conventions often use id.
        if (string.Equals(name, "id", StringComparison.OrdinalIgnoreCase) && _document.TryGetValue("_id", out value))
            return true;

        if (string.Equals(name, "_id", StringComparison.OrdinalIgnoreCase) && _document.TryGetValue("id", out value))
            return true;

        return false;
    }
}
