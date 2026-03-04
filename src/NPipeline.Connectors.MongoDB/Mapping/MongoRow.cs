using MongoDB.Bson;

namespace NPipeline.Connectors.MongoDB.Mapping;

/// <summary>
///     Provides typed access to a MongoDB document wrapped in a BsonDocument.
///     Acts as a thin wrapper with convenient typed getters.
/// </summary>
public sealed class MongoRow
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="MongoRow" /> class.
    /// </summary>
    /// <param name="document">The underlying BSON document.</param>
    public MongoRow(BsonDocument document)
    {
        Document = document ?? throw new ArgumentNullException(nameof(document));
    }

    /// <summary>
    ///     Gets the underlying BSON document.
    /// </summary>
    public BsonDocument Document { get; }

    /// <summary>
    ///     Gets the document's ObjectId from the _id field, if present and of ObjectId BSON type.
    ///     Returns null if the field is missing or holds a different type (e.g., string, int).
    /// </summary>
    public ObjectId? Id
    {
        get
        {
            if (!Document.TryGetValue("_id", out var idVal))
                return null;

            return idVal.BsonType == BsonType.ObjectId
                ? idVal.AsObjectId
                : null;
        }
    }

    /// <summary>
    ///     Gets the names of all fields in the document.
    /// </summary>
    public IEnumerable<string> FieldNames => Document.Names;

    /// <summary>
    ///     Gets the number of fields in the document.
    /// </summary>
    public int FieldCount => Document.ElementCount;

    /// <summary>
    ///     Checks whether the document contains the specified field.
    /// </summary>
    /// <param name="name">The field name.</param>
    /// <returns>True if the field exists; otherwise false.</returns>
    public bool HasField(string name)
    {
        return Document.Contains(name);
    }

    /// <summary>
    ///     Gets the value of the specified field as type <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <param name="name">The field name.</param>
    /// <param name="defaultValue">The default value if the field is missing or null.</param>
    /// <param name="caseInsensitive">Whether to perform case-insensitive field matching.</param>
    /// <returns>The field value.</returns>
    public T Get<T>(string name, T defaultValue = default!, bool caseInsensitive = false)
    {
        if (!TryGet(name, out var value, defaultValue, caseInsensitive))
            return defaultValue;

        return value!;
    }

    /// <summary>
    ///     Attempts to get a field value by name.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <param name="name">The field name.</param>
    /// <param name="value">The output value.</param>
    /// <param name="defaultValue">The default value if the field is missing or null.</param>
    /// <param name="caseInsensitive">Whether to perform case-insensitive field matching.</param>
    /// <returns>True if the field was found and has a value; otherwise false.</returns>
    public bool TryGet<T>(string name, out T? value, T defaultValue = default!, bool caseInsensitive = false)
    {
        // Find the field with optional case-insensitive matching
        var actualName = caseInsensitive
            ? Document.Names.FirstOrDefault(n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase))
            : name;

        if (actualName == null)
        {
            value = defaultValue;
            return false;
        }

        if (!Document.TryGetValue(actualName, out var bsonValue))
        {
            value = defaultValue;
            return false;
        }

        if (bsonValue == BsonNull.Value)
        {
            value = defaultValue;
            return false;
        }

        value = ConvertBsonValue<T>(bsonValue);
        return true;
    }

    /// <summary>
    ///     Gets the value of the specified field as a string.
    /// </summary>
    public string GetString(string name, string defaultValue = "")
    {
        return Get(name, defaultValue);
    }

    /// <summary>
    ///     Gets the value of the specified field as an Int32.
    /// </summary>
    public int GetInt32(string name, int defaultValue = 0)
    {
        return Get(name, defaultValue);
    }

    /// <summary>
    ///     Gets the value of the specified field as an Int64.
    /// </summary>
    public long GetInt64(string name, long defaultValue = 0)
    {
        return Get(name, defaultValue);
    }

    /// <summary>
    ///     Gets the value of the specified field as a double.
    /// </summary>
    public double GetDouble(string name, double defaultValue = 0)
    {
        return Get(name, defaultValue);
    }

    /// <summary>
    ///     Gets the value of the specified field as a decimal.
    /// </summary>
    public decimal GetDecimal(string name, decimal defaultValue = 0)
    {
        return Get(name, defaultValue);
    }

    /// <summary>
    ///     Gets the value of the specified field as a boolean.
    /// </summary>
    public bool GetBoolean(string name, bool defaultValue = false)
    {
        return Get(name, defaultValue);
    }

    /// <summary>
    ///     Gets the value of the specified field as a DateTime.
    /// </summary>
    public DateTime GetDateTime(string name, DateTime defaultValue = default)
    {
        return Get(name, defaultValue);
    }

    /// <summary>
    ///     Gets the value of the specified field as a Guid.
    /// </summary>
    public Guid GetGuid(string name, Guid defaultValue = default)
    {
        return Get(name, defaultValue);
    }

    /// <summary>
    ///     Gets the value of the specified field as a nested BsonDocument wrapped in a MongoRow.
    /// </summary>
    public MongoRow GetDocument(string name)
    {
        if (!Document.Contains(name) || Document[name] == BsonNull.Value)
            throw new KeyNotFoundException($"Field '{name}' not found or is null.");

        return new MongoRow(Document[name].AsBsonDocument);
    }

    /// <summary>
    ///     Gets the value of the specified field as a BsonArray.
    /// </summary>
    public BsonArray GetArray(string name)
    {
        if (!Document.Contains(name) || Document[name] == BsonNull.Value)
            throw new KeyNotFoundException($"Field '{name}' not found or is null.");

        return Document[name].AsBsonArray;
    }

    /// <summary>
    ///     Gets the raw BSON value for the specified field.
    /// </summary>
    public BsonValue GetBsonValue(string name)
    {
        return Document.Contains(name)
            ? Document[name]
            : BsonNull.Value;
    }

    /// <summary>
    ///     Determines whether the specified field is null or missing.
    /// </summary>
    public bool IsNullOrMissing(string name)
    {
        return !Document.Contains(name) || Document[name] == BsonNull.Value;
    }

    private static T ConvertBsonValue<T>(BsonValue bsonValue)
    {
        var targetType = typeof(T);

        // Direct BSON type conversions
        if (bsonValue is T typedValue)
            return typedValue;

        // Handle specific conversions
        if (targetType == typeof(string))
            return (T)(object)bsonValue.ToString()!;

        if (targetType == typeof(int))
            return (T)(object)bsonValue.AsInt32;

        if (targetType == typeof(long))
        {
            // Handle both Int32 and Int64 BSON values
            var longValue = bsonValue.BsonType == BsonType.Int64
                ? bsonValue.AsInt64
                : bsonValue.AsInt32;

            return (T)(object)longValue;
        }

        if (targetType == typeof(double))
            return (T)(object)bsonValue.AsDouble;

        if (targetType == typeof(decimal))
        {
            // Handle Decimal128 and double
            if (bsonValue.BsonType == BsonType.Decimal128)
                return (T)(object)(decimal)bsonValue.AsDecimal128;

            return (T)(object)(decimal)bsonValue.AsDouble;
        }

        if (targetType == typeof(bool))
            return (T)(object)bsonValue.AsBoolean;

        if (targetType == typeof(DateTime))
        {
            var bsonDateTime = bsonValue.ToUniversalTime();
            return (T)(object)bsonDateTime;
        }

        if (targetType == typeof(Guid))
        {
            if (bsonValue.BsonType == BsonType.Binary)
            {
                var binary = bsonValue.AsBsonBinaryData;
                return (T)(object)new Guid(binary.Bytes);
            }

            return (T)(object)Guid.Parse(bsonValue.AsString);
        }

        if (targetType == typeof(ObjectId))
            return (T)(object)bsonValue.AsObjectId;

        // Fallback to BsonTypeMapper conversion
        var dotNetValue = BsonTypeMapper.MapToDotNetValue(bsonValue);
        return (T)dotNetValue;
    }
}
