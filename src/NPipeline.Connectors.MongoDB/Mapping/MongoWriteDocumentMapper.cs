using System.Collections.Concurrent;
using System.Reflection;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.MongoDB.Attributes;

namespace NPipeline.Connectors.MongoDB.Mapping;

/// <summary>
///     Builds cached mapping delegates from CLR types to BsonDocument for write operations.
///     Uses compiled delegates for optimal performance during document mapping.
/// </summary>
internal static class MongoWriteDocumentMapper
{
    private static readonly ConcurrentDictionary<Type, Delegate> CachedMappers = new();

    /// <summary>
    ///     Gets or builds a cached mapper for the specified type.
    /// </summary>
    /// <typeparam name="T">The source type.</typeparam>
    /// <returns>A compiled delegate that maps T to BsonDocument.</returns>
    public static Func<T, BsonDocument> GetOrCreateMapper<T>()
    {
        return (Func<T, BsonDocument>)CachedMappers.GetOrAdd(typeof(T), _ => Build<T>());
    }

    /// <summary>
    ///     Builds a mapping delegate for the specified type.
    /// </summary>
    /// <typeparam name="T">The source type.</typeparam>
    /// <returns>A compiled delegate that maps T to BsonDocument.</returns>
    public static Func<T, BsonDocument> Build<T>()
    {
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && !IsIgnored(p))
            .Select(p => new
            {
                Property = p,
                FieldName = ResolveFieldName(p),
            })
            .ToList();

        return entity =>
        {
            var document = new BsonDocument();

            foreach (var mapping in properties)
            {
                var value = mapping.Property.GetValue(entity);
                var bsonValue = ConvertToBsonValue(value);
                document[mapping.FieldName] = bsonValue;
            }

            return document;
        };
    }

    /// <summary>
    ///     Maps an entity to a BsonDocument using the cached mapper.
    /// </summary>
    /// <typeparam name="T">The source type.</typeparam>
    /// <param name="entity">The entity to map.</param>
    /// <returns>The mapped BsonDocument.</returns>
    public static BsonDocument Map<T>(T entity)
    {
        if (entity == null)
            throw new ArgumentNullException(nameof(entity));

        var mapper = GetOrCreateMapper<T>();
        return mapper(entity);
    }

    /// <summary>
    ///     Maps a collection of entities to BsonDocuments.
    /// </summary>
    /// <typeparam name="T">The source type.</typeparam>
    /// <param name="entities">The entities to map.</param>
    /// <returns>An enumerable of BsonDocuments.</returns>
    public static IEnumerable<BsonDocument> MapAll<T>(IEnumerable<T> entities)
    {
        ArgumentNullException.ThrowIfNull(entities);

        var mapper = GetOrCreateMapper<T>();
        return entities.Select(mapper);
    }

    /// <summary>
    ///     Resolves the MongoDB field name for a property using attributes or convention.
    /// </summary>
    private static string ResolveFieldName(PropertyInfo property)
    {
        // Check for MongoFieldAttribute first
        var mongoFieldAttr = property.GetCustomAttribute<MongoFieldAttribute>();

        if (mongoFieldAttr != null)
            return mongoFieldAttr.Name;

        // Check for generic ColumnAttribute
        var columnAttr = property.GetCustomAttribute<ColumnAttribute>();

        if (columnAttr != null)
            return columnAttr.Name;

        // Check for BsonElementAttribute
        var bsonElementAttr = property.GetCustomAttribute<BsonElementAttribute>();

        if (bsonElementAttr != null)
            return bsonElementAttr.ElementName;

        // Check for BsonIdAttribute - map to _id
        if (property.GetCustomAttribute<BsonIdAttribute>() != null)
            return "_id";

        // Fall back to camelCase convention
        return ToCamelCase(property.Name);
    }

    /// <summary>
    ///     Determines whether a property should be ignored during mapping.
    /// </summary>
    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var mongoFieldAttribute = property.GetCustomAttribute<MongoFieldAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true || mongoFieldAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);

        // Also check for BsonIgnoreAttribute
        var hasBsonIgnore = property.GetCustomAttribute<BsonIgnoreAttribute>() != null;

        return ignoredByAttribute || hasIgnoreMarker || hasBsonIgnore;
    }

    /// <summary>
    ///     Converts a CLR value to a BsonValue.
    /// </summary>
    private static BsonValue ConvertToBsonValue(object? value)
    {
        if (value == null)
            return BsonNull.Value;

        return value switch
        {
            string s => new BsonString(s),
            int i => new BsonInt32(i),
            long l => new BsonInt64(l),
            double d => new BsonDouble(d),
            decimal dec => new BsonDecimal128(dec),
            bool b => new BsonBoolean(b),
            DateTime dt => new BsonDateTime(dt.ToUniversalTime()),
            DateTimeOffset dto => new BsonDateTime(dto.UtcDateTime),
            Guid g => new BsonBinaryData(g.ToByteArray(), BsonBinarySubType.UuidStandard),
            byte[] bytes => new BsonBinaryData(bytes),
            ObjectId oid => new BsonObjectId(oid),
            BsonDocument doc => doc,
            BsonArray arr => arr,
            _ => BsonTypeMapper.MapToBsonValue(value),
        };
    }

    /// <summary>
    ///     Converts a string to camelCase.
    /// </summary>
    private static string ToCamelCase(string str)
    {
        if (string.IsNullOrEmpty(str))
            return str;

        return char.ToLowerInvariant(str[0]) + str.Substring(1);
    }

    /// <summary>
    ///     Clears all cached mappers.
    ///     Use this if the mapping configuration has changed at runtime.
    /// </summary>
    public static void ClearCache()
    {
        CachedMappers.Clear();
    }
}
