using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using MongoDB.Bson.Serialization.Attributes;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.MongoDB.Attributes;
using NPipeline.Connectors.MongoDB.Exceptions;

namespace NPipeline.Connectors.MongoDB.Mapping;

/// <summary>
///     Builds cached mapping delegates from <see cref="MongoRow" /> to CLR types using attributes.
///     Uses compiled delegates for optimal performance during document mapping.
/// </summary>
internal static class MongoMapperBuilder
{
    private static readonly ConcurrentDictionary<Type, Delegate> CachedMappers = new();

    /// <summary>
    ///     Gets or builds a cached mapper for the specified type.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <returns>A compiled delegate that maps MongoRow to T.</returns>
    public static Func<MongoRow, T> GetOrCreateMapper<T>()
    {
        return (Func<MongoRow, T>)CachedMappers.GetOrAdd(typeof(T), _ => Build<T>());
    }

    /// <summary>
    ///     Builds a mapping delegate for the specified type.
    ///     Property setters are compiled once via Expression trees for maximum performance.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <returns>A compiled delegate that maps MongoRow to T.</returns>
    public static Func<MongoRow, T> Build<T>()
    {
        var mappings = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && !IsIgnored(p))
            .Select(p => (
                FieldName: ResolveFieldName(p),
                p.PropertyType,
                Setter: BuildPropertySetter<T>(p)))
            .ToArray();

        // Compile a delegate for instance creation to avoid Activator.CreateInstance on each row
        var createInstance = BuildCreateInstanceDelegate<T>();

        return row =>
        {
            var instance = createInstance();

            foreach (var (fieldName, propertyType, setter) in mappings)
            {
                try
                {
                    if (!row.HasField(fieldName))
                        continue;

                    var value = row.Get<object?>(fieldName);
                    var convertedValue = ConvertValue(value, propertyType);
                    setter(instance, convertedValue);
                }
                catch (MongoMappingException)
                {
                    throw;
                }
                catch (Exception ex)
                {
                    throw new MongoMappingException(
                        $"Failed to map field '{fieldName}' to property of type '{propertyType.Name}'",
                        fieldName,
                        row.Document,
                        ex);
                }
            }

            return instance;
        };
    }

    /// <summary>
    ///     Compiles a fast property setter delegate using Expression trees.
    ///     Avoids the overhead of PropertyInfo.SetValue on every row.
    /// </summary>
    private static Action<T, object?> BuildPropertySetter<T>(PropertyInfo property)
    {
        var targetParam = Expression.Parameter(typeof(T), "target");
        var valueParam = Expression.Parameter(typeof(object), "value");

        // Cast the boxed object to the exact property type
        var castValue = Expression.Convert(valueParam, property.PropertyType);
        var assign = Expression.Assign(Expression.Property(targetParam, property), castValue);

        return Expression.Lambda<Action<T, object?>>(assign, targetParam, valueParam).Compile();
    }

    /// <summary>
    ///     Builds a compiled delegate for creating instances of type T.
    /// </summary>
    private static Func<T> BuildCreateInstanceDelegate<T>()
    {
        var ctor = typeof(T).GetConstructor(Type.EmptyTypes)
                   ?? throw new InvalidOperationException($"Type '{typeof(T).FullName}' does not have a parameterless constructor");

        var newExpression = Expression.New(ctor);
        return Expression.Lambda<Func<T>>(newExpression).Compile();
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
        return ignoredByAttribute || hasIgnoreMarker;
    }

    /// <summary>
    ///     Converts a value to the target type.
    /// </summary>
    private static object? ConvertValue(object? value, Type targetType)
    {
        if (value == null)
        {
            return targetType.IsValueType
                ? Activator.CreateInstance(targetType)
                : null;
        }

        var sourceType = value.GetType();

        if (targetType.IsAssignableFrom(sourceType))
            return value;

        // Handle nullable types
        var underlyingType = Nullable.GetUnderlyingType(targetType);

        if (underlyingType != null)
        {
            if (value == null)
                return null;

            return ConvertValue(value, underlyingType);
        }

        // Handle common conversions
        if (targetType == typeof(Guid) && value is string guidStr)
            return Guid.Parse(guidStr);

        if (targetType == typeof(DateTime) && value is DateTime dt)
            return dt;

        if (targetType == typeof(DateTimeOffset) && value is DateTime dto)
            return new DateTimeOffset(dto);

        // Handle numeric conversions
        if (IsNumericType(targetType) && IsNumericType(sourceType))
            return Convert.ChangeType(value, targetType);

        // Handle string conversion
        if (targetType == typeof(string))
            return value.ToString();

        // Fallback
        return Convert.ChangeType(value, targetType);
    }

    /// <summary>
    ///     Checks if a type is a numeric type.
    /// </summary>
    private static bool IsNumericType(Type type)
    {
        return type == typeof(int) ||
               type == typeof(long) ||
               type == typeof(short) ||
               type == typeof(byte) ||
               type == typeof(float) ||
               type == typeof(double) ||
               type == typeof(decimal) ||
               type == typeof(uint) ||
               type == typeof(ulong) ||
               type == typeof(ushort) ||
               type == typeof(sbyte);
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
