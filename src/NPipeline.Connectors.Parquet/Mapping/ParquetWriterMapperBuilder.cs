using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace NPipeline.Connectors.Parquet.Mapping;

/// <summary>
///     Builds property mapping delegates for writing objects to Parquet column arrays data.
/// </summary>
public static class ParquetWriterMapperBuilder
{
    private static readonly ConcurrentDictionary<Type, object> ValueGetterCache = new();
    private static readonly ConcurrentDictionary<Type, string[]> ColumnNamesCache = new();
    private static readonly ConcurrentDictionary<Type, PropertyInfo[]> PropertiesCache = new();

    /// <summary>
    ///     Gets the column names for properties of type <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">The type to get column names for.</typeparam>
    /// <returns>An array of column names corresponding to the properties of type <typeparamref name="T" />.</returns>
    public static string[] GetColumnNames<T>()
    {
        var type = typeof(T);

        if (ColumnNamesCache.TryGetValue(type, out var cachedColumnNames))
            return cachedColumnNames;

        var columnNames = BuildColumnNames<T>();
        ColumnNamesCache.TryAdd(type, columnNames);
        return columnNames;
    }

    /// <summary>
    ///     Gets the value getters for properties of type <typeparamref name="T" />.
    ///     Each getter extracts a value from an instance and converts it to the appropriate Parquet representation.
    /// </summary>
    /// <typeparam name="T">The type to get value getters for.</typeparam>
    /// <returns>An array of functions that extract values from instances of type <typeparamref name="T" />.</returns>
    public static Func<T, object?>[] GetValueGetters<T>()
    {
        var type = typeof(T);

        if (ValueGetterCache.TryGetValue(type, out var cachedGetters))
            return (Func<T, object?>[])cachedGetters;

        var getters = BuildValueGetters<T>();
        ValueGetterCache.TryAdd(type, getters);
        return getters;
    }

    /// <summary>
    ///     Gets the writable properties for type <typeparamref name="T" /> that should be included in Parquet output.
    /// </summary>
    /// <typeparam name="T">The type to get properties for.</typeparam>
    /// <returns>An array of properties that should be written to Parquet.</returns>
    public static PropertyInfo[] GetProperties<T>()
    {
        var type = typeof(T);

        if (PropertiesCache.TryGetValue(type, out var cachedProperties))
            return cachedProperties;

        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && !ParquetSchemaBuilder.IsIgnored(p))
            .ToArray();

        PropertiesCache.TryAdd(type, properties);
        return properties;
    }

    private static string[] BuildColumnNames<T>()
    {
        return GetProperties<T>()
            .Select(ParquetSchemaBuilder.GetColumnName)
            .ToArray();
    }

    private static Func<T, object?>[] BuildValueGetters<T>()
    {
        var properties = GetProperties<T>();

        return properties
            .Select(p => BuildValueGetter<T>(p))
            .ToArray();
    }

    private static Func<T, object?> BuildValueGetter<T>(PropertyInfo property)
    {
        var instanceParam = Expression.Parameter(typeof(T), "item");
        var propertyAccess = Expression.Property(instanceParam, property);

        // Handle special conversions for Parquet compatibility
        var convertedAccess = GetConvertedExpression(propertyAccess, property.PropertyType);

        var convert = Expression.Convert(convertedAccess, typeof(object));
        return Expression.Lambda<Func<T, object?>>(convert, instanceParam).Compile();
    }

    /// <summary>
    ///     Gets an expression that converts the property value to the appropriate Parquet representation.
    /// </summary>
    private static Expression GetConvertedExpression(Expression propertyAccess, Type propertyType)
    {
        var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

        // Handle special type conversions for Parquet
        if (underlyingType == typeof(Guid))
        {
            // Convert Guid to string
            var toStringMethod = typeof(Guid).GetMethod("ToString", Type.EmptyTypes)!;

            // Handle nullable Guid
            if (propertyType != underlyingType)
            {
                var hasValue = Expression.Property(propertyAccess, "HasValue");
                var value = Expression.Property(propertyAccess, "Value");
                var toString = Expression.Call(value, toStringMethod);
                return Expression.Condition(hasValue, toString, Expression.Constant(null, typeof(string)));
            }

            return Expression.Call(propertyAccess, toStringMethod);
        }

        if (underlyingType.IsEnum)
        {
            // Convert enum to string
            var toStringMethod = typeof(object).GetMethod("ToString", Type.EmptyTypes)!;

            // Handle nullable enum
            if (propertyType != underlyingType)
            {
                var hasValue = Expression.Property(propertyAccess, "HasValue");
                var value = Expression.Property(propertyAccess, "Value");
                var toString = Expression.Call(value, toStringMethod);
                return Expression.Condition(hasValue, toString, Expression.Constant(null, typeof(string)));
            }

            return Expression.Call(propertyAccess, toStringMethod);
        }

        if (underlyingType == typeof(DateOnly))
        {
            // DateOnly is stored as DateTime in Parquet
            var toDateTimeMethod = typeof(DateOnly).GetMethod("ToDateTime", [typeof(TimeOnly)])!;
            var timeOnlyMidnight = Expression.Constant(TimeOnly.MinValue);

            // Handle nullable DateOnly
            if (propertyType != underlyingType)
            {
                var hasValue = Expression.Property(propertyAccess, "HasValue");
                var value = Expression.Property(propertyAccess, "Value");
                var toDateTime = Expression.Call(value, toDateTimeMethod, timeOnlyMidnight);
                return Expression.Condition(hasValue, toDateTime, Expression.Constant(null, typeof(DateTime?)));
            }

            return Expression.Call(propertyAccess, toDateTimeMethod, timeOnlyMidnight);
        }

        if (underlyingType == typeof(TimeOnly))
        {
            // TimeOnly is stored as TimeSpan in Parquet
            var toTimeSpanMethod = typeof(TimeOnly).GetMethod("ToTimeSpan")!;

            // Handle nullable TimeOnly
            if (propertyType != underlyingType)
            {
                var hasValue = Expression.Property(propertyAccess, "HasValue");
                var value = Expression.Property(propertyAccess, "Value");
                var toTimeSpan = Expression.Call(value, toTimeSpanMethod);
                return Expression.Condition(hasValue, toTimeSpan, Expression.Constant(null, typeof(TimeSpan?)));
            }

            return Expression.Call(propertyAccess, toTimeSpanMethod);
        }

        if (underlyingType == typeof(DateTimeOffset))
        {
            // DateTimeOffset is stored as DateTime (UTC) in Parquet
            var utcDateTimeProperty = typeof(DateTimeOffset).GetProperty("UtcDateTime")!;

            // Handle nullable DateTimeOffset
            if (propertyType != underlyingType)
            {
                var hasValue = Expression.Property(propertyAccess, "HasValue");
                var value = Expression.Property(propertyAccess, "Value");
                var utcDateTime = Expression.Property(value, utcDateTimeProperty);
                return Expression.Condition(hasValue, utcDateTime, Expression.Constant(null, typeof(DateTime?)));
            }

            return Expression.Property(propertyAccess, utcDateTimeProperty);
        }

        // No special conversion needed
        return propertyAccess;
    }

    /// <summary>
    ///     Creates a dictionary mapping column names to property info for type <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">The type to create the mapping for.</typeparam>
    /// <returns>A dictionary mapping column names to property info.</returns>
    public static Dictionary<string, PropertyInfo> GetColumnToPropertyMap<T>()
    {
        return GetProperties<T>()
            .ToDictionary(ParquetSchemaBuilder.GetColumnName, p => p);
    }
}
