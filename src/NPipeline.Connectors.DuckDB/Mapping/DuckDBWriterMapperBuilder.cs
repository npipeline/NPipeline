using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.DuckDB.Attributes;

namespace NPipeline.Connectors.DuckDB.Mapping;

/// <summary>
///     Builds column names and value getter delegates for writing CLR types to DuckDB.
///     Used by both the Appender and SQL writers.
/// </summary>
internal static class DuckDBWriterMapperBuilder
{
    private static readonly ConcurrentDictionary<Type, string[]> ColumnNamesCache = new();
    private static readonly ConcurrentDictionary<Type, Func<object, object?>[]> ValueGetterCache = new();
    private static readonly ConcurrentDictionary<Type, PropertyInfo[]> PropertiesCache = new();

    /// <summary>
    ///     Gets the ordered column names for the type.
    /// </summary>
    public static string[] GetColumnNames<T>()
    {
        return ColumnNamesCache.GetOrAdd(typeof(T), static type =>
        {
            var properties = GetWritableProperties(type);
            return properties.Select(DuckDBMapperBuilder.GetColumnName).ToArray();
        });
    }

    /// <summary>
    ///     Gets ordered compiled value getter delegates for the type.
    ///     Each delegate takes a boxed T and returns a boxed column value.
    /// </summary>
    public static Func<object, object?>[] GetValueGetters<T>()
    {
        return ValueGetterCache.GetOrAdd(typeof(T), static type =>
        {
            var properties = GetWritableProperties(type);
            return properties.Select(BuildValueGetter).ToArray();
        });
    }

    /// <summary>
    ///     Gets the writable properties for the type (ordered, filtered).
    /// </summary>
    public static PropertyInfo[] GetProperties<T>()
    {
        return PropertiesCache.GetOrAdd(typeof(T), static type => GetWritableProperties(type));
    }

    /// <summary>
    ///     Gets columns that are marked as primary keys (for upsert support).
    /// </summary>
    public static string[] GetPrimaryKeyColumns<T>()
    {
        var properties = GetWritableProperties(typeof(T));

        return properties
            .Where(p => p.GetCustomAttribute<DuckDBColumnAttribute>()?.PrimaryKey == true)
            .Select(DuckDBMapperBuilder.GetColumnName)
            .ToArray();
    }

    /// <summary>
    ///     Clears all caches. Primarily for testing.
    /// </summary>
    internal static void ClearCache()
    {
        ColumnNamesCache.Clear();
        ValueGetterCache.Clear();
        PropertiesCache.Clear();
    }

    private static PropertyInfo[] GetWritableProperties(Type type)
    {
        return type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && !DuckDBMapperBuilder.IsIgnored(p))
            .ToArray();
    }

    private static Func<object, object?> BuildValueGetter(PropertyInfo property)
    {
        // Build: (object obj) => (object?)((T)obj).Property
        var objParam = Expression.Parameter(typeof(object), "obj");
        var castToType = Expression.Convert(objParam, property.DeclaringType!);
        var propertyAccess = Expression.Property(castToType, property);

        // Apply type conversions for DuckDB compatibility
        var converted = GetConvertedExpression(propertyAccess, property.PropertyType);
        var boxed = Expression.Convert(converted, typeof(object));

        return Expression.Lambda<Func<object, object?>>(boxed, objParam).Compile();
    }

    private static Expression GetConvertedExpression(Expression propertyAccess, Type propertyType)
    {
        var underlyingType = Nullable.GetUnderlyingType(propertyType);
        var effectiveType = underlyingType ?? propertyType;

        // Guid → string
        if (effectiveType == typeof(Guid))
        {
            if (underlyingType is not null)
            {
                // Nullable<Guid>: value.HasValue ? value.Value.ToString() : null
                var hasValue = Expression.Property(propertyAccess, "HasValue");
                var getValue = Expression.Property(propertyAccess, "Value");
                var toString = Expression.Call(getValue, typeof(Guid).GetMethod("ToString", Type.EmptyTypes)!);

                return Expression.Condition(hasValue,
                    Expression.Convert(toString, typeof(object)),
                    Expression.Constant(null, typeof(object)));
            }

            return Expression.Call(propertyAccess, typeof(Guid).GetMethod("ToString", Type.EmptyTypes)!);
        }

        // Enum → string
        if (effectiveType.IsEnum)
        {
            if (underlyingType is not null)
            {
                var hasValue = Expression.Property(propertyAccess, "HasValue");
                var getValue = Expression.Property(propertyAccess, "Value");
                var toString = Expression.Call(getValue, typeof(object).GetMethod("ToString")!);

                return Expression.Condition(hasValue,
                    Expression.Convert(toString, typeof(object)),
                    Expression.Constant(null, typeof(object)));
            }

            return Expression.Call(propertyAccess, typeof(object).GetMethod("ToString")!);
        }

        // DateOnly → DateTime
        if (effectiveType == typeof(DateOnly))
        {
            var toDateTimeMethod = typeof(DateOnly).GetMethod("ToDateTime", new[] { typeof(TimeOnly) })!;

            if (underlyingType is not null)
            {
                var hasValue = Expression.Property(propertyAccess, "HasValue");
                var getValue = Expression.Property(propertyAccess, "Value");

                var toDateTime = Expression.Call(getValue, toDateTimeMethod,
                    Expression.Constant(TimeOnly.MinValue));

                return Expression.Condition(hasValue,
                    Expression.Convert(toDateTime, typeof(object)),
                    Expression.Constant(null, typeof(object)));
            }

            return Expression.Call(propertyAccess, toDateTimeMethod,
                Expression.Constant(TimeOnly.MinValue));
        }

        // DateTimeOffset → DateTime (UTC)
        if (effectiveType == typeof(DateTimeOffset))
        {
            if (underlyingType is not null)
            {
                var hasValue = Expression.Property(propertyAccess, "HasValue");
                var getValue = Expression.Property(propertyAccess, "Value");
                var utcDateTime = Expression.Property(getValue, "UtcDateTime");

                return Expression.Condition(hasValue,
                    Expression.Convert(utcDateTime, typeof(object)),
                    Expression.Constant(null, typeof(object)));
            }

            return Expression.Property(propertyAccess, "UtcDateTime");
        }

        return propertyAccess;
    }
}
