using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.Aws.Redshift.Mapping;

/// <summary>
///     Builds compiled parameter mappers for extracting values from objects for database writes.
/// </summary>
public static class RedshiftParameterMapper
{
    private static readonly ConcurrentDictionary<(Type Type, RedshiftNamingConvention Convention), Delegate> Cache = new();

    /// <summary>
    ///     Builds or retrieves a cached mapper for extracting values from type T.
    /// </summary>
    /// <typeparam name="T">The type to extract values from.</typeparam>
    /// <param name="namingConvention">The naming convention to use for column name resolution.</param>
    /// <returns>A compiled delegate that extracts values from an instance of type T.</returns>
    public static Func<T, object?[]> BuildValueExtractor<T>(RedshiftNamingConvention namingConvention = RedshiftNamingConvention.PascalToSnakeCase)
    {
        var key = (typeof(T), namingConvention);
        return (Func<T, object?[]>)Cache.GetOrAdd(key, _ => BuildExtractor<T>(namingConvention));
    }

    /// <summary>
    ///     Gets column names for type T in the order they map to parameters.
    /// </summary>
    /// <typeparam name="T">The type to get column names for.</typeparam>
    /// <param name="namingConvention">The naming convention to use for column name resolution.</param>
    /// <returns>A list of column names in parameter order.</returns>
    public static IReadOnlyList<string> GetColumnNames<T>(RedshiftNamingConvention namingConvention = RedshiftNamingConvention.PascalToSnakeCase)
    {
        var names = new List<string>();
        var type = typeof(T);

        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!property.CanRead)
                continue;

            // Skip ignored properties
            var redshiftColumnAttr = property.GetCustomAttribute<RedshiftColumnAttribute>();
            var columnAttr = property.GetCustomAttribute<ColumnAttribute>();
            var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);

            if (redshiftColumnAttr?.Ignore == true || columnAttr?.Ignore == true || hasIgnoreMarker)
                continue;

            var columnName = RedshiftMapperBuilder.GetColumnName(property, namingConvention);
            names.Add(columnName);
        }

        return names;
    }

    /// <summary>
    ///     Clears the mapper cache. Useful for testing.
    /// </summary>
    public static void ClearCache()
    {
        Cache.Clear();
    }

    private static Func<T, object?[]> BuildExtractor<T>(RedshiftNamingConvention namingConvention)
    {
        var param = Expression.Parameter(typeof(T), "item");

        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead)
            .ToList();

        var filteredProperties = properties
            .Where(p =>
                p.GetCustomAttribute<RedshiftColumnAttribute>()?.Ignore != true
                && p.GetCustomAttribute<ColumnAttribute>()?.Ignore != true
                && !p.IsDefined(typeof(IgnoreColumnAttribute), true))
            .ToList();

        var expressions = new List<Expression>();
        var resultVar = Expression.Variable(typeof(object?[]), "result");

        expressions.Add(Expression.Assign(resultVar, Expression.NewArrayBounds(typeof(object), Expression.Constant(filteredProperties.Count))));

        for (var i = 0; i < filteredProperties.Count; i++)
        {
            var property = filteredProperties[i];
            var propertyAccess = Expression.Property(param, property);
            var boxed = Expression.Convert(propertyAccess, typeof(object));
            var indexAccess = Expression.ArrayAccess(resultVar, Expression.Constant(i));
            expressions.Add(Expression.Assign(indexAccess, boxed));
        }

        expressions.Add(resultVar);

        var body = Expression.Block([resultVar], expressions);
        var lambda = Expression.Lambda<Func<T, object?[]>>(body, param);

        return lambda.Compile();
    }
}
