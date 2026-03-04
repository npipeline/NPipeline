using System.Linq.Expressions;
using System.Reflection;
using MySqlConnector;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.MySql.Configuration;

namespace NPipeline.Connectors.MySql.Mapping;

/// <summary>
///     Builds parameter mapping delegates for MySQL sink commands.
/// </summary>
internal static class MySqlParameterMapper
{
    public static Action<MySqlParameterCollection, T> Build<T>(MySqlConfiguration configuration)
    {
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && !IsIgnored(p))
            .Select(p => new
            {
                Property = p,
                MySqlAttribute = p.GetCustomAttribute<MySqlColumnAttribute>(),
                ColumnAttribute = p.GetCustomAttribute<ColumnAttribute>(),
            })
            .ToList();

        var mappings = properties
            .Select(p => new
            {
                ColumnName = GetColumnName(p.Property, p.MySqlAttribute, p.ColumnAttribute),
                Getter = BuildGetter<T>(p.Property),
            })
            .ToList();

        return (parameters, item) =>
        {
            foreach (var mapping in mappings)
            {
                var value = mapping.Getter(item) ?? DBNull.Value;
                parameters.AddWithValue(mapping.ColumnName, value);
            }
        };
    }

    public static string[] GetColumnNames<T>(MySqlConfiguration configuration)
    {
        return
        [
            .. typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead && !IsIgnored(p))
                .Select(p => GetColumnName(p,
                    p.GetCustomAttribute<MySqlColumnAttribute>(),
                    p.GetCustomAttribute<ColumnAttribute>())),
        ];
    }

    private static string GetColumnName(
        PropertyInfo property,
        MySqlColumnAttribute? mySqlAttribute,
        ColumnAttribute? commonAttribute)
    {
        if (mySqlAttribute != null && !string.IsNullOrWhiteSpace(mySqlAttribute.Name))
            return mySqlAttribute.Name;

        if (commonAttribute != null && !string.IsNullOrWhiteSpace(commonAttribute.Name))
            return commonAttribute.Name;

        return property.Name;
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var mySqlAttribute = property.GetCustomAttribute<MySqlColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true || mySqlAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);
        return ignoredByAttribute || hasIgnoreMarker;
    }

    private static Func<T, object?> BuildGetter<T>(PropertyInfo property)
    {
        var instanceParam = Expression.Parameter(typeof(T), "item");
        var propertyAccess = Expression.Property(instanceParam, property);
        var convert = Expression.Convert(propertyAccess, typeof(object));
        return Expression.Lambda<Func<T, object?>>(convert, instanceParam).Compile();
    }
}
