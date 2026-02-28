using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Snowflake.Configuration;

namespace NPipeline.Connectors.Snowflake.Mapping;

/// <summary>
///     Builds parameter mapping delegates for Snowflake commands.
/// </summary>
internal static class SnowflakeParameterMapper
{
    public static Action<System.Data.Common.DbParameterCollection, T> Build<T>(SnowflakeConfiguration configuration)
    {
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && !IsIgnored(p))
            .Select(p => new
            {
                Property = p,
                SnowflakeAttribute = p.GetCustomAttribute<SnowflakeColumnAttribute>(),
                ColumnAttribute = p.GetCustomAttribute<ColumnAttribute>(),
            })
            .ToList();

        var mappings = properties
            .Select(p => new
            {
                ColumnName = GetColumnName(p.Property, p.SnowflakeAttribute, p.ColumnAttribute),
                Getter = BuildGetter<T>(p.Property),
                Attribute = p.SnowflakeAttribute,
            })
            .ToList();

        return (parameters, item) =>
        {
            foreach (var mapping in mappings)
            {
                var value = mapping.Getter(item) ?? DBNull.Value;
                // Parameters are added via the command's AddParameter method in the writers
                // This delegate provides values for the writers to create parameters from
                parameters.Add(value);
            }
        };
    }

    /// <summary>
    ///     Gets the column name for a property based on attributes or convention.
    /// </summary>
    private static string GetColumnName(
        PropertyInfo property,
        SnowflakeColumnAttribute? attribute,
        ColumnAttribute? commonAttribute)
    {
        if (attribute != null && !string.IsNullOrWhiteSpace(attribute.Name))
            return attribute.Name;

        if (commonAttribute != null && !string.IsNullOrWhiteSpace(commonAttribute.Name))
            return commonAttribute.Name;

        return SnowflakeNamingConvention.ToDefaultColumnName(property.Name);
    }

    /// <summary>
    ///     Gets the column names for a type.
    /// </summary>
    public static string[] GetColumnNames<T>(SnowflakeConfiguration configuration)
    {
        return
        [
            .. typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead && !IsIgnored(p))
                .Select(p => GetColumnName(p,
                    p.GetCustomAttribute<SnowflakeColumnAttribute>(),
                    p.GetCustomAttribute<ColumnAttribute>())),
        ];
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var snowflakeAttribute = property.GetCustomAttribute<SnowflakeColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true || snowflakeAttribute?.Ignore == true;
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
