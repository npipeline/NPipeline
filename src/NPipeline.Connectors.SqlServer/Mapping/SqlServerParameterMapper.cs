using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Data.SqlClient;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.SqlServer.Configuration;

namespace NPipeline.Connectors.SqlServer.Mapping;

/// <summary>
///     Builds parameter mapping delegates for SQL Server commands.
/// </summary>
internal static class SqlServerParameterMapper
{
    public static Action<SqlParameterCollection, T> Build<T>(SqlServerConfiguration configuration)
    {
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && !IsIgnored(p))
            .Select(p => new
            {
                Property = p,
                SqlServerAttribute = p.GetCustomAttribute<SqlServerColumnAttribute>(),
                ColumnAttribute = p.GetCustomAttribute<ColumnAttribute>(),
            })
            .ToList();

        var mappings = properties
            .Select(p => new
            {
                ColumnName = GetColumnName(p.Property, p.SqlServerAttribute, p.ColumnAttribute),
                Getter = BuildGetter<T>(p.Property),
                Attribute = p.SqlServerAttribute,
            })
            .ToList();

        return (parameters, item) =>
        {
            foreach (var mapping in mappings)
            {
                var value = mapping.Getter(item) ?? DBNull.Value;
                var parameter = parameters.AddWithValue(mapping.ColumnName, value);

                if (mapping.Attribute?.DbTypeNullable is not null)
                    parameter.SqlDbType = mapping.Attribute.DbTypeNullable.Value;

                if (mapping.Attribute?.SizeNullable is not null)
                    parameter.Size = mapping.Attribute.SizeNullable.Value;
            }
        };
    }

    /// <summary>
    ///     Gets the column name for a property based on attributes or convention.
    /// </summary>
    private static string GetColumnName(
        PropertyInfo property,
        SqlServerColumnAttribute? attribute,
        ColumnAttribute? commonAttribute)
    {
        if (attribute != null && !string.IsNullOrWhiteSpace(attribute.Name))
            return attribute.Name;

        if (commonAttribute != null && !string.IsNullOrWhiteSpace(commonAttribute.Name))
            return commonAttribute.Name;

        // SQL Server uses PascalCase by convention
        return property.Name;
    }

    /// <summary>
    ///     Gets the column names for a type.
    /// </summary>
    public static string[] GetColumnNames<T>(SqlServerConfiguration configuration)
    {
        return
        [
            .. typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead && !IsIgnored(p))
                .Select(p => GetColumnName(p,
                    p.GetCustomAttribute<SqlServerColumnAttribute>(),
                    p.GetCustomAttribute<ColumnAttribute>())),
        ];
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var sqlServerAttribute = property.GetCustomAttribute<SqlServerColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true || sqlServerAttribute?.Ignore == true;
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
