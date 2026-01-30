using System.Linq.Expressions;
using System.Reflection;
using Npgsql;

namespace NPipeline.Connectors.PostgreSQL.Mapping;

/// <summary>
///     Builds parameter mapping delegates for PostgreSQL commands and COPY importers.
/// </summary>
internal static class PostgresParameterMapper
{
    public static Action<NpgsqlParameterCollection, T> Build<T>()
    {
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && !IsIgnored(p))
            .Select(p => new
            {
                Property = p,
                Attribute = p.GetCustomAttribute<PostgresColumnAttribute>(),
            })
            .ToList();

        var mappings = properties
            .Select(p => new
            {
                ColumnName = p.Attribute?.Name ?? p.Property.Name,
                Getter = BuildGetter<T>(p.Property),
                p.Attribute,
            })
            .ToList();

        return (parameters, item) =>
        {
            foreach (var mapping in mappings)
            {
                var value = mapping.Getter(item) ?? DBNull.Value;
                var parameter = parameters.AddWithValue(mapping.ColumnName, value);

                if (mapping.Attribute?.DbType is not null)
                    parameter.NpgsqlDbType = mapping.Attribute.DbType.Value;

                if (mapping.Attribute?.Size is not null)
                    parameter.Size = mapping.Attribute.Size.Value;
            }
        };
    }

    public static Action<NpgsqlBinaryImporter, T> BuildCopyMapper<T>()
    {
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && !IsIgnored(p))
            .Select(p => new
            {
                Property = p,
                Attribute = p.GetCustomAttribute<PostgresColumnAttribute>(),
            })
            .ToList();

        var mappings = properties
            .Select(p => new
            {
                Getter = BuildGetter<T>(p.Property),
                p.Attribute,
            })
            .ToList();

        return (importer, item) =>
        {
            foreach (var mapping in mappings)
            {
                var value = mapping.Getter(item);

                if (value is null or DBNull)
                {
                    importer.WriteNull();
                    continue;
                }

                if (mapping.Attribute?.DbType is not null)
                    importer.Write(value, mapping.Attribute.DbType.Value);
                else
                    importer.Write(value);
            }
        };
    }

    public static string[] GetColumnNames<T>()
    {
        return
        [
            .. typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead && !IsIgnored(p))
                .Select(p => p.GetCustomAttribute<PostgresColumnAttribute>()?.Name ?? p.Name),
        ];
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<PostgresColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(PostgresIgnoreAttribute), true);
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
