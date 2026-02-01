using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using CsvHelper;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.Csv;

/// <summary>
///     Builds property mapping delegates for writing objects to CSV rows.
/// </summary>
public static class CsvWriterMapperBuilder
{
    private static readonly ConcurrentDictionary<Type, Delegate> MapperCache = new();
    private static readonly ConcurrentDictionary<Type, string[]> ColumnNamesCache = new();

    /// <summary>
    ///     Builds or retrieves a cached mapping delegate from type <typeparamref name="T" /> to <see cref="CsvWriter" />.
    /// </summary>
    /// <typeparam name="T">The source type to map to CSV rows.</typeparam>
    /// <returns>A delegate that writes an instance of type <typeparamref name="T" /> to a CSV row.</returns>
    public static Action<CsvWriter, T> Build<T>()
    {
        var type = typeof(T);

        if (MapperCache.TryGetValue(type, out var cachedDelegate))
            return (Action<CsvWriter, T>)cachedDelegate;

        var mapper = BuildMapper<T>();
        MapperCache.TryAdd(type, mapper);
        return mapper;
    }

    /// <summary>
    ///     Gets the CSV column names for properties of type <typeparamref name="T" />.
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

    private static Action<CsvWriter, T> BuildMapper<T>()
    {
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && !IsIgnored(p))
            .Select(p => new
            {
                Property = p,
                Attribute = p.GetCustomAttribute<ColumnAttribute>(),
            })
            .ToList();

        var mappings = properties
            .Select(p => new
            {
                ColumnName = p.Attribute?.Name ?? ToColumnNameConvention(p.Property.Name),
                Getter = BuildGetter<T>(p.Property),
            })
            .ToList();

        return (writer, item) =>
        {
            foreach (var mapping in mappings)
            {
                var value = mapping.Getter(item);
                writer.WriteField(value);
            }
        };
    }

    private static string[] BuildColumnNames<T>()
    {
        return
        [
            .. typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead && !IsIgnored(p))
                .Select(p => p.GetCustomAttribute<ColumnAttribute>()?.Name ?? ToColumnNameConvention(p.Name)),
        ];
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true;
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

    /// <summary>
    ///     Converts a PascalCase property name to a CSV column name convention.
    ///     Converts to lowercase by default (e.g., "FirstName" -> "firstname").
    /// </summary>
    private static string ToColumnNameConvention(string str)
    {
        return str.ToLowerInvariant();
    }
}
