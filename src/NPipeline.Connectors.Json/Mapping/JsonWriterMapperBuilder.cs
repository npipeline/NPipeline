using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json.Serialization;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.Json.Mapping;

/// <summary>
///     Builds property mapping delegates for writing objects to JSON.
/// </summary>
public static class JsonWriterMapperBuilder
{
    private static readonly ConcurrentDictionary<(Type, JsonPropertyNamingPolicy), Delegate[]> GetterCache = new();
    private static readonly ConcurrentDictionary<(Type, JsonPropertyNamingPolicy), string[]> PropertyNamesCache = new();

    /// <summary>
    ///     Gets the JSON property names for properties of type <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">The type to get property names for.</typeparam>
    /// <param name="namingPolicy">The property naming policy to use for property name transformation.</param>
    /// <returns>An array of property names corresponding to the properties of type <typeparamref name="T" />.</returns>
    public static string[] GetPropertyNames<T>(JsonPropertyNamingPolicy namingPolicy = JsonPropertyNamingPolicy.LowerCase)
    {
        var type = typeof(T);
        var cacheKey = (type, namingPolicy);

        if (PropertyNamesCache.TryGetValue(cacheKey, out var cachedPropertyNames))
            return cachedPropertyNames;

        var propertyNames = BuildPropertyNames<T>(namingPolicy);
        PropertyNamesCache.TryAdd(cacheKey, propertyNames);
        return propertyNames;
    }

    /// <summary>
    ///     Gets the value getter delegates for properties of type <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">The type to get value getters for.</typeparam>
    /// <param name="namingPolicy">The property naming policy to use for property name transformation.</param>
    /// <returns>An array of getter delegates that retrieve property values from instances of type <typeparamref name="T" />.</returns>
    public static Func<T, object?>[] GetValueGetters<T>(JsonPropertyNamingPolicy namingPolicy = JsonPropertyNamingPolicy.LowerCase)
    {
        var type = typeof(T);
        var cacheKey = (type, namingPolicy);

        if (GetterCache.TryGetValue(cacheKey, out var cachedGetters))
            return (Func<T, object?>[])cachedGetters;

        var getters = BuildValueGetters<T>(namingPolicy);
        GetterCache.TryAdd(cacheKey, getters);
        return getters;
    }

    private static string[] BuildPropertyNames<T>(JsonPropertyNamingPolicy namingPolicy)
    {
        return
        [
            .. typeof(T)
                .GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(p => p.CanRead && !IsIgnored(p))
                .Select(p => GetJsonPropertyName(p, p.GetCustomAttribute<ColumnAttribute>(), p.GetCustomAttribute<JsonPropertyNameAttribute>(), namingPolicy)),
        ];
    }

    private static Func<T, object?>[] BuildValueGetters<T>(JsonPropertyNamingPolicy namingPolicy)
    {
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && !IsIgnored(p))
            .Select(p => new
            {
                Property = p,
                ColumnAttribute = p.GetCustomAttribute<ColumnAttribute>(),
                JsonPropertyNameAttribute = p.GetCustomAttribute<JsonPropertyNameAttribute>(),
            })
            .ToList();

        return properties
            .Select(p => BuildGetter<T>(p.Property))
            .ToArray();
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var ignoredByColumnAttribute = columnAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);

        // Check for JsonIgnore attribute from System.Text.Json
        var jsonIgnoreAttribute = property.GetCustomAttribute<JsonIgnoreAttribute>();
        var ignoredByJsonIgnore = jsonIgnoreAttribute is not null;

        return ignoredByColumnAttribute || hasIgnoreMarker || ignoredByJsonIgnore;
    }

    private static Func<T, object?> BuildGetter<T>(PropertyInfo property)
    {
        var instanceParam = Expression.Parameter(typeof(T), "item");
        var propertyAccess = Expression.Property(instanceParam, property);
        var convert = Expression.Convert(propertyAccess, typeof(object));
        return Expression.Lambda<Func<T, object?>>(convert, instanceParam).Compile();
    }

    private static string GetJsonPropertyName(PropertyInfo property, ColumnAttribute? columnAttribute, JsonPropertyNameAttribute? jsonPropertyNameAttribute,
        JsonPropertyNamingPolicy namingPolicy)
    {
        // ColumnAttribute takes precedence over JsonPropertyName for consistency with CSV/Excel
        if (columnAttribute?.Ignore == false && !string.IsNullOrWhiteSpace(columnAttribute.Name))
            return columnAttribute.Name;

        // Next, check JsonPropertyNameAttribute
        if (jsonPropertyNameAttribute is not null && !string.IsNullOrWhiteSpace(jsonPropertyNameAttribute.Name))
            return jsonPropertyNameAttribute.Name;

        // Finally, apply naming policy to the property name
        return ApplyNamingPolicy(property.Name, namingPolicy);
    }

    /// <summary>
    ///     Applies the specified naming policy to a property name.
    /// </summary>
    private static string ApplyNamingPolicy(string propertyName, JsonPropertyNamingPolicy namingPolicy)
    {
        return namingPolicy switch
        {
            JsonPropertyNamingPolicy.LowerCase => propertyName.ToLowerInvariant(),
            JsonPropertyNamingPolicy.CamelCase => ToCamelCase(propertyName),
            JsonPropertyNamingPolicy.SnakeCase => ToSnakeCase(propertyName),
            JsonPropertyNamingPolicy.PascalCase => propertyName,
            JsonPropertyNamingPolicy.AsIs => propertyName,
            _ => propertyName.ToLowerInvariant(),
        };
    }

    private static string ToCamelCase(string str)
    {
        if (string.IsNullOrEmpty(str))
            return str;

        return char.ToLowerInvariant(str[0]) + str.Substring(1);
    }

    private static string ToSnakeCase(string str)
    {
        if (string.IsNullOrEmpty(str))
            return str;

        var result = new StringBuilder();

        for (var i = 0; i < str.Length; i++)
        {
            var c = str[i];

            if (char.IsUpper(c))
            {
                if (i > 0 && !char.IsUpper(str[i - 1]))
                    result.Append('_');

                result.Append(char.ToLowerInvariant(c));
            }
            else
                result.Append(c);
        }

        return result.ToString();
    }
}
