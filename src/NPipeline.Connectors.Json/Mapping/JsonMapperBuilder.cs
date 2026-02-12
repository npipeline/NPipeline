using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json.Serialization;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.Json.Mapping;

/// <summary>
///     Builds cached mapping delegates from <see cref="JsonRow" /> to CLR types using attributes.
///     Uses compiled delegates for optimal performance during row mapping.
/// </summary>
public static class JsonMapperBuilder
{
    private static readonly ConcurrentDictionary<(Type, JsonPropertyNamingPolicy), Delegate> MapperCache = new();

    /// <summary>
    ///     Builds or retrieves a cached mapping delegate from <see cref="JsonRow" /> to type <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">The target type to map JSON rows to.</typeparam>
    /// <param name="namingPolicy">The property naming policy to use for property name transformation.</param>
    /// <returns>A delegate that maps a JSON row to an instance of type <typeparamref name="T" />.</returns>
    public static Func<JsonRow, T> Build<T>(JsonPropertyNamingPolicy namingPolicy = JsonPropertyNamingPolicy.LowerCase)
    {
        var type = typeof(T);
        var cacheKey = (type, namingPolicy);

        if (MapperCache.TryGetValue(cacheKey, out var cachedDelegate))
            return (Func<JsonRow, T>)cachedDelegate;

        var mapper = BuildMapper<T>(namingPolicy);
        MapperCache.TryAdd(cacheKey, mapper);
        return mapper;
    }

    private static Func<JsonRow, T> BuildMapper<T>(JsonPropertyNamingPolicy namingPolicy)
    {
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && !IsIgnored(p))
            .Select(p => new
            {
                Property = p,
                ColumnAttribute = p.GetCustomAttribute<ColumnAttribute>(),
                JsonPropertyNameAttribute = p.GetCustomAttribute<JsonPropertyNameAttribute>(),
            })
            .ToList();

        var mappings = properties
            .Select(p => new
            {
                PropertyName = p.Property.Name,
                JsonPropertyName = GetJsonPropertyName(p.Property, p.ColumnAttribute, p.JsonPropertyNameAttribute, namingPolicy),
                Apply = BuildApplyDelegate<T>(p.Property, GetJsonPropertyName(p.Property, p.ColumnAttribute, p.JsonPropertyNameAttribute, namingPolicy)),
            })
            .ToList();

        // Compile a delegate for instance creation to avoid Activator.CreateInstance<T>() reflection on each row
        var createInstance = BuildCreateInstanceDelegate<T>();

        return row =>
        {
            var instance = createInstance();

            foreach (var mapping in mappings)
            {
                try
                {
                    mapping.Apply(instance, row);
                }
                catch (Exception ex)
                {
                    throw new JsonMappingException($"Failed to map JSON property '{mapping.JsonPropertyName}' to property '{mapping.PropertyName}'", ex);
                }
            }

            return instance;
        };
    }

    /// <summary>
    ///     Builds a compiled delegate for creating instances of type T.
    /// </summary>
    private static Func<T> BuildCreateInstanceDelegate<T>()
    {
        var type = typeof(T);

        // Try to find a parameterless constructor first
        var ctor = type.GetConstructor(Type.EmptyTypes);

        if (ctor is not null)
        {
            var newExpression = Expression.New(ctor);
            return Expression.Lambda<Func<T>>(newExpression).Compile();
        }

        // If no parameterless constructor, find the best constructor
        // For simplicity, we'll throw an exception as the CSV mapper does
        throw new InvalidOperationException($"Type '{type.FullName}' does not have a parameterless constructor");
    }

    private static Action<T, JsonRow> BuildApplyDelegate<T>(PropertyInfo property, string jsonPropertyName)
    {
        var instanceParam = Expression.Parameter(typeof(T), "instance");
        var rowParam = Expression.Parameter(typeof(JsonRow), "row");

        var hasPropertyMethod = typeof(JsonRow).GetMethod(nameof(JsonRow.HasProperty))
                                ?? throw new InvalidOperationException("JsonRow.HasProperty not found");

        // Get the non-generic Get(string, T) method and make it generic for the property type
        var getMethodBase = typeof(JsonRow)
                                .GetMethods()
                                .FirstOrDefault(m => m.Name == nameof(JsonRow.Get)
                                                     && m.IsGenericMethodDefinition
                                                     && m.GetGenericArguments().Length == 1
                                                     && m.GetParameters().Length == 2
                                                     && m.GetParameters()[0].ParameterType == typeof(string))
                            ?? throw new InvalidOperationException("JsonRow.Get<T>(string, T) overload not found");

        var getMethod = getMethodBase.MakeGenericMethod(property.PropertyType);

        var hasPropertyCall = Expression.Call(rowParam, hasPropertyMethod, Expression.Constant(jsonPropertyName));
        var getCall = Expression.Call(rowParam, getMethod, Expression.Constant(jsonPropertyName), Expression.Default(property.PropertyType));
        var assign = Expression.Assign(Expression.Property(instanceParam, property), getCall);
        var body = Expression.IfThen(hasPropertyCall, assign);

        return Expression.Lambda<Action<T, JsonRow>>(body, instanceParam, rowParam).Compile();
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
