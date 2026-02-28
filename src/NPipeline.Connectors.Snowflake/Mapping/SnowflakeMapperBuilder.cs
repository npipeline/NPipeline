using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Snowflake.Configuration;
using NPipeline.Connectors.Snowflake.Exceptions;

namespace NPipeline.Connectors.Snowflake.Mapping;

/// <summary>
///     Builds cached mapping delegates from <see cref="SnowflakeRow" /> to CLR types using attributes.
///     Uses compiled delegates for optimal performance during row mapping.
/// </summary>
internal static class SnowflakeMapperBuilder
{
    private static readonly ConcurrentDictionary<Type, Delegate> MapperCache = new();

    public static Func<SnowflakeRow, T> BuildMapper<T>(SnowflakeConfiguration configuration)
    {
        if (configuration.CacheMappingMetadata && MapperCache.TryGetValue(typeof(T), out var cachedDelegate))
            return (Func<SnowflakeRow, T>)cachedDelegate;

        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && !IsIgnored(p))
            .Select(p => new
            {
                Property = p,
                Attribute = p.GetCustomAttribute<ColumnAttribute>() ?? p.GetCustomAttribute<SnowflakeColumnAttribute>(),
            })
            .ToList();

        var mappings = properties
            .Select(p => new
            {
                ColumnName = GetColumnName(p.Property, p.Attribute),
                PropertyName = p.Property.Name,
                p.Property.PropertyType,
                Apply = BuildApplyDelegate<T>(p.Property, GetColumnName(p.Property, p.Attribute)),
            })
            .ToList();

        var createInstance = BuildCreateInstanceDelegate<T>();

        var mapper = new Func<SnowflakeRow, T>(row =>
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
                    if (configuration.ThrowOnMappingError)
                    {
                        throw new SnowflakeMappingException(
                            $"Failed to map column '{mapping.ColumnName}' to property '{mapping.PropertyName}' of type '{mapping.PropertyType.Name}'",
                            ex);
                    }
                }
            }

            return instance;
        });

        if (configuration.CacheMappingMetadata)
            _ = MapperCache.TryAdd(typeof(T), mapper);

        return mapper;
    }

    private static string GetColumnName(PropertyInfo property, ColumnAttribute? attribute)
    {
        if (attribute != null && !string.IsNullOrWhiteSpace(attribute.Name))
            return attribute.Name;

        return SnowflakeNamingConvention.ToDefaultColumnName(property.Name);
    }

    private static Func<T> BuildCreateInstanceDelegate<T>()
    {
        var ctor = typeof(T).GetConstructor(Type.EmptyTypes)
                   ?? throw new InvalidOperationException($"Type '{typeof(T).FullName}' does not have a parameterless constructor");

        var newExpression = Expression.New(ctor);
        return Expression.Lambda<Func<T>>(newExpression).Compile();
    }

    private static Action<T, SnowflakeRow> BuildApplyDelegate<T>(PropertyInfo property, string columnName)
    {
        var instanceParam = Expression.Parameter(typeof(T), "instance");
        var rowParam = Expression.Parameter(typeof(SnowflakeRow), "row");

        var hasColumnMethod = typeof(SnowflakeRow).GetMethod(nameof(SnowflakeRow.HasColumn))
                              ?? throw new InvalidOperationException("SnowflakeRow.HasColumn not found");

        var getMethodBase = typeof(SnowflakeRow)
                                .GetMethods()
                                .FirstOrDefault(m => m.Name == nameof(SnowflakeRow.Get)
                                                     && m.IsGenericMethodDefinition
                                                     && m.GetGenericArguments().Length == 1
                                                     && m.GetParameters().Length == 2
                                                     && m.GetParameters()[0].ParameterType == typeof(string))
                            ?? throw new InvalidOperationException("SnowflakeRow.Get<T>(string, T) overload not found");

        var getMethod = getMethodBase.MakeGenericMethod(property.PropertyType);

        var hasColumnCall = Expression.Call(rowParam, hasColumnMethod, Expression.Constant(columnName));
        var getCall = Expression.Call(rowParam, getMethod, Expression.Constant(columnName), Expression.Default(property.PropertyType));
        var assign = Expression.Assign(Expression.Property(instanceParam, property), getCall);
        var body = Expression.IfThen(hasColumnCall, assign);

        return Expression.Lambda<Action<T, SnowflakeRow>>(body, instanceParam, rowParam).Compile();
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var snowflakeAttribute = property.GetCustomAttribute<SnowflakeColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true || snowflakeAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);
        return ignoredByAttribute || hasIgnoreMarker;
    }
}
