using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.MySql.Configuration;
using NPipeline.Connectors.MySql.Exceptions;

namespace NPipeline.Connectors.MySql.Mapping;

/// <summary>
///     Builds cached mapping delegates from <see cref="MySqlRow"/> to CLR types using attributes and convention.
///     Uses compiled expression delegates for optimal performance during row mapping.
/// </summary>
internal static class MySqlMapperBuilder
{
    private static readonly ConcurrentDictionary<Type, Delegate> MapperCache = new();

    public static Func<MySqlRow, T> BuildMapper<T>(MySqlConfiguration configuration)
    {
        if (configuration.CacheMappingMetadata && MapperCache.TryGetValue(typeof(T), out var cached))
            return (Func<MySqlRow, T>)cached;

        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && !IsIgnored(p))
            .Select(p => new
            {
                Property = p,
                Attribute = (ColumnAttribute?)(p.GetCustomAttribute<MySqlColumnAttribute>()
                             ?? (ColumnAttribute?)p.GetCustomAttribute<ColumnAttribute>()),
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

        var mapper = new Func<MySqlRow, T>(row =>
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
                        throw new MySqlMappingException(
                            $"Failed to map column '{mapping.ColumnName}' to property '{mapping.PropertyName}' of type '{mapping.PropertyType.Name}'",
                            ex);
                    }
                    // If ThrowOnMappingError is false, skip this mapping
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

        // Convention: use property name (MySQL column names are typically snake_case,
        // but convention matching works case-insensitively at lookup time)
        return property.Name;
    }

    private static Func<T> BuildCreateInstanceDelegate<T>()
    {
        if (typeof(T).IsValueType)
            return Expression.Lambda<Func<T>>(Expression.Default(typeof(T))).Compile();

        var ctor = typeof(T).GetConstructor(Type.EmptyTypes)
                   ?? throw new InvalidOperationException($"Type '{typeof(T).FullName}' does not have a parameterless constructor");

        return Expression.Lambda<Func<T>>(Expression.New(ctor)).Compile();
    }

    private static Action<T, MySqlRow> BuildApplyDelegate<T>(PropertyInfo property, string columnName)
    {
        var instanceParam = Expression.Parameter(typeof(T), "instance");
        var rowParam = Expression.Parameter(typeof(MySqlRow), "row");

        var hasColumnMethod = typeof(MySqlRow).GetMethod(nameof(MySqlRow.HasColumn))
            ?? throw new InvalidOperationException("MySqlRow.HasColumn not found");

        var getMethodBase = typeof(MySqlRow)
            .GetMethods()
            .FirstOrDefault(m => m.Name == nameof(MySqlRow.Get)
                                 && m.IsGenericMethodDefinition
                                 && m.GetGenericArguments().Length == 1
                                 && m.GetParameters().Length == 2
                                 && m.GetParameters()[0].ParameterType == typeof(string))
            ?? throw new InvalidOperationException("MySqlRow.Get<T>(string, T) overload not found");

        var getMethod = getMethodBase.MakeGenericMethod(property.PropertyType);

        var hasColumnCall = Expression.Call(rowParam, hasColumnMethod, Expression.Constant(columnName));
        var getCall = Expression.Call(rowParam, getMethod, Expression.Constant(columnName), Expression.Default(property.PropertyType));
        var assign = Expression.Assign(Expression.Property(instanceParam, property), getCall);
        var body = Expression.IfThen(hasColumnCall, assign);

        return Expression.Lambda<Action<T, MySqlRow>>(body, instanceParam, rowParam).Compile();
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var mySqlAttribute = property.GetCustomAttribute<MySqlColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true || mySqlAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);
        return ignoredByAttribute || hasIgnoreMarker;
    }
}
