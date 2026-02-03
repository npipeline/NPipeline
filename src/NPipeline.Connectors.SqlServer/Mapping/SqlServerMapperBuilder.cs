using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.SqlServer.Configuration;
using NPipeline.Connectors.SqlServer.Exceptions;

namespace NPipeline.Connectors.SqlServer.Mapping;

/// <summary>
///     Builds cached mapping delegates from <see cref="SqlServerRow" /> to CLR types using attributes.
///     Uses compiled delegates for optimal performance during row mapping.
/// </summary>
internal static class SqlServerMapperBuilder
{
    private static readonly ConcurrentDictionary<Type, Delegate> _mapperCache = new();

    public static Func<SqlServerRow, T> BuildMapper<T>(SqlServerConfiguration configuration)
    {
        if (configuration.CacheMappingMetadata && _mapperCache.TryGetValue(typeof(T), out var cachedDelegate))
            return (Func<SqlServerRow, T>)cachedDelegate;

        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && !IsIgnored(p))
            .Select(p => new
            {
                Property = p,
                Attribute = p.GetCustomAttribute<ColumnAttribute>() ?? p.GetCustomAttribute<SqlServerColumnAttribute>(),
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

        // Compile a delegate for instance creation to avoid Activator.CreateInstance<T>() reflection on each row
        var createInstance = BuildCreateInstanceDelegate<T>();

        var mapper = new Func<SqlServerRow, T>(row =>
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
                        throw new SqlServerMappingException(
                            $"Failed to map column '{mapping.ColumnName}' to property '{mapping.PropertyName}' of type '{mapping.PropertyType.Name}'",
                            ex);
                    }

                    // If ThrowOnMappingError is false, we skip the mapping
                }
            }

            return instance;
        });

        if (configuration.CacheMappingMetadata)
            _ = _mapperCache.TryAdd(typeof(T), mapper);

        return mapper;
    }

    /// <summary>
    ///     Gets the column name for a property based on attributes or convention.
    /// </summary>
    private static string GetColumnName(PropertyInfo property, ColumnAttribute? attribute)
    {
        if (attribute != null && !string.IsNullOrWhiteSpace(attribute.Name))
            return attribute.Name;

        // SQL Server uses PascalCase by convention
        return property.Name;
    }

    /// <summary>
    ///     Builds a compiled delegate for creating instances of type T.
    /// </summary>
    private static Func<T> BuildCreateInstanceDelegate<T>()
    {
        var ctor = typeof(T).GetConstructor(Type.EmptyTypes)
                   ?? throw new InvalidOperationException($"Type '{typeof(T).FullName}' does not have a parameterless constructor");

        var newExpression = Expression.New(ctor);
        return Expression.Lambda<Func<T>>(newExpression).Compile();
    }

    private static Action<T, SqlServerRow> BuildApplyDelegate<T>(PropertyInfo property, string columnName)
    {
        var instanceParam = Expression.Parameter(typeof(T), "instance");
        var rowParam = Expression.Parameter(typeof(SqlServerRow), "row");

        var hasColumnMethod = typeof(SqlServerRow).GetMethod(nameof(SqlServerRow.HasColumn))
                              ?? throw new InvalidOperationException("SqlServerRow.HasColumn not found");

        // Get the non-generic Get(string, T) method and make it generic for the property type
        var getMethodBase = typeof(SqlServerRow)
                                .GetMethods()
                                .FirstOrDefault(m => m.Name == nameof(SqlServerRow.Get)
                                                     && m.IsGenericMethodDefinition
                                                     && m.GetGenericArguments().Length == 1
                                                     && m.GetParameters().Length == 2
                                                     && m.GetParameters()[0].ParameterType == typeof(string))
                            ?? throw new InvalidOperationException("SqlServerRow.Get<T>(string, T) overload not found");

        var getMethod = getMethodBase.MakeGenericMethod(property.PropertyType);

        var hasColumnCall = Expression.Call(rowParam, hasColumnMethod, Expression.Constant(columnName));
        var getCall = Expression.Call(rowParam, getMethod, Expression.Constant(columnName), Expression.Default(property.PropertyType));
        var assign = Expression.Assign(Expression.Property(instanceParam, property), getCall);
        var body = Expression.IfThen(hasColumnCall, assign);

        return Expression.Lambda<Action<T, SqlServerRow>>(body, instanceParam, rowParam).Compile();
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var sqlServerAttribute = property.GetCustomAttribute<SqlServerColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true || sqlServerAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);
        return ignoredByAttribute || hasIgnoreMarker;
    }
}
