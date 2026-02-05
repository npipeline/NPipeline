using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.PostgreSQL.Exceptions;
using NPipeline.Connectors.PostgreSQL.Mapping;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.PostgreSQL.Mapping;

/// <summary>
///     Builds cached mapping delegates from <see cref="PostgresRow" /> to CLR types using attributes.
///     Uses compiled delegates for optimal performance during row mapping.
/// </summary>
internal static class PostgresMapperBuilder
{
    public static Func<PostgresRow, T> Build<T>()
    {
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && !IsIgnored(p))
            .Select(p => new
            {
                Property = p,
                Attribute = p.GetCustomAttribute<ColumnAttribute>() ?? p.GetCustomAttribute<PostgresColumnAttribute>(),
            })
            .ToList();

        var mappings = properties
            .Select(p => new
            {
                ColumnName = p.Attribute?.Name ?? ToSnakeCase(p.Property.Name),
                PropertyName = p.Property.Name,
                Apply = BuildApplyDelegate<T>(p.Property, p.Attribute?.Name ?? ToSnakeCase(p.Property.Name)),
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
                    throw new PostgresMappingException($"Failed to map column '{mapping.ColumnName}' to property '{mapping.PropertyName}'", ex);
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
        var ctor = typeof(T).GetConstructor(Type.EmptyTypes)
                   ?? throw new InvalidOperationException($"Type '{typeof(T).FullName}' does not have a parameterless constructor");

        var newExpression = Expression.New(ctor);
        return Expression.Lambda<Func<T>>(newExpression).Compile();
    }

    private static Action<T, PostgresRow> BuildApplyDelegate<T>(PropertyInfo property, string columnName)
    {
        var instanceParam = Expression.Parameter(typeof(T), "instance");
        var rowParam = Expression.Parameter(typeof(PostgresRow), "row");

        var hasColumnMethod = typeof(PostgresRow).GetMethod(nameof(PostgresRow.HasColumn))
                              ?? throw new InvalidOperationException("PostgresRow.HasColumn not found");

        // Get the non-generic Get(string, T) method and make it generic for the property type
        var getMethodBase = typeof(PostgresRow)
                                .GetMethods()
                                .FirstOrDefault(m => m.Name == nameof(PostgresRow.Get)
                                                     && m.IsGenericMethodDefinition
                                                     && m.GetGenericArguments().Length == 1
                                                     && m.GetParameters().Length == 2
                                                     && m.GetParameters()[0].ParameterType == typeof(string))
                            ?? throw new InvalidOperationException("PostgresRow.Get<T>(string, T) overload not found");

        var getMethod = getMethodBase.MakeGenericMethod(property.PropertyType);

        var hasColumnCall = Expression.Call(rowParam, hasColumnMethod, Expression.Constant(columnName));
        var getCall = Expression.Call(rowParam, getMethod, Expression.Constant(columnName), Expression.Default(property.PropertyType));
        var assign = Expression.Assign(Expression.Property(instanceParam, property), getCall);
        var body = Expression.IfThen(hasColumnCall, assign);

        return Expression.Lambda<Action<T, PostgresRow>>(body, instanceParam, rowParam).Compile();
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var postgresAttribute = property.GetCustomAttribute<PostgresColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true || postgresAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);
        return ignoredByAttribute || hasIgnoreMarker;
    }

    private static string ToSnakeCase(string str)
    {
        return string.Concat(str.Select((x, i) => i > 0 && char.IsUpper(x)
            ? "_" + x
            : x.ToString())).ToLowerInvariant();
    }
}
