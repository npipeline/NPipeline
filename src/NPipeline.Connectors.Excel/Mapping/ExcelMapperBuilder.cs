using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.Excel;

/// <summary>
///     Builds cached mapping delegates from <see cref="ExcelRow" /> to CLR types using attributes.
///     Uses compiled delegates for optimal performance during row mapping.
/// </summary>
public static class ExcelMapperBuilder
{
    private static readonly ConcurrentDictionary<Type, Delegate> MapperCache = new();

    /// <summary>
    ///     Builds a cached mapping delegate from <see cref="ExcelRow" /> to type T.
    /// </summary>
    /// <typeparam name="T">The target type to map Excel rows to.</typeparam>
    /// <returns>A delegate that maps an <see cref="ExcelRow" /> to an instance of type T.</returns>
    public static Func<ExcelRow, T> Build<T>()
    {
        var type = typeof(T);

        if (MapperCache.TryGetValue(type, out var cachedDelegate))
            return (Func<ExcelRow, T>)cachedDelegate;

        var mapper = BuildMapper<T>();
        MapperCache.TryAdd(type, mapper);
        return mapper;
    }

    private static Func<ExcelRow, T> BuildMapper<T>()
    {
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && !IsIgnored(p))
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
                PropertyName = p.Property.Name,
                Apply = BuildApplyDelegate<T>(p.Property, p.Attribute?.Name ?? ToColumnNameConvention(p.Property.Name)),
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
                    throw new ExcelMappingException($"Failed to map column '{mapping.ColumnName}' to property '{mapping.PropertyName}'", ex);
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

    private static Action<T, ExcelRow> BuildApplyDelegate<T>(PropertyInfo property, string columnName)
    {
        var instanceParam = Expression.Parameter(typeof(T), "instance");
        var rowParam = Expression.Parameter(typeof(ExcelRow), "row");

        // Get the non-generic Get(string, T) method and make it generic for the property type
        var getMethodBase = typeof(ExcelRow)
                                .GetMethods()
                                .FirstOrDefault(m => m.Name == nameof(ExcelRow.Get)
                                                     && m.IsGenericMethodDefinition
                                                     && m.GetGenericArguments().Length == 1
                                                     && m.GetParameters().Length == 2
                                                     && m.GetParameters()[0].ParameterType == typeof(string))
                            ?? throw new InvalidOperationException("ExcelRow.Get<T>(string, T) overload not found");

        var getMethod = getMethodBase.MakeGenericMethod(property.PropertyType);

        var getCall = Expression.Call(rowParam, getMethod, Expression.Constant(columnName), Expression.Default(property.PropertyType));
        var assign = Expression.Assign(Expression.Property(instanceParam, property), getCall);

        return Expression.Lambda<Action<T, ExcelRow>>(assign, instanceParam, rowParam).Compile();
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);
        return ignoredByAttribute || hasIgnoreMarker;
    }

    /// <summary>
    ///     Converts a PascalCase property name to an Excel column name convention.
    ///     Converts to lowercase by default (e.g., "FirstName" -> "firstname").
    /// </summary>
    private static string ToColumnNameConvention(string str)
    {
        return str.ToLowerInvariant();
    }
}

/// <summary>
///     Exception thrown when Excel mapping fails.
/// </summary>
public sealed class ExcelMappingException : Exception
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="ExcelMappingException" /> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public ExcelMappingException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="ExcelMappingException" /> class with a specified error message and a reference to the inner exception that is
    ///     the cause of this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public ExcelMappingException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
