using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Connectors.Attributes;

namespace NPipeline.Connectors.Csv.Mapping;

/// <summary>
///     Builds cached mapping delegates from <see cref="CsvRow" /> to CLR types using attributes.
///     Uses compiled delegates for optimal performance during row mapping.
/// </summary>
public static class CsvMapperBuilder
{
    private static readonly ConcurrentDictionary<Type, Delegate> MapperCache = new();

    /// <summary>
    ///     Builds or retrieves a cached mapping delegate from <see cref="CsvRow" /> to type <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">The target type to map CSV rows to.</typeparam>
    /// <returns>A delegate that maps a CSV row to an instance of type <typeparamref name="T" />.</returns>
    public static Func<CsvRow, T> Build<T>()
    {
        var type = typeof(T);

        if (MapperCache.TryGetValue(type, out var cachedDelegate))
            return (Func<CsvRow, T>)cachedDelegate;

        var mapper = BuildMapper<T>();
        MapperCache.TryAdd(type, mapper);
        return mapper;
    }

    private static Func<CsvRow, T> BuildMapper<T>()
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
                    throw new CsvMappingException($"Failed to map column '{mapping.ColumnName}' to property '{mapping.PropertyName}'", ex);
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

    private static Action<T, CsvRow> BuildApplyDelegate<T>(PropertyInfo property, string columnName)
    {
        var instanceParam = Expression.Parameter(typeof(T), "instance");
        var rowParam = Expression.Parameter(typeof(CsvRow), "row");

        var hasColumnMethod = typeof(CsvRow).GetMethod(nameof(CsvRow.HasColumn))
                              ?? throw new InvalidOperationException("CsvRow.HasColumn not found");

        // Get the non-generic Get(string, T) method and make it generic for the property type
        var getMethodBase = typeof(CsvRow)
                                .GetMethods()
                                .FirstOrDefault(m => m.Name == nameof(CsvRow.Get)
                                                     && m.IsGenericMethodDefinition
                                                     && m.GetGenericArguments().Length == 1
                                                     && m.GetParameters().Length == 2
                                                     && m.GetParameters()[0].ParameterType == typeof(string))
                            ?? throw new InvalidOperationException("CsvRow.Get<T>(string, T) overload not found");

        var getMethod = getMethodBase.MakeGenericMethod(property.PropertyType);

        var hasColumnCall = Expression.Call(rowParam, hasColumnMethod, Expression.Constant(columnName));
        var getCall = Expression.Call(rowParam, getMethod, Expression.Constant(columnName), Expression.Default(property.PropertyType));
        var assign = Expression.Assign(Expression.Property(instanceParam, property), getCall);
        var body = Expression.IfThen(hasColumnCall, assign);

        return Expression.Lambda<Action<T, CsvRow>>(body, instanceParam, rowParam).Compile();
    }

    private static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);
        return ignoredByAttribute || hasIgnoreMarker;
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

/// <summary>
///     Exception thrown when CSV mapping fails.
/// </summary>
public sealed class CsvMappingException : Exception
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="CsvMappingException" /> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public CsvMappingException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="CsvMappingException" /> class with a specified error message and a reference to the inner exception that is
    ///     the cause of this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public CsvMappingException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
