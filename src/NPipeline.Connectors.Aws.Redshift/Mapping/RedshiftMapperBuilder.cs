using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Aws.Redshift.Exceptions;

namespace NPipeline.Connectors.Aws.Redshift.Mapping;

/// <summary>
///     Builds compiled mappers for converting RedshiftRow to type T.
///     Uses expression trees for high-performance mapping without reflection at runtime.
/// </summary>
public static class RedshiftMapperBuilder
{
    private static readonly ConcurrentDictionary<(Type Type, RedshiftNamingConvention Convention), Delegate> Cache = new();

    /// <summary>
    ///     Builds or retrieves a cached mapper for converting RedshiftRow to type T.
    /// </summary>
    /// <typeparam name="T">The target type to map to.</typeparam>
    /// <param name="namingConvention">The naming convention to use for column name resolution.</param>
    /// <returns>A compiled delegate that maps a RedshiftRow to an instance of type T.</returns>
    public static Func<RedshiftRow, T> Build<T>(RedshiftNamingConvention namingConvention = RedshiftNamingConvention.PascalToSnakeCase)
    {
        var key = (typeof(T), namingConvention);
        return (Func<RedshiftRow, T>)Cache.GetOrAdd(key, _ => BuildMapper<T>(namingConvention));
    }

    /// <summary>
    ///     Clears the mapper cache. Useful for testing.
    /// </summary>
    public static void ClearCache()
    {
        Cache.Clear();
    }

    private static Func<RedshiftRow, T> BuildMapper<T>(RedshiftNamingConvention namingConvention)
    {
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && !IsIgnored(p))
            .ToList();

        var mappings = properties
            .Select(p => new
            {
                Property = p,
                ColumnName = GetColumnName(p, namingConvention),
            })
            .Select(p => new
            {
                p.ColumnName,
                PropertyName = p.Property.Name,
                Apply = BuildApplyDelegate<T>(p.Property, p.ColumnName),
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
                    throw new RedshiftMappingException(
                        $"Failed to map column '{mapping.ColumnName}' to property '{mapping.PropertyName}'", ex);
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
                   ?? throw new InvalidOperationException(
                       $"Type '{typeof(T).FullName}' does not have a parameterless constructor");

        var newExpression = Expression.New(ctor);
        return Expression.Lambda<Func<T>>(newExpression).Compile();
    }

    private static Action<T, RedshiftRow> BuildApplyDelegate<T>(PropertyInfo property, string columnName)
    {
        var instanceParam = Expression.Parameter(typeof(T), "instance");
        var rowParam = Expression.Parameter(typeof(RedshiftRow), "row");

        var propertyType = property.PropertyType;
        var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
        var isNullable = Nullable.GetUnderlyingType(propertyType) is not null || !propertyType.IsValueType;

        // Get the HasColumn method
        var hasColumnMethod = typeof(RedshiftRow).GetMethod(nameof(RedshiftRow.HasColumn))
                              ?? throw new InvalidOperationException("RedshiftRow.HasColumn method not found");

        // Get the IsNull method
        var isNullMethod = typeof(RedshiftRow).GetMethod(nameof(RedshiftRow.IsNull))
                           ?? throw new InvalidOperationException("RedshiftRow.IsNull method not found");

        // Get the Get method with generic parameter: Get<T>(string, T)
        var getMethodBase = typeof(RedshiftRow)
                                .GetMethods()
                                .FirstOrDefault(m => m.Name == nameof(RedshiftRow.Get)
                                                     && m.IsGenericMethodDefinition
                                                     && m.GetGenericArguments().Length == 1
                                                     && m.GetParameters().Length == 2
                                                     && m.GetParameters()[0].ParameterType == typeof(string))
                            ?? throw new InvalidOperationException("RedshiftRow.Get<T>(string, T) not found");

        var getMethod = getMethodBase.MakeGenericMethod(underlyingType);

        // Call HasColumn to check if column exists
        var hasColumnCall = Expression.Call(rowParam, hasColumnMethod, Expression.Constant(columnName));

        // Call IsNull to check if the value is null
        var isNullCall = Expression.Call(rowParam, isNullMethod, Expression.Constant(columnName));

        // Call Get<T>(columnName, default(T))
        var defaultValue = Expression.Default(underlyingType);
        var getCall = Expression.Call(rowParam, getMethod, Expression.Constant(columnName), defaultValue);

        // Build the assignment expression
        Expression assignValue;

        if (isNullable && underlyingType != propertyType)
        {
            // Property is nullable (Nullable<T>), need to handle null separately
            // If IsNull, assign null; otherwise, convert the value to Nullable<T>
            var convertToNullable = Expression.Convert(getCall, propertyType);
            var nullConstant = Expression.Constant(null, propertyType);

            assignValue = Expression.Assign(Expression.Property(instanceParam, property),
                Expression.Condition(isNullCall, nullConstant, convertToNullable));
        }
        else
        {
            // Property is not nullable or is already a reference type
            assignValue = Expression.Assign(Expression.Property(instanceParam, property), getCall);
        }

        // If HasColumn returns true, assign the value
        var body = Expression.IfThen(hasColumnCall, assignValue);

        return Expression.Lambda<Action<T, RedshiftRow>>(body, instanceParam, rowParam).Compile();
    }

    /// <summary>
    ///     Gets the column name for a property based on attributes or naming convention.
    /// </summary>
    /// <param name="property">The property to get the column name for.</param>
    /// <param name="convention">The naming convention to use.</param>
    /// <returns>The column name in Redshift.</returns>
    public static string GetColumnName(PropertyInfo property, RedshiftNamingConvention convention)
    {
        // Check for RedshiftColumn attribute first
        var redshiftColumnAttr = property.GetCustomAttribute<RedshiftColumnAttribute>();

        if (redshiftColumnAttr != null)
            return redshiftColumnAttr.Name;

        var columnAttr = property.GetCustomAttribute<ColumnAttribute>();

        if (columnAttr is { Name.Length: > 0 })
            return columnAttr.Name;

        // Apply naming convention
        return convention switch
        {
            RedshiftNamingConvention.PascalToSnakeCase => ConvertToSnakeCase(property.Name),
            RedshiftNamingConvention.Lowercase => property.Name.ToLowerInvariant(),
            RedshiftNamingConvention.AsIs => property.Name,
            _ => property.Name,
        };
    }

    /// <summary>
    ///     Checks if a property should be ignored during mapping.
    /// </summary>
    /// <param name="property">The property to check.</param>
    /// <returns>True if the property should be ignored; otherwise, false.</returns>
    public static bool IsIgnored(PropertyInfo property)
    {
        var columnAttribute = property.GetCustomAttribute<ColumnAttribute>();
        var redshiftColumnAttr = property.GetCustomAttribute<RedshiftColumnAttribute>();
        var ignoredByAttribute = columnAttribute?.Ignore == true || redshiftColumnAttr?.Ignore == true;
        var hasIgnoreMarker = property.IsDefined(typeof(IgnoreColumnAttribute), true);

        return ignoredByAttribute || hasIgnoreMarker;
    }

    private static string ConvertToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;

        var result = new StringBuilder();
        result.Append(char.ToLowerInvariant(name[0]));

        for (var i = 1; i < name.Length; i++)
        {
            var c = name[i];

            if (char.IsUpper(c))
            {
                // Check if previous char is lowercase or next char is lowercase (for acronyms)
                if (char.IsLower(name[i - 1]) || (i + 1 < name.Length && char.IsLower(name[i + 1])))
                    result.Append('_');

                result.Append(char.ToLowerInvariant(c));
            }
            else
                result.Append(c);
        }

        return result.ToString();
    }
}
