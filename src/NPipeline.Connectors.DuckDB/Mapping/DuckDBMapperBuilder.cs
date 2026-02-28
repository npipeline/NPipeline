using System.Collections.Concurrent;
using System.Data.Common;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.DuckDB.Attributes;
using NPipeline.Connectors.DuckDB.Configuration;
using NPipeline.Connectors.DuckDB.Exceptions;

namespace NPipeline.Connectors.DuckDB.Mapping;

/// <summary>
///     Builds compiled <see cref="Func{DuckDBRow, T}" /> delegates for mapping DuckDB rows to CLR types.
///     Delegates are cached by (Type, column-set) key for performance.
/// </summary>
internal static class DuckDBMapperBuilder
{
    private static readonly ConcurrentDictionary<string, Delegate> Cache = new();

    /// <summary>
    ///     Builds a compiled mapper from <see cref="DuckDBRow" /> to <typeparamref name="T" />.
    ///     Uses <see cref="DuckDBColumnAttribute" />, then <see cref="ColumnAttribute" />,
    ///     then property name matching (with optional snake_case conversion).
    /// </summary>
    public static Func<DuckDBRow, T> Build<T>(DbDataReader reader, DuckDBConfiguration? configuration = null)
    {
        var cacheKey = BuildCacheKey<T>(reader);

        if (configuration?.CacheMappingMetadata != false && Cache.TryGetValue(cacheKey, out var cached))
            return (Func<DuckDBRow, T>)cached;

        var mapper = BuildMapper<T>(reader, configuration);

        if (configuration?.CacheMappingMetadata != false)
            Cache.TryAdd(cacheKey, mapper);

        return mapper;
    }

    /// <summary>
    ///     Builds a compiled mapper from column names (for testing and direct use).
    /// </summary>
    public static Func<DuckDBRow, T> BuildMapper<T>(IReadOnlyList<string> columnNames, DuckDBConfiguration? configuration = null)
    {
        var cacheKey = $"{typeof(T).FullName}|{string.Join(",", columnNames)}";

        if (configuration?.CacheMappingMetadata != false && Cache.TryGetValue(cacheKey, out var cached))
            return (Func<DuckDBRow, T>)cached;

        var mapper = BuildMapperFromColumns<T>(columnNames, configuration);

        if (configuration?.CacheMappingMetadata != false)
            Cache.TryAdd(cacheKey, mapper);

        return mapper;
    }

    /// <summary>
    ///     Clears the mapper cache. Primarily used for testing.
    /// </summary>
    internal static void ClearCache()
    {
        Cache.Clear();
    }

    private static string BuildCacheKey<T>(DbDataReader reader)
    {
        var columns = new string[reader.FieldCount];

        for (var i = 0; i < reader.FieldCount; i++)
        {
            columns[i] = reader.GetName(i);
        }

        return $"{typeof(T).FullName}|{string.Join(",", columns)}";
    }

    private static Func<DuckDBRow, T> BuildMapper<T>(DbDataReader reader, DuckDBConfiguration? configuration)
    {
        var columns = new string[reader.FieldCount];

        for (var i = 0; i < reader.FieldCount; i++)
        {
            columns[i] = reader.GetName(i);
        }

        return BuildMapperFromColumns<T>(columns, configuration);
    }

    private static Func<DuckDBRow, T> BuildMapperFromColumns<T>(IReadOnlyList<string> columnNames, DuckDBConfiguration? configuration)
    {
        var type = typeof(T);
        var caseInsensitive = configuration?.CaseInsensitiveMapping ?? true;

        // Build column name set from column names
        var readerColumns = new HashSet<string>(
            caseInsensitive
                ? StringComparer.OrdinalIgnoreCase
                : StringComparer.Ordinal);

        foreach (var col in columnNames)
        {
            readerColumns.Add(col);
        }

        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && !IsIgnored(p))
            .ToArray();

        // Build property-to-column mapping
        var mappings = new List<(PropertyInfo Property, string ColumnName)>();

        foreach (var prop in properties)
        {
            var columnName = GetColumnName(prop);

            if (readerColumns.Contains(columnName))
            {
                mappings.Add((prop, columnName));
                continue;
            }

            // Try snake_case conversion: OrderId → order_id
            var snakeName = ToSnakeCase(prop.Name);

            if (readerColumns.Contains(snakeName))
                mappings.Add((prop, snakeName));

            // Property not found in result set — skip (use default value)
        }

        // Build compiled delegate using expression trees
        return BuildCompiledMapper<T>(mappings);
    }

    private static Func<DuckDBRow, T> BuildCompiledMapper<T>(
        List<(PropertyInfo Property, string ColumnName)> mappings)
    {
        var type = typeof(T);

        // Try to find a parameterless constructor
        var ctor = type.GetConstructor(BindingFlags.Public | BindingFlags.Instance, Type.EmptyTypes);

        if (ctor is not null)
            return BuildObjectInitializerMapper<T>(mappings);

        // Try to find a constructor matching property names (for records / positional ctors)
        return BuildConstructorMapper<T>(mappings);
    }

    private static Func<DuckDBRow, T> BuildObjectInitializerMapper<T>(
        List<(PropertyInfo Property, string ColumnName)> mappings)
    {
        var type = typeof(T);
        var rowParam = Expression.Parameter(typeof(DuckDBRow), "row");

        // var obj = new T();
        var objVar = Expression.Variable(type, "obj");
        var newExpr = Expression.New(type);

        var statements = new List<Expression> { Expression.Assign(objVar, newExpr) };

        foreach (var (property, columnName) in mappings)
        {
            // obj.Property = row.Get<PropertyType>("columnName");
            var getMethod = typeof(DuckDBRow)
                .GetMethod(nameof(DuckDBRow.Get), new[] { typeof(string) })!
                .MakeGenericMethod(property.PropertyType);

            var callGet = Expression.Call(rowParam, getMethod, Expression.Constant(columnName));
            var assign = Expression.Assign(Expression.Property(objVar, property), callGet);
            statements.Add(assign);
        }

        statements.Add(objVar); // return obj

        var body = Expression.Block(new[] { objVar }, statements);
        return Expression.Lambda<Func<DuckDBRow, T>>(body, rowParam).Compile();
    }

    private static Func<DuckDBRow, T> BuildConstructorMapper<T>(
        List<(PropertyInfo Property, string ColumnName)> mappings)
    {
        var type = typeof(T);
        var rowParam = Expression.Parameter(typeof(DuckDBRow), "row");

        // Find the best constructor
        var constructors = type.GetConstructors(BindingFlags.Public | BindingFlags.Instance)
            .OrderByDescending(c => c.GetParameters().Length)
            .ToArray();

        foreach (var ctor in constructors)
        {
            var parameters = ctor.GetParameters();
            var args = new Expression[parameters.Length];
            var allMatched = true;

            for (var i = 0; i < parameters.Length; i++)
            {
                var param = parameters[i];

                var mapping = mappings.FirstOrDefault(m =>
                    string.Equals(m.Property.Name, param.Name, StringComparison.OrdinalIgnoreCase));

                if (mapping.Property is not null)
                {
                    var getMethod = typeof(DuckDBRow)
                        .GetMethod(nameof(DuckDBRow.Get), new[] { typeof(string) })!
                        .MakeGenericMethod(param.ParameterType);

                    args[i] = Expression.Call(rowParam, getMethod, Expression.Constant(mapping.ColumnName));
                }
                else if (param.HasDefaultValue)
                    args[i] = Expression.Constant(param.DefaultValue, param.ParameterType);
                else
                {
                    allMatched = false;
                    break;
                }
            }

            if (allMatched)
            {
                var newExpr = Expression.New(ctor, args);
                return Expression.Lambda<Func<DuckDBRow, T>>(newExpr, rowParam).Compile();
            }
        }

        throw new DuckDBMappingException(
            $"No suitable constructor found for type '{type.Name}'. " +
            "Ensure the type has a parameterless constructor or a constructor whose parameters match the column names.");
    }

    internal static string GetColumnName(PropertyInfo property)
    {
        // Check DuckDBColumnAttribute first
        var duckDbAttr = property.GetCustomAttribute<DuckDBColumnAttribute>();

        if (duckDbAttr?.Name is not null)
            return duckDbAttr.Name;

        // Check base ColumnAttribute
        var columnAttr = property.GetCustomAttribute<ColumnAttribute>();

        if (columnAttr is not null)
            return columnAttr.Name;

        // Default to property name
        return property.Name;
    }

    internal static bool IsIgnored(PropertyInfo property)
    {
        var duckDbAttr = property.GetCustomAttribute<DuckDBColumnAttribute>();

        if (duckDbAttr?.Ignore == true)
            return true;

        var columnAttr = property.GetCustomAttribute<ColumnAttribute>();

        if (columnAttr?.Ignore == true)
            return true;

        if (property.GetCustomAttribute<IgnoreColumnAttribute>() is not null)
            return true;

        return false;
    }

    internal static string ToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;

        var builder = new StringBuilder(name.Length + 4);

        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];

            if (char.IsUpper(c))
            {
                if (i > 0)
                    builder.Append('_');

                builder.Append(char.ToLowerInvariant(c));
            }
            else
                builder.Append(c);
        }

        return builder.ToString();
    }
}
