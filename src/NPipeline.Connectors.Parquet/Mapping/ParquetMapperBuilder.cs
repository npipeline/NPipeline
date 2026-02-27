using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace NPipeline.Connectors.Parquet.Mapping;

/// <summary>
///     Builds cached mapping delegates from <see cref="ParquetRow" /> to CLR types using attributes.
///     Uses compiled delegates for optimal performance during row mapping.
/// </summary>
public static class ParquetMapperBuilder
{
    private static readonly ConcurrentDictionary<Type, Delegate> MapperCache = new();

    /// <summary>
    ///     Builds or retrieves a cached mapping delegate from <see cref="ParquetRow" /> to type <typeparamref name="T" />.
    /// </summary>
    /// <typeparam name="T">The target type to map Parquet rows to.</typeparam>
    /// <returns>A delegate that maps a Parquet row to an instance of type <typeparamref name="T" />.</returns>
    public static Func<ParquetRow, T> Build<T>()
    {
        var type = typeof(T);

        if (MapperCache.TryGetValue(type, out var cachedDelegate))
            return (Func<ParquetRow, T>)cachedDelegate;

        var mapper = BuildMapper<T>();
        MapperCache.TryAdd(type, mapper);
        return mapper;
    }

    private static Func<ParquetRow, T> BuildMapper<T>()
    {
        var properties = typeof(T)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && !ParquetSchemaBuilder.IsIgnored(p))
            .ToList();

        var mappings = properties
            .Select(p => new
            {
                Property = p,
                ColumnName = ParquetSchemaBuilder.GetColumnName(p),
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
                    throw new ParquetMappingException(
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

    private static Action<T, ParquetRow> BuildApplyDelegate<T>(PropertyInfo property, string columnName)
    {
        var instanceParam = Expression.Parameter(typeof(T), "instance");
        var rowParam = Expression.Parameter(typeof(ParquetRow), "row");

        var propertyType = property.PropertyType;
        var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
        var isNullable = Nullable.GetUnderlyingType(propertyType) is not null || !propertyType.IsValueType;

        // Special handling for DateTimeOffset - stored as DateTime in Parquet
        if (underlyingType == typeof(DateTimeOffset))
            return BuildDateTimeOffsetApplyDelegate<T>(property, columnName, instanceParam, rowParam, propertyType, isNullable);

        // Special handling for DateOnly - stored as DateTime in Parquet
        if (underlyingType == typeof(DateOnly))
            return BuildDateOnlyApplyDelegate<T>(property, columnName, instanceParam, rowParam, propertyType, isNullable);

        // Get the TryGet method with the appropriate generic parameter
        var tryGetMethodBase = typeof(ParquetRow)
                                   .GetMethods()
                                   .FirstOrDefault(m => m.Name == nameof(ParquetRow.TryGet)
                                                        && m.IsGenericMethodDefinition
                                                        && m.GetGenericArguments().Length == 1
                                                        && m.GetParameters().Length == 3)
                               ?? throw new InvalidOperationException("ParquetRow.TryGet<T>(string, out T, T) not found");

        var tryGetMethod = tryGetMethodBase.MakeGenericMethod(underlyingType);

        // Create variables for the out parameter and default value
        var valueVar = Expression.Variable(underlyingType, "value");
        var defaultValue = Expression.Default(underlyingType);

        // Call TryGet
        var tryGetCall = Expression.Call(rowParam, tryGetMethod,
            Expression.Constant(columnName),
            valueVar,
            defaultValue);

        // Handle the assignment based on whether the property is nullable
        Expression assignValue;

        if (isNullable && underlyingType != propertyType)
        {
            // Property is nullable (Nullable<T>), need to convert from T to Nullable<T>
            var convertToNullable = Expression.Convert(valueVar, propertyType);
            assignValue = Expression.Assign(Expression.Property(instanceParam, property), convertToNullable);
        }
        else
        {
            // Property is not nullable or is already a reference type
            assignValue = Expression.Assign(Expression.Property(instanceParam, property), valueVar);
        }

        // If TryGet returns true, assign the value
        var body = Expression.IfThen(tryGetCall, assignValue);

        var block = Expression.Block([valueVar], body);

        return Expression.Lambda<Action<T, ParquetRow>>(block, instanceParam, rowParam).Compile();
    }

    private static Action<T, ParquetRow> BuildDateTimeOffsetApplyDelegate<T>(
        PropertyInfo property,
        string columnName,
        ParameterExpression instanceParam,
        ParameterExpression rowParam,
        Type propertyType,
        bool isNullable)
    {
        // DateTimeOffset is stored as DateTime in Parquet, need to convert
        var tryGetMethodBase = typeof(ParquetRow)
                                   .GetMethods()
                                   .FirstOrDefault(m => m.Name == nameof(ParquetRow.TryGet)
                                                        && m.IsGenericMethodDefinition
                                                        && m.GetGenericArguments().Length == 1
                                                        && m.GetParameters().Length == 3)
                               ?? throw new InvalidOperationException("ParquetRow.TryGet<T>(string, out T, T) not found");

        var tryGetMethod = tryGetMethodBase.MakeGenericMethod(typeof(DateTime));

        // Create variables for the out parameter and default value
        var valueVar = Expression.Variable(typeof(DateTime), "value");
        var defaultValue = Expression.Default(typeof(DateTime));

        // Call TryGet to get DateTime
        var tryGetCall = Expression.Call(rowParam, tryGetMethod,
            Expression.Constant(columnName),
            valueVar,
            defaultValue);

        // Convert DateTime to DateTimeOffset (assuming UTC)
        var dateTimeOffsetConstructor = typeof(DateTimeOffset).GetConstructor([typeof(DateTime)])
                                        ?? throw new InvalidOperationException("DateTimeOffset constructor not found");

        // Handle the assignment based on whether the property is nullable
        Expression assignValue;

        if (isNullable && propertyType == typeof(DateTimeOffset?))
        {
            // Property is nullable DateTimeOffset, need to convert from DateTime to DateTimeOffset?
            var newDateTimeOffset = Expression.New(dateTimeOffsetConstructor, valueVar);
            var convertToNullable = Expression.Convert(newDateTimeOffset, typeof(DateTimeOffset?));
            assignValue = Expression.Assign(Expression.Property(instanceParam, property), convertToNullable);
        }
        else
        {
            // Property is non-nullable DateTimeOffset
            var newDateTimeOffset = Expression.New(dateTimeOffsetConstructor, valueVar);
            assignValue = Expression.Assign(Expression.Property(instanceParam, property), newDateTimeOffset);
        }

        // If TryGet returns true, assign the value
        var body = Expression.IfThen(tryGetCall, assignValue);

        var block = Expression.Block([valueVar], body);

        return Expression.Lambda<Action<T, ParquetRow>>(block, instanceParam, rowParam).Compile();
    }

    private static Action<T, ParquetRow> BuildDateOnlyApplyDelegate<T>(
        PropertyInfo property,
        string columnName,
        ParameterExpression instanceParam,
        ParameterExpression rowParam,
        Type propertyType,
        bool isNullable)
    {
        // DateOnly is stored as DateTime in Parquet, need to convert
        var tryGetMethodBase = typeof(ParquetRow)
                                   .GetMethods()
                                   .FirstOrDefault(m => m.Name == nameof(ParquetRow.TryGet)
                                                        && m.IsGenericMethodDefinition
                                                        && m.GetGenericArguments().Length == 1
                                                        && m.GetParameters().Length == 3)
                               ?? throw new InvalidOperationException("ParquetRow.TryGet<T>(string, out T, T) not found");

        var tryGetMethod = tryGetMethodBase.MakeGenericMethod(typeof(DateTime));

        // Create variables for the out parameter and default value
        var valueVar = Expression.Variable(typeof(DateTime), "value");
        var defaultValue = Expression.Default(typeof(DateTime));

        // Call TryGet to get DateTime
        var tryGetCall = Expression.Call(rowParam, tryGetMethod,
            Expression.Constant(columnName),
            valueVar,
            defaultValue);

        // Convert DateTime to DateOnly using DateOnly.FromDateTime
        var fromDateTimeMethod = typeof(DateOnly).GetMethod("FromDateTime", [typeof(DateTime)])
                                 ?? throw new InvalidOperationException("DateOnly.FromDateTime method not found");

        // Handle the assignment based on whether the property is nullable
        Expression assignValue;

        if (isNullable && propertyType == typeof(DateOnly?))
        {
            // Property is nullable DateOnly, need to convert from DateTime to DateOnly?
            var fromDateOnly = Expression.Call(fromDateTimeMethod, valueVar);
            var convertToNullable = Expression.Convert(fromDateOnly, typeof(DateOnly?));
            assignValue = Expression.Assign(Expression.Property(instanceParam, property), convertToNullable);
        }
        else
        {
            // Property is non-nullable DateOnly
            var fromDateOnly = Expression.Call(fromDateTimeMethod, valueVar);
            assignValue = Expression.Assign(Expression.Property(instanceParam, property), fromDateOnly);
        }

        // If TryGet returns true, assign the value
        var body = Expression.IfThen(tryGetCall, assignValue);

        var block = Expression.Block([valueVar], body);

        return Expression.Lambda<Action<T, ParquetRow>>(block, instanceParam, rowParam).Compile();
    }
}

/// <summary>
///     Exception thrown when Parquet row mapping fails.
/// </summary>
public sealed class ParquetMappingException : Exception
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="ParquetMappingException" /> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public ParquetMappingException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="ParquetMappingException" /> class with a specified error message
    ///     and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public ParquetMappingException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
