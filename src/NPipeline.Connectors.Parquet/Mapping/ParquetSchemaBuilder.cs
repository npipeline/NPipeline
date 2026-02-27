using System.Collections.Concurrent;
using System.Reflection;
using NPipeline.Connectors.Attributes;
using NPipeline.Connectors.Parquet.Attributes;
using Parquet.Schema;

namespace NPipeline.Connectors.Parquet.Mapping;

/// <summary>
///     Builds Parquet schemas from CLR types using reflection and attributes.
///     Results are cached per type for optimal performance.
/// </summary>
public static class ParquetSchemaBuilder
{
    private static readonly ConcurrentDictionary<Type, ParquetSchema> SchemaCache = new();

    /// <summary>
    ///     Builds or retrieves a cached Parquet schema for the specified CLR type.
    /// </summary>
    /// <typeparam name="T">The CLR type to build a schema for.</typeparam>
    /// <returns>A Parquet schema corresponding to the CLR type.</returns>
    /// <exception cref="ParquetSchemaException">Thrown when schema building fails.</exception>
    public static ParquetSchema Build<T>()
    {
        return Build(typeof(T));
    }

    /// <summary>
    ///     Builds or retrieves a cached Parquet schema for the specified CLR type.
    /// </summary>
    /// <param name="type">The CLR type to build a schema for.</param>
    /// <returns>A Parquet schema corresponding to the CLR type.</returns>
    /// <exception cref="ParquetSchemaException">Thrown when schema building fails.</exception>
    public static ParquetSchema Build(Type type)
    {
        return SchemaCache.GetOrAdd(type, BuildSchema);
    }

    /// <summary>
    ///     Gets the column name for a property, considering attributes and conventions.
    /// </summary>
    /// <param name="property">The property to get the column name for.</param>
    /// <returns>The column name to use in Parquet.</returns>
    public static string GetColumnName(PropertyInfo property)
    {
        // Check ParquetColumnAttribute first
        var parquetColumnAttr = property.GetCustomAttribute<ParquetColumnAttribute>();
        if (parquetColumnAttr is not null)
        {
            if (!string.IsNullOrEmpty(parquetColumnAttr.Name))
            {
                return parquetColumnAttr.Name;
            }

            // Attribute present but no name override - use property name as-is
            return property.Name;
        }

        // Fallback to ColumnAttribute (generic)
        var columnAttr = property.GetCustomAttribute<ColumnAttribute>();
        if (columnAttr is not null && !string.IsNullOrEmpty(columnAttr.Name))
        {
            return columnAttr.Name;
        }

        // Convention: use property name as-is (Parquet is case-sensitive, preserve PascalCase)
        return property.Name;
    }

    /// <summary>
    ///     Checks if a property should be ignored during schema building.
    /// </summary>
    /// <param name="property">The property to check.</param>
    /// <returns><c>true</c> if the property should be ignored; otherwise <c>false</c>.</returns>
    public static bool IsIgnored(PropertyInfo property)
    {
        var parquetColumnAttr = property.GetCustomAttribute<ParquetColumnAttribute>();
        if (parquetColumnAttr is not null && parquetColumnAttr.Ignore)
        {
            return true;
        }

        var columnAttr = property.GetCustomAttribute<ColumnAttribute>();
        if (columnAttr is not null && columnAttr.Ignore)
        {
            return true;
        }

        return property.IsDefined(typeof(IgnoreColumnAttribute), true);
    }

    private static ParquetSchema BuildSchema(Type type)
    {
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.CanWrite && !IsIgnored(p))
            .ToList();

        var fields = new List<Field>();

        foreach (var property in properties)
        {
            var columnName = GetColumnName(property);
            var field = CreateField(property, columnName);
            fields.Add(field);
        }

        return new ParquetSchema(fields);
    }

    private static Field CreateField(PropertyInfo property, string columnName)
    {
        var propertyType = property.PropertyType;
        var underlyingType = Nullable.GetUnderlyingType(propertyType);
        var targetType = underlyingType ?? propertyType;
        var isNullable = underlyingType is not null || !propertyType.IsValueType;

        Field field = targetType switch
        {
            _ when targetType == typeof(string) => new DataField<string>(columnName, isNullable),
            _ when targetType == typeof(int) => new DataField<int>(columnName, isNullable),
            _ when targetType == typeof(short) => new DataField<short>(columnName, isNullable),
            _ when targetType == typeof(byte) => new DataField<byte>(columnName, isNullable),
            _ when targetType == typeof(sbyte) => new DataField<sbyte>(columnName, isNullable),
            _ when targetType == typeof(long) => new DataField<long>(columnName, isNullable),
            _ when targetType == typeof(uint) => new DataField<uint>(columnName, isNullable),
            _ when targetType == typeof(ushort) => new DataField<ushort>(columnName, isNullable),
            _ when targetType == typeof(ulong) => new DataField<ulong>(columnName, isNullable),
            _ when targetType == typeof(float) => new DataField<float>(columnName, isNullable),
            _ when targetType == typeof(double) => new DataField<double>(columnName, isNullable),
            _ when targetType == typeof(bool) => new DataField<bool>(columnName, isNullable),
            _ when targetType == typeof(decimal) => CreateDecimalField(property, columnName, isNullable),
            _ when targetType == typeof(DateTime) => new DataField<DateTime>(columnName, isNullable),
            _ when targetType == typeof(DateTimeOffset) => new DataField<DateTime>(columnName, isNullable), // Store DateTimeOffset as DateTime (UTC)
            _ when targetType == typeof(DateOnly) => new DataField<DateTime>(columnName, isNullable), // Store DateOnly as DateTime
            _ when targetType == typeof(Guid) => new DataField<string>(columnName, isNullable), // Store GUIDs as strings
            _ when targetType == typeof(byte[]) => new DataField<byte[]>(columnName),
            _ when targetType.IsEnum => new DataField<string>(columnName, isNullable), // Store enums as strings
            _ when ImplementsInterface(targetType, typeof(IList<>)) || targetType.IsArray
                => CreateListField(property, columnName, targetType),
            _ => throw new ParquetSchemaException(
                $"Unsupported type '{targetType.Name}' for property '{property.Name}' on type '{property.DeclaringType?.Name}'. " +
                "Consider using a custom converter or a supported type.")
        };

        return field;
    }

    private static Field CreateDecimalField(PropertyInfo property, string columnName, bool isNullable)
    {
        var decimalAttr = property.GetCustomAttribute<ParquetDecimalAttribute>();

        if (decimalAttr is null)
        {
            throw new ParquetSchemaException(
                $"Property '{property.Name}' on type '{property.DeclaringType?.Name}' is of type decimal " +
                $"but is not decorated with [{nameof(ParquetDecimalAttribute)}]. " +
                "Decimal properties require precision and scale to be specified.");
        }

        return new DecimalDataField(columnName, decimalAttr.Precision, decimalAttr.Scale, isNullable: isNullable);
    }

    private static Field CreateListField(PropertyInfo property, string columnName, Type listType)
    {
        Type? elementType = null;

        if (listType.IsArray)
        {
            elementType = listType.GetElementType();
        }
        else if (ImplementsInterface(listType, typeof(IList<>)))
        {
            var genericInterface = listType.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IList<>));

            elementType = genericInterface?.GetGenericArguments()[0];
        }
        else if (listType.IsGenericType)
        {
            elementType = listType.GetGenericArguments()[0];
        }

        if (elementType is null)
        {
            throw new ParquetSchemaException(
                $"Could not determine element type for list property '{property.Name}' on type '{property.DeclaringType?.Name}'.");
        }

        // Create the appropriate list field based on element type
        return CreateListFieldForElement(columnName, elementType);
    }

    private static Field CreateListFieldForElement(string columnName, Type elementType)
    {
        var underlyingElementType = Nullable.GetUnderlyingType(elementType) ?? elementType;

        // Get the element field and wrap it in a ListField
        Field elementField = underlyingElementType switch
        {
            _ when underlyingElementType == typeof(string) => new DataField<string>(ListField.ElementName),
            _ when underlyingElementType == typeof(int) => new DataField<int>(ListField.ElementName),
            _ when underlyingElementType == typeof(long) => new DataField<long>(ListField.ElementName),
            _ when underlyingElementType == typeof(short) => new DataField<short>(ListField.ElementName),
            _ when underlyingElementType == typeof(byte) => new DataField<byte>(ListField.ElementName),
            _ when underlyingElementType == typeof(float) => new DataField<float>(ListField.ElementName),
            _ when underlyingElementType == typeof(double) => new DataField<double>(ListField.ElementName),
            _ when underlyingElementType == typeof(bool) => new DataField<bool>(ListField.ElementName),
            _ when underlyingElementType == typeof(decimal) => new DataField<decimal>(ListField.ElementName),
            _ when underlyingElementType == typeof(DateTime) => new DataField<DateTime>(ListField.ElementName),
            _ when underlyingElementType == typeof(DateTimeOffset) => new DataField<DateTime>(ListField.ElementName), // Store DateTimeOffset as DateTime (UTC)
            _ when underlyingElementType == typeof(DateOnly) => new DataField<DateTime>(ListField.ElementName), // Store DateOnly as DateTime
            _ when underlyingElementType == typeof(Guid) => new DataField<string>(ListField.ElementName),
            _ => new DataField<string>(ListField.ElementName)
        };

        return new ListField(columnName, elementField);
    }

    private static bool ImplementsInterface(Type type, Type interfaceType)
    {
        return type.GetInterfaces().Any(i =>
            i == interfaceType ||
            (i.IsGenericType && i.GetGenericTypeDefinition() == interfaceType));
    }
}

/// <summary>
///     Exception thrown when Parquet schema building fails.
/// </summary>
public sealed class ParquetSchemaException : Exception
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="ParquetSchemaException" /> class with a specified error message.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    public ParquetSchemaException(string message) : base(message)
    {
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="ParquetSchemaException" /> class with a specified error message
    ///     and a reference to the inner exception that is the cause of this exception.
    /// </summary>
    /// <param name="message">The message that describes the error.</param>
    /// <param name="innerException">The exception that is the cause of the current exception.</param>
    public ParquetSchemaException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
