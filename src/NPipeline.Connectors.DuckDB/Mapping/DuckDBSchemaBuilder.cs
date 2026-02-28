using System.Reflection;
using System.Text;
using NPipeline.Connectors.DuckDB.Attributes;
using NPipeline.Connectors.DuckDB.Exceptions;

namespace NPipeline.Connectors.DuckDB.Mapping;

/// <summary>
///     Generates CREATE TABLE DDL from CLR types for DuckDB.
/// </summary>
internal static class DuckDBSchemaBuilder
{
    /// <summary>
    ///     Generates a CREATE TABLE IF NOT EXISTS statement for the given type and table name.
    /// </summary>
    public static string BuildCreateTable<T>(string tableName)
    {
        return BuildCreateTable(typeof(T), tableName);
    }

    /// <summary>
    ///     Generates a CREATE TABLE IF NOT EXISTS statement for the given type and table name.
    /// </summary>
    public static string BuildCreateTable(Type type, string tableName)
    {
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && !DuckDBMapperBuilder.IsIgnored(p))
            .ToArray();

        if (properties.Length == 0)
            throw new DuckDBConnectorException($"Type '{type.Name}' has no writable properties for table creation.");

        var sb = new StringBuilder();
        sb.Append($"CREATE TABLE IF NOT EXISTS \"{tableName}\" (");

        var primaryKeys = new List<string>();

        for (var i = 0; i < properties.Length; i++)
        {
            var prop = properties[i];
            var columnName = DuckDBMapperBuilder.GetColumnName(prop);
            var duckDbType = MapClrTypeToDuckDB(prop.PropertyType, prop);
            var isNullable = IsNullableProperty(prop);

            if (i > 0)
                sb.Append(", ");

            sb.Append($"\"{columnName}\" {duckDbType}");

            if (!isNullable)
                sb.Append(" NOT NULL");

            // Check for primary key
            var duckDbAttr = prop.GetCustomAttribute<DuckDBColumnAttribute>();

            if (duckDbAttr?.PrimaryKey == true)
                primaryKeys.Add(columnName);
        }

        if (primaryKeys.Count > 0)
        {
            sb.Append(", PRIMARY KEY (");
            sb.Append(string.Join(", ", primaryKeys.Select(k => $"\"{k}\"")));
            sb.Append(')');
        }

        sb.Append(')');
        return sb.ToString();
    }

    internal static string MapClrTypeToDuckDB(Type type, PropertyInfo? property = null)
    {
        var underlyingType = Nullable.GetUnderlyingType(type);
        var effectiveType = underlyingType ?? type;

        if (effectiveType == typeof(bool))
            return "BOOLEAN";

        if (effectiveType == typeof(byte))
            return "UTINYINT";

        if (effectiveType == typeof(sbyte))
            return "TINYINT";

        if (effectiveType == typeof(short))
            return "SMALLINT";

        if (effectiveType == typeof(ushort))
            return "USMALLINT";

        if (effectiveType == typeof(int))
            return "INTEGER";

        if (effectiveType == typeof(uint))
            return "UINTEGER";

        if (effectiveType == typeof(long))
            return "BIGINT";

        if (effectiveType == typeof(ulong))
            return "UBIGINT";

        if (effectiveType == typeof(float))
            return "FLOAT";

        if (effectiveType == typeof(double))
            return "DOUBLE";

        if (effectiveType == typeof(decimal))
            return "DECIMAL(18, 6)";

        if (effectiveType == typeof(string))
            return "VARCHAR";

        if (effectiveType == typeof(DateTime))
            return "TIMESTAMP";

        if (effectiveType == typeof(DateTimeOffset))
            return "TIMESTAMPTZ";

        if (effectiveType == typeof(DateOnly))
            return "DATE";

        if (effectiveType == typeof(TimeOnly))
            return "TIME";

        if (effectiveType == typeof(Guid))
            return "UUID";

        if (effectiveType == typeof(byte[]))
            return "BLOB";

        if (effectiveType.IsEnum)
            return "VARCHAR";

        throw new DuckDBConnectorException(
            $"Unsupported CLR type '{effectiveType.Name}' for DuckDB schema generation" +
            (property is not null
                ? $" (property: {property.Name})."
                : "."));
    }

    private static bool IsNullableProperty(PropertyInfo property)
    {
        // Nullable value types
        if (Nullable.GetUnderlyingType(property.PropertyType) is not null)
            return true;

        // Reference types — check NRT annotations
        if (!property.PropertyType.IsValueType)
        {
            var nullabilityContext = new NullabilityInfoContext();
            var nullabilityInfo = nullabilityContext.Create(property);
            return nullabilityInfo.WriteState != NullabilityState.NotNull;
        }

        return false;
    }
}
