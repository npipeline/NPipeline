using System.Globalization;
using System.Text;

namespace NPipeline.Connectors.DataLake.Partitioning;

/// <summary>
///     Converts record instances and partition specs into Hive-style partition directory paths.
///     Example: <c>event_date=2025-01-15/region=EU/</c>
/// </summary>
public static class PartitionPathBuilder
{
    /// <summary>
    ///     Builds a Hive-style partition path from a record and partition spec.
    /// </summary>
    /// <typeparam name="T">The record type.</typeparam>
    /// <param name="record">The record instance to extract partition values from.</param>
    /// <param name="spec">The partition specification.</param>
    /// <returns>A Hive-style partition path string (e.g., "event_date=2025-01-15/region=EU/").</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="spec" /> is <c>null</c>.</exception>
    public static string BuildPath<T>(T record, PartitionSpec<T> spec)
    {
        ArgumentNullException.ThrowIfNull(spec);

        if (!spec.HasPartitions)
            return string.Empty;

        var builder = new StringBuilder();

        foreach (var column in spec.Columns)
        {
            var value = column.GetValue(record);
            var formattedValue = FormatPartitionValue(value, column.ValueType);

            _ = builder.Append(column.ColumnName)
                .Append('=')
                .Append(formattedValue)
                .Append('/');
        }

        return builder.ToString();
    }

    /// <summary>
    ///     Extracts partition keys from a record using a partition spec.
    /// </summary>
    /// <typeparam name="T">The record type.</typeparam>
    /// <param name="record">The record instance to extract partition values from.</param>
    /// <param name="spec">The partition specification.</param>
    /// <returns>A list of partition keys.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="spec" /> is <c>null</c>.</exception>
    public static IReadOnlyList<PartitionKey> ExtractPartitionKeys<T>(T record, PartitionSpec<T> spec)
    {
        ArgumentNullException.ThrowIfNull(spec);

        if (!spec.HasPartitions)
            return [];

        var keys = new List<PartitionKey>(spec.Columns.Count);

        foreach (var column in spec.Columns)
        {
            var value = column.GetValue(record);
            var formattedValue = FormatPartitionValue(value, column.ValueType);

            keys.Add(new PartitionKey
            {
                ColumnName = column.ColumnName,
                Value = formattedValue,
            });
        }

        return keys;
    }

    /// <summary>
    ///     Formats a partition value for use in a Hive-style path.
    ///     Handles DateOnly, DateTime, DateTimeOffset, enums, strings (URL-encoded), and other types.
    /// </summary>
    /// <param name="value">The value to format.</param>
    /// <param name="valueType">The type of the value.</param>
    /// <returns>A string representation suitable for use in a partition path.</returns>
    public static string FormatPartitionValue(object? value, Type valueType)
    {
        if (value is null)
            return "null";

        var underlyingType = Nullable.GetUnderlyingType(valueType) ?? valueType;

        // DateOnly -> yyyy-MM-dd
        if (underlyingType == typeof(DateOnly))
        {
            var dateOnly = (DateOnly)value;
            return dateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        // DateTime -> yyyy-MM-dd-HH-mm-ss (ISO-like format for paths)
        if (underlyingType == typeof(DateTime))
        {
            var dateTime = (DateTime)value;
            return dateTime.ToString("yyyy-MM-dd-HH-mm-ss", CultureInfo.InvariantCulture);
        }

        // DateTimeOffset -> yyyy-MM-dd-HH-mm-ss
        if (underlyingType == typeof(DateTimeOffset))
        {
            var dateTimeOffset = (DateTimeOffset)value;
            return dateTimeOffset.ToString("yyyy-MM-dd-HH-mm-ss", CultureInfo.InvariantCulture);
        }

        // Enum -> lowercase string
        if (underlyingType.IsEnum)
            return value.ToString()?.ToLowerInvariant() ?? "unknown";

        // String -> URL-encoded for safety
        if (underlyingType == typeof(string))
        {
            var stringValue = (string)value;
            return Uri.EscapeDataString(stringValue);
        }

        // Guid -> lowercase without braces
        if (underlyingType == typeof(Guid))
            return ((Guid)value).ToString("D").ToLowerInvariant();

        // Numeric types and others -> invariant culture string
        return Convert.ToString(value, CultureInfo.InvariantCulture) ?? "null";
    }

    /// <summary>
    ///     Parses a partition path segment into column name and value.
    /// </summary>
    /// <param name="segment">A path segment in the format "column_name=value".</param>
    /// <param name="columnName">The parsed column name.</param>
    /// <param name="value">The parsed value (still as string).</param>
    /// <returns><c>true</c> if parsing succeeded; otherwise, <c>false</c>.</returns>
    public static bool TryParseSegment(string segment, out string? columnName, out string? value)
    {
        columnName = null;
        value = null;

        if (string.IsNullOrEmpty(segment))
            return false;

        var separatorIndex = segment.IndexOf('=');

        if (separatorIndex <= 0 || separatorIndex >= segment.Length - 1)
            return false;

        columnName = segment[..separatorIndex];
        value = segment[(separatorIndex + 1)..];

        return true;
    }

    /// <summary>
    ///     Parses a full partition path into partition keys.
    /// </summary>
    /// <param name="path">A partition path like "event_date=2025-01-15/region=EU/".</param>
    /// <returns>A list of partition keys extracted from the path.</returns>
    public static IReadOnlyList<PartitionKey> ParsePath(string path)
    {
        if (string.IsNullOrEmpty(path))
            return [];

        var segments = path.Split(['/', '\\'], StringSplitOptions.RemoveEmptyEntries);
        var keys = new List<PartitionKey>(segments.Length);

        foreach (var segment in segments)
        {
            if (PartitionKey.TryParse(segment, out var key) && key is not null)
                keys.Add(key);
        }

        return keys;
    }
}
