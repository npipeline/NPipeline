namespace NPipeline.Connectors.DataLake.Partitioning;

/// <summary>
///     Represents a single partition key/value pair for Hive-style partitioning.
///     Example: <c>event_date=2025-01-15</c>
/// </summary>
public sealed record PartitionKey
{
    /// <summary>
    ///     Gets the partition column name (e.g., "event_date").
    /// </summary>
    public required string ColumnName { get; init; }

    /// <summary>
    ///     Gets the string representation of the partition value (e.g., "2025-01-15").
    /// </summary>
    public required string Value { get; init; }

    /// <summary>
    ///     Converts the partition key to Hive-style directory format.
    ///     Example: <c>event_date=2025-01-15/</c>
    /// </summary>
    /// <returns>A Hive-style partition directory string.</returns>
    public string ToHiveStylePath()
    {
        return $"{ColumnName}={Value}/";
    }

    /// <summary>
    ///     Returns a string representation of the partition key in Hive format.
    /// </summary>
    /// <returns>The Hive-style partition string.</returns>
    public override string ToString()
    {
        return $"{ColumnName}={Value}";
    }

    /// <summary>
    ///     Parses a Hive-style partition string into a <see cref="PartitionKey" />.
    /// </summary>
    /// <param name="hiveStyleString">A string in the format "column_name=value".</param>
    /// <returns>The parsed partition key.</returns>
    /// <exception cref="FormatException">Thrown if the string is not in the expected format.</exception>
    public static PartitionKey Parse(string hiveStyleString)
    {
        ArgumentNullException.ThrowIfNull(hiveStyleString);

        var separatorIndex = hiveStyleString.IndexOf('=');

        if (separatorIndex <= 0 || separatorIndex >= hiveStyleString.Length - 1)
        {
            throw new FormatException(
                $"Invalid partition key format: '{hiveStyleString}'. Expected 'column_name=value'.");
        }

        return new PartitionKey
        {
            ColumnName = hiveStyleString[..separatorIndex],
            Value = hiveStyleString[(separatorIndex + 1)..],
        };
    }

    /// <summary>
    ///     Attempts to parse a Hive-style partition string into a <see cref="PartitionKey" />.
    /// </summary>
    /// <param name="hiveStyleString">A string in the format "column_name=value".</param>
    /// <param name="partitionKey">The parsed partition key, or <c>null</c> if parsing failed.</param>
    /// <returns><c>true</c> if parsing succeeded; otherwise, <c>false</c>.</returns>
    public static bool TryParse(string? hiveStyleString, out PartitionKey? partitionKey)
    {
        partitionKey = null;

        if (string.IsNullOrEmpty(hiveStyleString))
            return false;

        var separatorIndex = hiveStyleString.IndexOf('=');

        if (separatorIndex <= 0 || separatorIndex >= hiveStyleString.Length - 1)
            return false;

        partitionKey = new PartitionKey
        {
            ColumnName = hiveStyleString[..separatorIndex],
            Value = hiveStyleString[(separatorIndex + 1)..],
        };

        return true;
    }
}
