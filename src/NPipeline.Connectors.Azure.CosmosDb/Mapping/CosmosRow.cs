using System.Text.Json;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Azure.CosmosDb.Mapping;

/// <summary>
///     Provides typed access to Cosmos DB query results and change feed documents.
///     Wraps a JSON document or IDatabaseReader and provides convenient accessor methods.
/// </summary>
public sealed class CosmosRow
    : ICosmosDataWrapper
{
    private readonly bool _caseInsensitive;
    private readonly Dictionary<string, int> _columnIndexes;
    private readonly JsonElement _document;
    private readonly Dictionary<string, object?>? _rowData;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosRow" /> class from a JsonElement.
    /// </summary>
    /// <param name="document">The JSON document containing the row data.</param>
    /// <param name="columnNames">Optional list of column names for indexed access.</param>
    public CosmosRow(JsonElement document, IReadOnlyList<string>? columnNames = null)
    {
        _document = document;
        _caseInsensitive = true;
        ColumnNames = columnNames ?? GetPropertyNames(document);
        _columnIndexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < ColumnNames.Count; i++)
        {
            _columnIndexes[ColumnNames[i]] = i;
        }

        FieldCount = ColumnNames.Count;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosRow" /> class from an IDatabaseReader.
    /// </summary>
    /// <param name="reader">The database reader.</param>
    /// <param name="caseInsensitive">Whether to perform case-insensitive column lookups.</param>
    /// <param name="throwOnConversionError">Whether to throw on field conversion errors. Default is false for backward compatibility.</param>
    public CosmosRow(IDatabaseReader reader, bool caseInsensitive = true, bool throwOnConversionError = false)
    {
        _caseInsensitive = caseInsensitive;

        _columnIndexes = new Dictionary<string, int>(caseInsensitive
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal);

        // Extract data from reader
        _rowData = new Dictionary<string, object?>(caseInsensitive
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal);

        var names = new List<string>();
        var conversionErrors = new List<(int Ordinal, string Name, Exception Exception)>();

        // Note: At this point, the reader should already be positioned on a row
        // We build column names from FieldCount
        for (var i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            names.Add(name);
            _columnIndexes[name] = i;

            try
            {
                _rowData[name] = reader.GetFieldValue<object>(i);
            }
            catch (Exception ex)
            {
                // Track conversion errors for diagnostics
                conversionErrors.Add((i, name, ex));
                _rowData[name] = null;
            }
        }

        // If configured to throw and we have errors, throw an aggregate exception
        if (throwOnConversionError && conversionErrors.Count > 0)
        {
            var errorMessages = conversionErrors.Select(e => $"Column '{e.Name}' (ordinal {e.Ordinal}): {e.Exception.Message}");

            throw new AggregateException(
                $"Failed to read {conversionErrors.Count} column(s) from database reader: {string.Join("; ", errorMessages)}",
                conversionErrors.Select(e => e.Exception));
        }

        ColumnNames = names;
        FieldCount = ColumnNames.Count;
        _document = default;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosRow" /> class from a dictionary.
    /// </summary>
    /// <param name="data">The dictionary containing the row data.</param>
    /// <param name="caseInsensitive">Whether to perform case-insensitive column lookups.</param>
    public CosmosRow(Dictionary<string, object?> data, bool caseInsensitive = true)
    {
        _caseInsensitive = caseInsensitive;

        _rowData = new Dictionary<string, object?>(data, caseInsensitive
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal);

        _columnIndexes = new Dictionary<string, int>(caseInsensitive
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal);

        var names = new List<string>();
        var index = 0;

        foreach (var key in data.Keys)
        {
            names.Add(key);
            _columnIndexes[key] = index++;
        }

        ColumnNames = names;
        FieldCount = ColumnNames.Count;
        _document = default;
    }

    /// <summary>
    ///     Gets the number of fields in the row.
    /// </summary>
    public int FieldCount { get; }

    /// <summary>
    ///     Gets the list of column names in the row.
    /// </summary>
    public IReadOnlyList<string> ColumnNames { get; }

    /// <summary>
    ///     Gets or sets a value by column name using indexer syntax.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <returns>The column value.</returns>
    public object? this[string columnName]
    {
        get => GetValue(columnName);
        set
        {
            if (_rowData != null)
            {
                _rowData[columnName] = value;

                if (!_columnIndexes.ContainsKey(columnName))
                {
                    _columnIndexes[columnName] = ColumnNames.Count;

                    // Can't add to IReadOnlyList, so this is best-effort
                }
            }
        }
    }

    /// <summary>
    ///     Converts the row to a dictionary.
    /// </summary>
    /// <returns>A dictionary containing the row data.</returns>
    public Dictionary<string, object?> ToDictionary()
    {
        if (_rowData != null)
            return new Dictionary<string, object?>(_rowData);

        var result = new Dictionary<string, object?>();

        foreach (var name in ColumnNames)
        {
            result[name] = GetValue(name);
        }

        return result;
    }

    /// <summary>
    ///     Gets a value from the row by column name.
    /// </summary>
    /// <typeparam name="T">The type of the value to get.</typeparam>
    /// <param name="name">The column name.</param>
    /// <param name="defaultValue">The default value if the column is not found or is null.</param>
    /// <returns>The column value or default.</returns>
    public T Get<T>(string name, T defaultValue = default!)
    {
        if (TryGet<T>(name, out var value))
            return value!;

        return defaultValue;
    }

    /// <summary>
    ///     Gets the raw value of a column as an object.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <returns>The column value as an object, or null if not found.</returns>
    public object? GetValue(string name)
    {
        // Try dictionary-backed data first
        if (_rowData != null)
        {
            return _rowData.TryGetValue(name, out var value)
                ? value
                : null;
        }

        // Try JSON document
        if (_document.ValueKind != JsonValueKind.Undefined && _document.ValueKind != JsonValueKind.Null)
        {
            if (!_document.TryGetProperty(name, out var property) &&
                !_document.TryGetProperty(CamelCase(name), out property))
                return null;

            return GetObjectValue(property);
        }

        return null;
    }

    /// <summary>
    ///     Checks if a column exists in the row.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <returns>True if the column exists; otherwise false.</returns>
    public bool HasColumn(string name)
    {
        if (_columnIndexes.ContainsKey(name))
            return true;

        if (_rowData != null)
            return _rowData.ContainsKey(name);

        if (_document.ValueKind != JsonValueKind.Undefined)
            return _document.TryGetProperty(name, out _) || _document.TryGetProperty(CamelCase(name), out _);

        return false;
    }

    /// <summary>
    ///     Gets the underlying JSON document.
    /// </summary>
    /// <returns>The <see cref="JsonElement" /> representing the document.</returns>
    public JsonElement GetDocument()
    {
        return _document;
    }

    /// <summary>
    ///     Gets a value from the row by ordinal position.
    /// </summary>
    /// <typeparam name="T">The type of the value to get.</typeparam>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The column value.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when ordinal is out of range.</exception>
    public T Get<T>(int ordinal)
    {
        if (ordinal < 0 || ordinal >= ColumnNames.Count)
        {
            throw new ArgumentOutOfRangeException(nameof(ordinal),
                $"Ordinal {ordinal} is out of range. Valid range is 0 to {ColumnNames.Count - 1}.");
        }

        return Get<T>(ColumnNames[ordinal]);
    }

    /// <summary>
    ///     Tries to get a value from the row by column name.
    /// </summary>
    /// <typeparam name="T">The type of the value to get.</typeparam>
    /// <param name="name">The column name.</param>
    /// <param name="value">The output value if found.</param>
    /// <returns>
    ///     True if the value was found and could be converted; otherwise false.
    ///     Returns false for both missing columns and deserialization failures.
    ///     Use <see cref="TryGetWithDiagnostics{T}" /> to distinguish between these cases.
    /// </returns>
    /// <remarks>
    ///     <para>
    ///         This method silently catches deserialization exceptions and returns false.
    ///         This behavior is intentional for convenience but may mask type mismatch issues.
    ///     </para>
    ///     <para>
    ///         Common reasons for returning false:
    ///         <list type="bullet">
    ///             <item>
    ///                 <description>Column does not exist in the row</description>
    ///             </item>
    ///             <item>
    ///                 <description>Column value is incompatible with the requested type</description>
    ///             </item>
    ///             <item>
    ///                 <description>JSON deserialization failed due to type mismatch</description>
    ///             </item>
    ///         </list>
    ///     </para>
    /// </remarks>
    public bool TryGet<T>(string name, out T? value)
    {
        value = default;

        if (string.IsNullOrEmpty(name))
            return false;

        // Try dictionary-backed data first
        if (_rowData != null)
        {
            if (!_rowData.TryGetValue(name, out var dictValue))
                return false;

            if (dictValue == null)
                return true; // Found but null

            if (dictValue is T typedValue)
            {
                value = typedValue;
                return true;
            }

            try
            {
                value = (T?)Convert.ChangeType(dictValue, typeof(T));
                return true;
            }
            catch (InvalidCastException)
            {
                return false;
            }
            catch (FormatException)
            {
                return false;
            }
            catch (OverflowException)
            {
                return false;
            }
        }

        // Try JSON document
        if (_document.ValueKind != JsonValueKind.Undefined && _document.ValueKind != JsonValueKind.Null)
        {
            if (!_document.TryGetProperty(name, out var property) &&
                !_document.TryGetProperty(CamelCase(name), out property))
                return false;

            if (property.ValueKind == JsonValueKind.Null || property.ValueKind == JsonValueKind.Undefined)
                return true; // Found but null

            try
            {
                value = JsonSerializer.Deserialize<T>(property.GetRawText());
                return true;
            }
            catch (JsonException)
            {
                return false;
            }
            catch (NotSupportedException)
            {
                // Type is not supported for deserialization
                return false;
            }
        }

        return false;
    }

    /// <summary>
    ///     Tries to get a value from the row by column name with detailed diagnostic information.
    /// </summary>
    /// <typeparam name="T">The type of the value to get.</typeparam>
    /// <param name="name">The column name.</param>
    /// <param name="value">The output value if found and successfully converted.</param>
    /// <param name="error">The error message if the operation failed; otherwise null.</param>
    /// <returns>
    ///     True if the value was found and successfully converted; otherwise false.
    ///     Check <paramref name="error" /> for details when false is returned.
    /// </returns>
    /// <remarks>
    ///     <para>
    ///         Possible error scenarios:
    ///         <list type="bullet">
    ///             <item>
    ///                 <description>Column not found: error contains "Column '{name}' not found"</description>
    ///             </item>
    ///             <item>
    ///                 <description>Type conversion failed: error contains conversion exception details</description>
    ///             </item>
    ///             <item>
    ///                 <description>JSON deserialization failed: error contains JSON exception details</description>
    ///             </item>
    ///         </list>
    ///     </para>
    /// </remarks>
    public bool TryGetWithDiagnostics<T>(string name, out T? value, out string? error)
    {
        value = default;
        error = null;

        if (string.IsNullOrEmpty(name))
        {
            error = "Column name cannot be null or empty.";
            return false;
        }

        // Try dictionary-backed data first
        if (_rowData != null)
        {
            if (!_rowData.TryGetValue(name, out var dictValue))
            {
                error = $"Column '{name}' not found in row. Available columns: {string.Join(", ", _rowData.Keys)}";
                return false;
            }

            if (dictValue == null)
                return true; // Found but null

            if (dictValue is T typedValue)
            {
                value = typedValue;
                return true;
            }

            try
            {
                value = (T?)Convert.ChangeType(dictValue, typeof(T));
                return true;
            }
            catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
            {
                error = $"Cannot convert column '{name}' from {dictValue.GetType().Name} to {typeof(T).Name}: {ex.Message}";
                return false;
            }
        }

        // Try JSON document
        if (_document.ValueKind != JsonValueKind.Undefined && _document.ValueKind != JsonValueKind.Null)
        {
            if (!_document.TryGetProperty(name, out var property) &&
                !_document.TryGetProperty(CamelCase(name), out property))
            {
                var availableProperties = _document.EnumerateObject().Select(p => p.Name).ToList();
                error = $"Column '{name}' not found in JSON document. Available properties: {string.Join(", ", availableProperties)}";
                return false;
            }

            if (property.ValueKind == JsonValueKind.Null || property.ValueKind == JsonValueKind.Undefined)
                return true; // Found but null

            try
            {
                value = JsonSerializer.Deserialize<T>(property.GetRawText());
                return true;
            }
            catch (Exception ex) when (ex is JsonException or NotSupportedException)
            {
                error = $"Cannot deserialize column '{name}' (JSON {property.ValueKind}) to {typeof(T).Name}: {ex.Message}";
                return false;
            }
        }

        error = "No data available in row (neither dictionary nor JSON document).";
        return false;
    }

    /// <summary>
    ///     Gets the document ID (id property).
    /// </summary>
    /// <returns>The document ID, or null if not found.</returns>
    public string? GetId()
    {
        return Get<string>("id") ?? Get<string>("Id") ?? Get<string>("_id");
    }

    /// <summary>
    ///     Gets the partition key value if present.
    /// </summary>
    /// <returns>The partition key value, or null if not found.</returns>
    public string? GetPartitionKey()
    {
        return Get<string>("partitionKey") ?? Get<string>("PartitionKey");
    }

    /// <summary>
    ///     Gets the _ts (timestamp) value if present.
    /// </summary>
    /// <returns>The timestamp as a DateTime, or null if not found.</returns>
    public DateTime? GetTimestamp()
    {
        var ts = Get<long>("_ts");

        if (ts == 0)
            return null;

        return DateTimeOffset.FromUnixTimeSeconds(ts).DateTime;
    }

    /// <summary>
    ///     Gets the _etag value if present.
    /// </summary>
    /// <returns>The etag, or null if not found.</returns>
    public string? GetEtag()
    {
        return Get<string>("_etag");
    }

    private static object? GetObjectValue(JsonElement property)
    {
        return property.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Number when property.TryGetInt32(out var intVal) => intVal,
            JsonValueKind.Number when property.TryGetInt64(out var longVal) => longVal,
            JsonValueKind.Number when property.TryGetDouble(out var doubleVal) => doubleVal,
            JsonValueKind.Number when property.TryGetDecimal(out var decimalVal) => decimalVal,
            JsonValueKind.String => property.GetString(),
            JsonValueKind.Array => property.EnumerateArray().Select(GetObjectValue).ToList(),
            JsonValueKind.Object => property.EnumerateObject().ToDictionary(p => p.Name, p => GetObjectValue(p.Value)),
            _ => property.GetRawText(),
        };
    }

    private static List<string> GetPropertyNames(JsonElement document)
    {
        if (document.ValueKind != JsonValueKind.Object)
            return [];

        return document.EnumerateObject().Select(p => p.Name).ToList();
    }

    private static string CamelCase(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;

        return char.ToLowerInvariant(name[0]) + name[1..];
    }
}
