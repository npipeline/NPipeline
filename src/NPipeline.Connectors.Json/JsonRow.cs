using System.Globalization;
using System.Text.Json;

namespace NPipeline.Connectors.Json;

/// <summary>
///     Represents a JSON object and provides typed accessors for reading property values.
/// </summary>
/// <remarks>
///     <para>
///         This readonly struct provides efficient, read-only access to JSON object properties.
///         It holds a <see cref="JsonElement" /> internally and offers methods for retrieving
///         values with type conversion and default value support.
///     </para>
///     <para>
///         The struct supports both direct property access and nested property access via dot notation.
///         Property lookup can be case-sensitive or case-insensitive based on configuration.
///     </para>
/// </remarks>
public readonly struct JsonRow
{
    private readonly JsonElement _element;
    private readonly bool _caseInsensitive;

    /// <summary>
    ///     Initializes a new instance of the <see cref="JsonRow" /> struct.
    /// </summary>
    /// <param name="element">The JSON element representing the object.</param>
    /// <param name="caseInsensitive">
    ///     Whether property name comparison should be case-insensitive.
    ///     Default is <c>true</c>.
    /// </param>
    public JsonRow(JsonElement element, bool caseInsensitive = true)
    {
        _element = element;
        _caseInsensitive = caseInsensitive;
    }

    /// <summary>
    ///     Try to read a property by name and convert it to <typeparamref name="T" />.
    /// </summary>
    /// <param name="name">Property name.</param>
    /// <param name="value">Converted value or <paramref name="defaultValue" />.</param>
    /// <param name="defaultValue">Value used when the property is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns><c>true</c> when a value is present and converted; otherwise <c>false</c>.</returns>
    /// <remarks>
    ///     <para>
    ///         This method attempts to find a property with specified name and convert its value
    ///         to the target type. If the property is missing or conversion fails, the default value
    ///         is returned.
    ///     </para>
    ///     <para>
    ///         Property name comparison is case-sensitive or case-insensitive based on the
    ///         caseInsensitive parameter passed to the constructor.
    ///     </para>
    /// </remarks>
    public bool TryGet<T>(string name, out T? value, T? defaultValue = default)
    {
        value = defaultValue;

        if (_element.ValueKind != JsonValueKind.Object)
            return false;

        var property = _caseInsensitive
            ? FindPropertyCaseInsensitive(name)
            : FindPropertyCaseSensitive(name);

        if (property is null)
            return false;

        // Get the JsonElement from the JsonProperty
        var element = property.Value.Value;
        return TryConvertTo(element, out value, defaultValue, _caseInsensitive);
    }

    /// <summary>
    ///     Read a property by name and return a converted value or <paramref name="defaultValue" />.
    /// </summary>
    /// <param name="name">Property name.</param>
    /// <param name="defaultValue">Value used when the property is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns>Converted value or <paramref name="defaultValue" />.</returns>
    /// <remarks>
    ///     This is a convenience method that calls <see cref="TryGet{T}" /> and returns the value
    ///     or the default value if the property is missing or conversion fails.
    /// </remarks>
    public T? Get<T>(string name, T? defaultValue = default)
    {
        return TryGet(name, out var value, defaultValue)
            ? value
            : defaultValue;
    }

    /// <summary>
    ///     Checks whether the row contains a specified property.
    /// </summary>
    /// <param name="name">Property name.</param>
    /// <returns><c>true</c> when property exists; otherwise <c>false</c>.</returns>
    /// <remarks>
    ///     Property name comparison is case-sensitive or case-insensitive based on the
    ///     caseInsensitive parameter passed to the constructor.
    /// </remarks>
    public bool HasProperty(string name)
    {
        if (_element.ValueKind != JsonValueKind.Object)
            return false;

        return _caseInsensitive
            ? FindPropertyCaseInsensitive(name) is not null
            : FindPropertyCaseSensitive(name) is not null;
    }

    /// <summary>
    ///     Read a nested property by path and return a converted value or <paramref name="defaultValue" />.
    /// </summary>
    /// <param name="path">Dot-notation path to the nested property (e.g., "user.address.city").</param>
    /// <param name="defaultValue">Value used when any part of the path is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns>Converted value or <paramref name="defaultValue" />.</returns>
    /// <remarks>
    ///     <para>
    ///         This method allows accessing nested properties using dot notation. For example,
    ///         to access <c>user.address.city</c>, the path would be <c>"user.address.city"</c>.
    ///     </para>
    ///     <para>
    ///         If any part of the path is missing or is not an object, the default value is returned.
    ///     </para>
    /// </remarks>
    public T? GetNested<T>(string path, T? defaultValue = default)
    {
        if (string.IsNullOrWhiteSpace(path))
            return defaultValue;

        var parts = path.Split('.');
        var current = _element;

        foreach (var part in parts)
        {
            if (current.ValueKind != JsonValueKind.Object)
                return defaultValue;

            var property = _caseInsensitive
                ? FindPropertyCaseInsensitive(part, current)
                : FindPropertyCaseSensitive(part, current);

            if (property is null)
                return defaultValue;

            // Get JsonElement from JsonProperty
            current = property.Value.Value;
        }

        return TryConvertTo(current, out var value, defaultValue, _caseInsensitive)
            ? value
            : defaultValue;
    }

    private static bool TryConvertTo<T>(JsonElement element, out T? value, T? defaultValue, bool caseInsensitive)
    {
        value = defaultValue;

        try
        {
            var kind = element.ValueKind;

            if (kind == JsonValueKind.Null || kind == JsonValueKind.Undefined)
                return false;

            var targetType = typeof(T);
            var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

            if (underlyingType == typeof(JsonElement))
            {
                value = (T?)(object?)element;
                return true;
            }

            if (underlyingType == typeof(JsonRow))
            {
                value = (T?)(object?)new JsonRow(element, caseInsensitive);
                return true;
            }

            // Handle string
            if (underlyingType == typeof(string))
            {
                if (kind == JsonValueKind.String)
                {
                    value = (T?)(object?)element.GetString();
                    return true;
                }

                value = (T?)(object?)element.ToString();
                return true;
            }

            // Handle boolean
            if (underlyingType == typeof(bool))
            {
                if (kind == JsonValueKind.True || kind == JsonValueKind.False)
                {
                    value = (T?)(object?)element.GetBoolean();
                    return true;
                }

                if (kind == JsonValueKind.String && bool.TryParse(element.GetString(), out var boolValue))
                {
                    value = (T?)(object?)boolValue;
                    return true;
                }

                if (kind == JsonValueKind.Number)
                {
                    var numValue = element.GetInt32();
                    value = (T?)(object?)(numValue != 0);
                    return true;
                }

                return false;
            }

            // Handle integers
            if (underlyingType == typeof(int))
            {
                if (kind == JsonValueKind.Number)
                {
                    value = (T?)(object?)element.GetInt32();
                    return true;
                }

                if (kind == JsonValueKind.String && int.TryParse(element.GetString(), out var intValue))
                {
                    value = (T?)(object?)intValue;
                    return true;
                }

                return false;
            }

            if (underlyingType == typeof(long))
            {
                if (kind == JsonValueKind.Number)
                {
                    value = (T?)(object?)element.GetInt64();
                    return true;
                }

                if (kind == JsonValueKind.String && long.TryParse(element.GetString(), out var longValue))
                {
                    value = (T?)(object?)longValue;
                    return true;
                }

                return false;
            }

            if (underlyingType == typeof(short))
            {
                if (kind == JsonValueKind.Number)
                {
                    value = (T?)(object?)element.GetInt16();
                    return true;
                }

                if (kind == JsonValueKind.String && short.TryParse(element.GetString(), out var shortValue))
                {
                    value = (T?)(object?)shortValue;
                    return true;
                }

                return false;
            }

            // Handle floating point
            if (underlyingType == typeof(float))
            {
                if (kind == JsonValueKind.Number)
                {
                    value = (T?)(object?)element.GetSingle();
                    return true;
                }

                if (kind == JsonValueKind.String && float.TryParse(element.GetString(), out var floatValue))
                {
                    value = (T?)(object?)floatValue;
                    return true;
                }

                return false;
            }

            if (underlyingType == typeof(double))
            {
                if (kind == JsonValueKind.Number)
                {
                    value = (T?)(object?)element.GetDouble();
                    return true;
                }

                if (kind == JsonValueKind.String && double.TryParse(element.GetString(), out var doubleValue))
                {
                    value = (T?)(object?)doubleValue;
                    return true;
                }

                return false;
            }

            if (underlyingType == typeof(decimal))
            {
                if (kind == JsonValueKind.Number)
                {
                    value = (T?)(object?)element.GetDecimal();
                    return true;
                }

                if (kind == JsonValueKind.String && decimal.TryParse(element.GetString(), out var decimalValue))
                {
                    value = (T?)(object?)decimalValue;
                    return true;
                }

                return false;
            }

            // Handle DateTime
            if (underlyingType == typeof(DateTime))
            {
                if (kind == JsonValueKind.String && DateTime.TryParse(element.GetString(),
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
                        out var dateTimeValue))
                {
                    value = (T?)(object?)dateTimeValue;
                    return true;
                }

                return false;
            }

            // Handle Guid
            if (underlyingType == typeof(Guid))
            {
                if (kind == JsonValueKind.String && Guid.TryParse(element.GetString(), out var guidValue))
                {
                    value = (T?)(object?)guidValue;
                    return true;
                }

                return false;
            }

            // Fallback to JsonSerializer.Deserialize
            var json = element.GetRawText();
            var deserialized = JsonSerializer.Deserialize(json, targetType);

            if (deserialized is not null)
            {
                value = (T?)deserialized;
                return true;
            }

            return false;
        }
        catch
        {
            return false;
        }
    }

    private JsonProperty? FindPropertyCaseSensitive(string name)
    {
        return FindPropertyCaseSensitive(name, _element);
    }

    private static JsonProperty? FindPropertyCaseSensitive(string name, JsonElement element)
    {
        foreach (var property in element.EnumerateObject())
        {
            if (property.Name == name)
                return property;
        }

        return null;
    }

    private JsonProperty? FindPropertyCaseInsensitive(string name)
    {
        return FindPropertyCaseInsensitive(name, _element);
    }

    private static JsonProperty? FindPropertyCaseInsensitive(string name, JsonElement element)
    {
        foreach (var property in element.EnumerateObject())
        {
            if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
                return property;
        }

        return null;
    }
}
