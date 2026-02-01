using CsvHelper;

namespace NPipeline.Connectors.Csv;

/// <summary>
///     Represents a current CSV row and provides typed accessors.
/// </summary>
/// <param name="reader">The CSV reader used to access field values.</param>
/// <param name="headers">Header lookup map.</param>
/// <param name="hasHeaders">Whether header records are present.</param>
public readonly struct CsvRow(
    CsvReader reader,
    IReadOnlyDictionary<string, int> headers,
    bool hasHeaders)
{
    private readonly CsvReader _reader = reader;
    private readonly IReadOnlyDictionary<string, int> _headers = headers;
    private readonly bool _hasHeaders = hasHeaders;

    /// <summary>
    ///     Try to read a field by header name and convert it to <typeparamref name="T" />.
    /// </summary>
    /// <param name="name">Header name.</param>
    /// <param name="value">Converted value or <paramref name="defaultValue" />.</param>
    /// <param name="defaultValue">Value used when field is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns><c>true</c> when a value is present and converted; otherwise <c>false</c>.</returns>
    public bool TryGet<T>(string name, out T value, T defaultValue = default!)
    {
        value = defaultValue!;

        if (!_hasHeaders || !_headers.TryGetValue(name, out _))
            return false;

        try
        {
            var field = _reader.GetField<T>(name);

            if (field is null)
                return false;

            value = field;
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    ///     Checks whether the row contains a specified column.
    /// </summary>
    /// <param name="name">Header name.</param>
    /// <returns><c>true</c> when column exists; otherwise <c>false</c>.</returns>
    public bool HasColumn(string name)
    {
        return _hasHeaders && _headers.ContainsKey(name);
    }

    /// <summary>
    ///     Read a field by header name and return a converted value or <paramref name="defaultValue" />.
    /// </summary>
    /// <param name="name">Header name.</param>
    /// <param name="defaultValue">Value used when field is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns>Converted value or <paramref name="defaultValue" />.</returns>
    public T Get<T>(string name, T defaultValue = default!)
    {
        return TryGet(name, out var value, defaultValue)
            ? value
            : defaultValue;
    }

    /// <summary>
    ///     Read a field by index and return a converted value or <paramref name="defaultValue" />.
    /// </summary>
    /// <param name="index">Field index.</param>
    /// <param name="defaultValue">Value used when field is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns>Converted value or <paramref name="defaultValue" />.</returns>
    public T GetByIndex<T>(int index, T defaultValue = default!)
    {
        try
        {
            var field = _reader.GetField<T>(index);

            return field is null
                ? defaultValue
                : field;
        }
        catch
        {
            return defaultValue;
        }
    }
}
