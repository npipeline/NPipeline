using System.Data;
using System.Globalization;

namespace NPipeline.Connectors.Excel;

/// <summary>
///     Represents the current Excel row and provides typed accessors.
/// </summary>
public readonly struct ExcelRow
{
    private readonly IDataRecord _record;
    private readonly IReadOnlyDictionary<string, int> _headers;
    private readonly bool _hasHeaders;

    /// <summary>
    ///     Initialize a new instance of <see cref="ExcelRow" />.
    /// </summary>
    /// <param name="record">The data record representing the current row.</param>
    /// <param name="headers">Header lookup map.</param>
    /// <param name="hasHeaders">Whether header records are present.</param>
    public ExcelRow(IDataRecord record, IReadOnlyDictionary<string, int> headers, bool hasHeaders)
    {
        _record = record;
        _headers = headers;
        _hasHeaders = hasHeaders;
    }

    /// <summary>
    ///     Try to read a field by header name and convert it to <typeparamref name="T" />.
    /// </summary>
    /// <param name="name">Header name.</param>
    /// <param name="value">Converted value or <paramref name="defaultValue" />.</param>
    /// <param name="defaultValue">Value used when the field is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns><c>true</c> when a value is present and converted; otherwise <c>false</c>.</returns>
    public bool TryGet<T>(string name, out T? value, T? defaultValue = default)
    {
        value = defaultValue;

        if (!_hasHeaders || !_headers.TryGetValue(name, out var index))
            return false;

        if (index >= _record.FieldCount)
            return false;

        var raw = _record.GetValue(index);

        if (raw == DBNull.Value)
            return false;

        var converted = ExcelValueConverter.Convert(raw, typeof(T));

        if (converted is T typed)
        {
            value = typed;
            return true;
        }

        return false;
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
    /// <param name="defaultValue">Value used when the field is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns>Converted value or <paramref name="defaultValue" />.</returns>
    public T? Get<T>(string name, T? defaultValue = default)
    {
        return TryGet(name, out var value, defaultValue)
            ? value
            : defaultValue;
    }

    /// <summary>
    ///     Read a field by index and return a converted value or <paramref name="defaultValue" />.
    /// </summary>
    /// <param name="index">Field index.</param>
    /// <param name="defaultValue">Value used when the field is missing or conversion fails.</param>
    /// <typeparam name="T">Target type.</typeparam>
    /// <returns>Converted value or <paramref name="defaultValue" />.</returns>
    public T? GetByIndex<T>(int index, T? defaultValue = default)
    {
        if (index < 0 || index >= _record.FieldCount)
            return defaultValue;

        var raw = _record.GetValue(index);

        if (raw == DBNull.Value)
            return defaultValue;

        var converted = ExcelValueConverter.Convert(raw, typeof(T));

        return converted is T typed
            ? typed
            : defaultValue;
    }
}

/// <summary>
///     Internal utility class for converting Excel values to target types.
/// </summary>
internal static class ExcelValueConverter
{
    public static object? Convert(object? value, Type targetType)
    {
        if (value is null || value == DBNull.Value)
            return null;

        if (targetType.IsAssignableFrom(value.GetType()))
            return value;

        try
        {
            var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

            if (underlyingType == typeof(string))
                return value.ToString();

            if (underlyingType == typeof(int) || underlyingType == typeof(int?))
                return System.Convert.ToInt32(value);

            if (underlyingType == typeof(long) || underlyingType == typeof(long?))
                return System.Convert.ToInt64(value);

            if (underlyingType == typeof(short) || underlyingType == typeof(short?))
                return System.Convert.ToInt16(value);

            if (underlyingType == typeof(decimal) || underlyingType == typeof(decimal?))
                return System.Convert.ToDecimal(value);

            if (underlyingType == typeof(double) || underlyingType == typeof(double?))
                return System.Convert.ToDouble(value);

            if (underlyingType == typeof(float) || underlyingType == typeof(float?))
                return System.Convert.ToSingle(value);

            if (underlyingType == typeof(bool) || underlyingType == typeof(bool?))
            {
                if (value is bool b)
                    return b;

                if (value is string s)
                {
                    return bool.TryParse(s, out var boolResult)
                        ? boolResult
                        : false;
                }

                return System.Convert.ToBoolean(value);
            }

            if (underlyingType == typeof(DateTime) || underlyingType == typeof(DateTime?))
            {
                if (value is DateTime dt)
                    return dt;

                if (double.TryParse(value.ToString(), out var oaDate))
                    return DateTime.FromOADate(oaDate);

                return System.Convert.ToDateTime(value);
            }

            if (underlyingType == typeof(Guid) || underlyingType == typeof(Guid?))
            {
                if (value is Guid g)
                    return g;

                return Guid.TryParse(value.ToString(), out var guidResult)
                    ? guidResult
                    : Guid.Empty;
            }

            return System.Convert.ChangeType(value, underlyingType, CultureInfo.InvariantCulture);
        }
        catch
        {
            return null;
        }
    }
}
