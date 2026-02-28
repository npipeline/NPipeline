using System.Data.Common;
using NPipeline.Connectors.DuckDB.Exceptions;

namespace NPipeline.Connectors.DuckDB.Mapping;

/// <summary>
///     Lightweight row wrapper over a <see cref="DbDataReader" /> providing typed column access.
///     A single instance is reused across rows — column ordinal lookup is cached on first use.
/// </summary>
public sealed class DuckDBRow
{
    private readonly Dictionary<string, int> _columnOrdinals;
    private readonly DbDataReader _reader;

    /// <summary>
    ///     Creates a new <see cref="DuckDBRow" /> wrapper for the specified reader.
    /// </summary>
    /// <param name="reader">The underlying data reader.</param>
    /// <param name="caseInsensitive">Whether to use case-insensitive column name matching.</param>
    public DuckDBRow(DbDataReader reader, bool caseInsensitive = true)
    {
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));

        _columnOrdinals = new Dictionary<string, int>(
            caseInsensitive
                ? StringComparer.OrdinalIgnoreCase
                : StringComparer.Ordinal);

        for (var i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            _columnOrdinals.TryAdd(name, i);
        }
    }

    /// <summary>
    ///     The number of columns in the current row.
    /// </summary>
    public int FieldCount => _reader.FieldCount;

    /// <summary>
    ///     Gets the column names from the result set.
    /// </summary>
    public IReadOnlyList<string> ColumnNames => _columnOrdinals.Keys.ToList();

    /// <summary>
    ///     The zero-based index of the current row.
    /// </summary>
    public long RowIndex { get; private set; }

    /// <summary>
    ///     Gets a typed value by column name.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <param name="columnName">The column name.</param>
    /// <returns>The typed column value.</returns>
    public T Get<T>(string columnName)
    {
        var ordinal = GetOrdinal(columnName);
        return Get<T>(ordinal);
    }

    /// <summary>
    ///     Gets a typed value by column ordinal.
    /// </summary>
    /// <typeparam name="T">The target type.</typeparam>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <returns>The typed column value.</returns>
    public T Get<T>(int ordinal)
    {
        if (_reader.IsDBNull(ordinal))
        {
            var type = typeof(T);

            if (Nullable.GetUnderlyingType(type) is not null || !type.IsValueType)
                return default!;

            throw new DuckDBMappingException(
                $"Column at ordinal {ordinal} is NULL but target type '{type.Name}' is not nullable.",
                RowIndex);
        }

        var value = _reader.GetValue(ordinal);
        return ConvertValue<T>(value, ordinal);
    }

    /// <summary>
    ///     Gets a typed value by column name, returning a default if the column is null or missing.
    /// </summary>
    public T GetOrDefault<T>(string columnName, T defaultValue = default!)
    {
        if (!HasColumn(columnName))
            return defaultValue;

        var ordinal = GetOrdinal(columnName);

        if (_reader.IsDBNull(ordinal))
            return defaultValue;

        var value = _reader.GetValue(ordinal);
        return ConvertValue<T>(value, ordinal);
    }

    /// <summary>
    ///     Tries to get a typed value by column name.
    /// </summary>
    public bool TryGet<T>(string columnName, out T value, T defaultValue = default!)
    {
        if (!HasColumn(columnName))
        {
            value = defaultValue;
            return false;
        }

        var ordinal = GetOrdinal(columnName);

        if (_reader.IsDBNull(ordinal))
        {
            value = defaultValue;
            return false;
        }

        try
        {
            value = ConvertValue<T>(_reader.GetValue(ordinal), ordinal);
            return true;
        }
        catch
        {
            value = defaultValue;
            return false;
        }
    }

    /// <summary>
    ///     Checks if a column value is null.
    /// </summary>
    public bool IsNull(string columnName)
    {
        return _reader.IsDBNull(GetOrdinal(columnName));
    }

    /// <summary>
    ///     Checks if a column value is null.
    /// </summary>
    public bool IsNull(int ordinal)
    {
        return _reader.IsDBNull(ordinal);
    }

    /// <summary>
    ///     Gets the raw object value by column name.
    /// </summary>
    public object? GetValue(string columnName)
    {
        var ordinal = GetOrdinal(columnName);

        return _reader.IsDBNull(ordinal)
            ? null
            : _reader.GetValue(ordinal);
    }

    /// <summary>
    ///     Gets the raw object value by ordinal.
    /// </summary>
    public object? GetValue(int ordinal)
    {
        return _reader.IsDBNull(ordinal)
            ? null
            : _reader.GetValue(ordinal);
    }

    /// <summary>
    ///     Gets the column name by ordinal.
    /// </summary>
    public string GetColumnName(int ordinal)
    {
        return _reader.GetName(ordinal);
    }

    /// <summary>
    ///     Checks if a column with the given name exists.
    /// </summary>
    public bool HasColumn(string columnName)
    {
        return _columnOrdinals.ContainsKey(columnName);
    }

    /// <summary>
    ///     Updates the current row index. Called internally by the source node.
    /// </summary>
    internal void SetCurrentRow(long rowIndex)
    {
        RowIndex = rowIndex;
    }

    private int GetOrdinal(string columnName)
    {
        if (_columnOrdinals.TryGetValue(columnName, out var ordinal))
            return ordinal;

        throw new DuckDBMappingException(
            $"Column '{columnName}' not found. Available columns: {string.Join(", ", _columnOrdinals.Keys)}",
            RowIndex);
    }

    private T ConvertValue<T>(object value, int ordinal)
    {
        var targetType = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);

        try
        {
            // Direct type match
            if (value is T typedValue)
                return typedValue;

            // Enum handling
            if (targetType.IsEnum)
            {
                if (value is string strVal)
                    return (T)Enum.Parse(targetType, strVal, true);

                return (T)Enum.ToObject(targetType, value);
            }

            // Guid handling
            if (targetType == typeof(Guid))
            {
                if (value is Guid guid)
                    return (T)(object)guid;

                if (value is string guidStr)
                    return (T)(object)Guid.Parse(guidStr);
            }

            // DateOnly handling
            if (targetType == typeof(DateOnly))
            {
                if (value is DateTime dt)
                    return (T)(object)DateOnly.FromDateTime(dt);

                if (value is DateOnly d)
                    return (T)(object)d;
            }

            // TimeOnly handling
            if (targetType == typeof(TimeOnly))
            {
                if (value is TimeSpan ts)
                    return (T)(object)TimeOnly.FromTimeSpan(ts);

                if (value is TimeOnly t)
                    return (T)(object)t;
            }

            // DateTimeOffset handling
            if (targetType == typeof(DateTimeOffset))
            {
                if (value is DateTime dtVal)
                    return (T)(object)new DateTimeOffset(dtVal);

                if (value is DateTimeOffset dto)
                    return (T)(object)dto;
            }

            // General conversion
            return (T)Convert.ChangeType(value, targetType);
        }
        catch (Exception ex) when (ex is not DuckDBMappingException)
        {
            throw new DuckDBMappingException(
                $"Cannot convert column '{_reader.GetName(ordinal)}' value of type " +
                $"'{value.GetType().Name}' to '{typeof(T).Name}'.",
                RowIndex,
                ex);
        }
    }
}
