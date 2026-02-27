using Parquet.Schema;

namespace NPipeline.Connectors.Parquet;

/// <summary>
///     Represents a row of data from a Parquet file with typed accessors.
///     Intermediate row abstraction similar to CsvRow, JsonRow, and ExcelRow.
/// </summary>
public sealed class ParquetRow
{
    private readonly Dictionary<string, int> _columnNameToIndex;
    private readonly object?[] _values;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ParquetRow" /> class.
    /// </summary>
    /// <param name="values">The column values for this row.</param>
    /// <param name="columnNameToIndex">Mapping from column names (case-insensitive) to indices.</param>
    /// <param name="schema">The Parquet schema for the file.</param>
    internal ParquetRow(object?[] values, Dictionary<string, int> columnNameToIndex, ParquetSchema schema)
    {
        _values = values;
        _columnNameToIndex = columnNameToIndex;
        Schema = schema;
    }

    /// <summary>
    ///     Gets the Parquet schema for the file.
    /// </summary>
    public ParquetSchema Schema { get; }

    /// <summary>
    ///     Gets the number of columns in this row.
    /// </summary>
    public int ColumnCount => _values.Length;

    /// <summary>
    ///     Gets the column names available in this row.
    /// </summary>
    public IReadOnlyList<string> ColumnNames => _columnNameToIndex.Keys.ToList();

    /// <summary>
    ///     Gets a column value by column name (case-insensitive).
    /// </summary>
    /// <param name="columnName">The name of the column.</param>
    /// <returns>The column value, or null if the column does not exist.</returns>
    public object? this[string columnName]
    {
        get
        {
            if (!_columnNameToIndex.TryGetValue(columnName, out var index))
                return null;

            return _values[index];
        }
    }

    /// <summary>
    ///     Gets a column value by zero-based index.
    /// </summary>
    /// <param name="index">The zero-based column index.</param>
    /// <returns>The column value.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if index is out of range.</exception>
    public object? this[int index]
    {
        get
        {
            if (index < 0 || index >= _values.Length)
                throw new ArgumentOutOfRangeException(nameof(index), $"Index {index} is out of range. Column count: {_values.Length}");

            return _values[index];
        }
    }

    /// <summary>
    ///     Gets a typed column value by column name.
    /// </summary>
    /// <typeparam name="T">The type to convert the value to.</typeparam>
    /// <param name="columnName">The name of the column.</param>
    /// <returns>The typed column value.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the column does not exist or conversion fails.</exception>
    public T Get<T>(string columnName)
    {
        var value = this[columnName];

        if (value is null)
        {
            if (default(T) is null)
                return default!;

            throw new InvalidOperationException($"Column '{columnName}' is null and cannot be converted to non-nullable type {typeof(T).Name}.");
        }

        if (value is T typedValue)
            return typedValue;

        return ConvertValue<T>(value, columnName);
    }

    /// <summary>
    ///     Gets a typed column value by zero-based index.
    /// </summary>
    /// <typeparam name="T">The type to convert the value to.</typeparam>
    /// <param name="index">The zero-based column index.</param>
    /// <returns>The typed column value.</returns>
    /// <exception cref="InvalidOperationException">Thrown if conversion fails.</exception>
    public T Get<T>(int index)
    {
        var value = this[index];

        if (value is null)
        {
            if (default(T) is null)
                return default!;

            throw new InvalidOperationException($"Column at index {index} is null and cannot be converted to non-nullable type {typeof(T).Name}.");
        }

        if (value is T typedValue)
            return typedValue;

        return ConvertValue<T>(value, $"index {index}");
    }

    /// <summary>
    ///     Gets a typed column value by column name, or a default value if the column is missing or null.
    /// </summary>
    /// <typeparam name="T">The type to convert the value to.</typeparam>
    /// <param name="columnName">The name of the column.</param>
    /// <param name="defaultValue">The default value to return if the column is missing or null.</param>
    /// <returns>The typed column value, or the default value.</returns>
    public T GetOrDefault<T>(string columnName, T defaultValue = default!)
    {
        if (!_columnNameToIndex.TryGetValue(columnName, out var index))
            return defaultValue;

        var value = _values[index];

        if (value is null)
            return defaultValue;

        if (value is T typedValue)
            return typedValue;

        try
        {
            return ConvertValue<T>(value, columnName);
        }
        catch
        {
            return defaultValue;
        }
    }

    /// <summary>
    ///     Checks whether a column is null by column name.
    /// </summary>
    /// <param name="columnName">The name of the column.</param>
    /// <returns><c>true</c> if the column exists and its value is null; otherwise <c>false</c>.</returns>
    public bool IsNull(string columnName)
    {
        if (!_columnNameToIndex.TryGetValue(columnName, out var index))
            return true;

        return _values[index] is null;
    }

    /// <summary>
    ///     Checks whether a column exists in this row.
    /// </summary>
    /// <param name="columnName">The name of the column.</param>
    /// <returns><c>true</c> if the column exists; otherwise <c>false</c>.</returns>
    public bool HasColumn(string columnName)
    {
        return _columnNameToIndex.ContainsKey(columnName);
    }

    /// <summary>
    ///     Tries to get a typed column value by column name.
    /// </summary>
    /// <typeparam name="T">The type to convert the value to.</typeparam>
    /// <param name="columnName">The name of the column.</param>
    /// <param name="value">The typed column value if successful.</param>
    /// <param name="defaultValue">The default value to use if the column is missing or null.</param>
    /// <returns><c>true</c> if the value was successfully retrieved; otherwise <c>false</c>.</returns>
    public bool TryGet<T>(string columnName, out T value, T defaultValue = default!)
    {
        value = defaultValue;

        if (!_columnNameToIndex.TryGetValue(columnName, out var index))
            return false;

        var columnValue = _values[index];

        if (columnValue is null)
            return false;

        if (columnValue is T typedValue)
        {
            value = typedValue;
            return true;
        }

        try
        {
            value = ConvertValue<T>(columnValue, columnName);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static T ConvertValue<T>(object value, string columnName)
    {
        var targetType = typeof(T);
        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        try
        {
            if (targetType == typeof(string) && value is not null)
                return (T)(object)value.ToString()!;

            if (underlyingType == typeof(Guid) && value is string guidString)
                return (T)(object)Guid.Parse(guidString);

            if (underlyingType == typeof(DateTime) && value is DateTimeOffset dto)
                return (T)(object)dto.DateTime;

            if (underlyingType == typeof(DateTimeOffset) && value is DateTime dt)
                return (T)(object)new DateTimeOffset(dt, TimeSpan.Zero);

            var converted = Convert.ChangeType(value, underlyingType);

            // Handle nullable types - if target is nullable and we got a value, wrap it
            if (targetType != underlyingType && converted is not null)
                return (T)converted;

            return (T)converted!;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                $"Failed to convert column '{columnName}' from {value.GetType().Name} to {targetType.Name}.", ex);
        }
    }
}
