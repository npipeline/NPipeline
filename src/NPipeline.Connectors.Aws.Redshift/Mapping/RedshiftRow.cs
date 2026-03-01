using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Aws.Redshift.Mapping;

/// <summary>
///     Provides typed, cached access to a Redshift data row.
///     Column lookups are case-insensitive by default.
/// </summary>
public sealed class RedshiftRow
{
    private readonly bool _caseInsensitive;
    private readonly Dictionary<string, int> _columnIndexes;
    private readonly List<string> _columnNames;
    private readonly IDatabaseReader _reader;

    /// <summary>
    ///     Initializes a new instance of the <see cref="RedshiftRow" /> class.
    /// </summary>
    /// <param name="reader">The database reader to wrap.</param>
    /// <param name="caseInsensitive">Whether column name lookups should be case-insensitive. Default is true.</param>
    public RedshiftRow(IDatabaseReader reader, bool caseInsensitive = true)
    {
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _caseInsensitive = caseInsensitive;

        _columnIndexes = new Dictionary<string, int>(caseInsensitive
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal);

        _columnNames = new List<string>();

        for (var i = 0; i < reader.FieldCount; i++)
        {
            var name = reader.GetName(i);
            _columnIndexes[name] = i;
            _columnNames.Add(name);
        }
    }

    /// <summary>
    ///     Gets the number of columns in the current row.
    /// </summary>
    public int FieldCount => _reader.FieldCount;

    /// <summary>
    ///     Gets the list of column names in the current row.
    /// </summary>
    public IReadOnlyList<string> ColumnNames => _columnNames;

    /// <summary>
    ///     Gets the value of the specified column by name.
    /// </summary>
    /// <typeparam name="T">The type of the value to return.</typeparam>
    /// <param name="name">The name of the column.</param>
    /// <param name="defaultValue">The default value to return if the column is not found or is null.</param>
    /// <returns>The column value or the default value.</returns>
    public T Get<T>(string name, T defaultValue = default!)
    {
        if (!TryGetOrdinal(name, out var ordinal))
            return defaultValue;

        return Get<T>(ordinal, defaultValue);
    }

    /// <summary>
    ///     Gets the value of the specified column by ordinal position.
    /// </summary>
    /// <typeparam name="T">The type of the value to return.</typeparam>
    /// <param name="ordinal">The zero-based column ordinal.</param>
    /// <param name="defaultValue">The default value to return if the column is null.</param>
    /// <returns>The column value or the default value.</returns>
    public T Get<T>(int ordinal, T defaultValue = default!)
    {
        if (_reader.IsDBNull(ordinal))
            return defaultValue;

        var value = _reader.GetFieldValue<T>(ordinal);
        return value ?? defaultValue;
    }

    /// <summary>
    ///     Checks if the specified column value is null.
    /// </summary>
    /// <param name="name">The name of the column.</param>
    /// <returns>True if the column is null or does not exist; otherwise, false.</returns>
    public bool IsNull(string name)
    {
        if (!TryGetOrdinal(name, out var ordinal))
            return true;

        return _reader.IsDBNull(ordinal);
    }

    /// <summary>
    ///     Checks if a column with the specified name exists in the row.
    /// </summary>
    /// <param name="name">The name of the column.</param>
    /// <returns>True if the column exists; otherwise, false.</returns>
    public bool HasColumn(string name)
    {
        return TryGetOrdinal(name, out _);
    }

    /// <summary>
    ///     Tries to get the ordinal position of the specified column.
    /// </summary>
    /// <param name="name">The name of the column.</param>
    /// <param name="ordinal">When this method returns, contains the zero-based column ordinal if found; otherwise, -1.</param>
    /// <returns>True if the column was found; otherwise, false.</returns>
    public bool TryGetOrdinal(string name, out int ordinal)
    {
        return _columnIndexes.TryGetValue(name, out ordinal);
    }
}
