using NPipeline.Connectors.Abstractions;

namespace NPipeline.Connectors.SqlServer.Mapping;

/// <summary>
///     Provides typed, cached access to a SQL Server data row.
/// </summary>
public sealed class SqlServerRow
{
    private readonly bool _caseInsensitive;
    private readonly Dictionary<string, int> _columnIndexes;
    private readonly IDatabaseReader _reader;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SqlServerRow" /> class.
    /// </summary>
    /// <param name="reader">The underlying data reader.</param>
    /// <param name="caseInsensitive">Whether to perform case-insensitive column lookups.</param>
    public SqlServerRow(IDatabaseReader reader, bool caseInsensitive = true)
    {
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _caseInsensitive = caseInsensitive;

        _columnIndexes = new Dictionary<string, int>(caseInsensitive
            ? StringComparer.OrdinalIgnoreCase
            : StringComparer.Ordinal);

        for (var i = 0; i < reader.FieldCount; i++)
        {
            _columnIndexes[reader.GetName(i)] = i;
        }
    }

    /// <summary>
    ///     Gets the number of columns in the current row.
    /// </summary>
    public int FieldCount => _reader.FieldCount;

    /// <summary>
    ///     Gets the column names in the current row.
    /// </summary>
    public IReadOnlyList<string> ColumnNames
    {
        get
        {
            var names = new string[_reader.FieldCount];

            for (var i = 0; i < _reader.FieldCount; i++)
            {
                names[i] = _reader.GetName(i);
            }

            return names;
        }
    }

    /// <summary>
    ///     Gets the name of the column at the specified ordinal.
    /// </summary>
    public string GetName(int ordinal)
    {
        return _reader.GetName(ordinal);
    }

    /// <summary>
    ///     Gets the data type of the column at the specified ordinal.
    /// </summary>
    public Type GetFieldType(int ordinal)
    {
        return _reader.GetFieldType(ordinal);
    }

    /// <summary>
    ///     Checks whether the row contains the specified column.
    /// </summary>
    public bool HasColumn(string name)
    {
        return TryGetOrdinal(name, out _);
    }

    /// <summary>
    ///     Gets the value of the specified column as type <typeparamref name="T" />.
    /// </summary>
    public T Get<T>(string name, T defaultValue = default!)
    {
        return TryGet(name, out var value, defaultValue)
            ? value!
            : defaultValue;
    }

    /// <summary>
    ///     Gets the value of the specified column ordinal as type <typeparamref name="T" />.
    /// </summary>
    public T Get<T>(int ordinal)
    {
        if (_reader.IsDBNull(ordinal))
            return default!;

        return _reader.GetFieldValue<T>(ordinal)!;
    }

    /// <summary>
    ///     Attempts to get a column value by name.
    /// </summary>
    public bool TryGet<T>(string name, out T? value, T defaultValue = default!)
    {
        if (!TryGetOrdinal(name, out var ordinal) || _reader.IsDBNull(ordinal))
        {
            value = defaultValue;
            return false;
        }

        value = _reader.GetFieldValue<T>(ordinal);
        return true;
    }

    /// <summary>
    ///     Attempts to get a column value by ordinal.
    /// </summary>
    public bool TryGet<T>(int ordinal, out T? value)
    {
        if (_reader.IsDBNull(ordinal))
        {
            value = default;
            return false;
        }

        value = _reader.GetFieldValue<T>(ordinal);
        return true;
    }

    /// <summary>
    ///     Gets a column value as an object by name.
    /// </summary>
    public object? GetValue(string name)
    {
        return TryGetOrdinal(name, out var ordinal)
            ? GetValue(ordinal)
            : null;
    }

    /// <summary>
    ///     Gets a column value as an object by ordinal.
    /// </summary>
    public object? GetValue(int ordinal)
    {
        if (_reader.IsDBNull(ordinal))
            return null;

        return _reader.GetFieldValue<object>(ordinal)!;
    }

    /// <summary>
    ///     Determines whether the specified column value is null.
    /// </summary>
    public bool IsDBNull(string name)
    {
        return TryGetOrdinal(name, out var ordinal) && _reader.IsDBNull(ordinal);
    }

    /// <summary>
    ///     Determines whether the specified column ordinal is null.
    /// </summary>
    public bool IsDBNull(int ordinal)
    {
        return _reader.IsDBNull(ordinal);
    }

    private bool TryGetOrdinal(string name, out int ordinal)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            ordinal = -1;
            return false;
        }

        if (_columnIndexes.TryGetValue(name, out ordinal))
            return true;

        if (!_caseInsensitive)
        {
            try
            {
                for (var i = 0; i < _reader.FieldCount; i++)
                {
                    if (_reader.GetName(i) == name)
                    {
                        ordinal = i;
                        _columnIndexes[name] = ordinal;
                        return true;
                    }
                }
            }
            catch (IndexOutOfRangeException)
            {
                // ignore and fall through
            }
        }

        ordinal = -1;
        return false;
    }
}
