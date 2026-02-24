using Cassandra;

namespace NPipeline.Connectors.Azure.CosmosDb.Mapping;

/// <summary>
///     Cassandra-backed row wrapper for Cosmos Cassandra API.
/// </summary>
public sealed class CosmosCassandraRow : ICosmosDataWrapper
{
    private readonly Row _row;

    /// <summary>
    ///     Initializes a new instance of <see cref="CosmosCassandraRow" />.
    /// </summary>
    /// <param name="row">The Cassandra row.</param>
    public CosmosCassandraRow(Row row)
    {
        _row = row ?? throw new ArgumentNullException(nameof(row));
    }

    /// <inheritdoc />
    public T Get<T>(string name, T defaultValue = default!)
    {
        try
        {
            return _row.GetValue<T>(name);
        }
        catch
        {
            return defaultValue;
        }
    }

    /// <inheritdoc />
    public object? GetValue(string name)
    {
        try
        {
            return _row.GetValue<object>(name);
        }
        catch
        {
            return null;
        }
    }

    /// <inheritdoc />
    public bool HasColumn(string name)
    {
        try
        {
            _ = _row.GetValue<object>(name);
            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <inheritdoc />
    public Dictionary<string, object?> ToDictionary()
    {
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        var columnsProperty = _row.GetType().GetProperty("Columns");

        if (columnsProperty?.GetValue(_row) is IEnumerable<object> columns)
        {
            foreach (var column in columns)
            {
                var nameProperty = column.GetType().GetProperty("Name");
                var name = nameProperty?.GetValue(column)?.ToString();

                if (string.IsNullOrWhiteSpace(name))
                    continue;

                result[name] = _row.GetValue<object>(name);
            }
        }

        return result;
    }
}
