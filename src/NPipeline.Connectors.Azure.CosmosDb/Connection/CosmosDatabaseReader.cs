using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Azure.CosmosDb.Connection;

/// <summary>
///     Cosmos DB implementation of IDatabaseReader.
///     Wraps FeedIterator for database-agnostic operations.
/// </summary>
internal sealed class CosmosDatabaseReader : IDatabaseReader
{
    private readonly List<Dictionary<string, object?>> _currentRows = [];
    private readonly FeedIterator _feedIterator;
    private Dictionary<string, int>? _columnIndexes;
    private int _currentRowIndex = -1;
    private bool _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CosmosDatabaseReader" /> class.
    /// </summary>
    /// <param name="feedIterator">The Cosmos DB feed iterator.</param>
    public CosmosDatabaseReader(FeedIterator feedIterator)
    {
        _feedIterator = feedIterator;
    }

    /// <summary>
    ///     Gets the current row data.
    /// </summary>
    public Dictionary<string, object?>? CurrentRow =>
        _currentRowIndex >= 0 && _currentRowIndex < _currentRows.Count
            ? _currentRows[_currentRowIndex]
            : null;

    /// <summary>
    ///     Gets a value indicating whether reader has rows.
    /// </summary>
    public bool HasRows => _currentRows.Count > 0;

    /// <summary>
    ///     Gets number of columns in current row.
    /// </summary>
    public int FieldCount => _columnIndexes?.Count ?? 0;

    /// <summary>
    ///     Gets column name by ordinal position.
    /// </summary>
    /// <param name="ordinal">The column ordinal.</param>
    /// <returns>The column name.</returns>
    public string GetName(int ordinal)
    {
        if (_columnIndexes == null)
            throw new InvalidOperationException("No data available. Call ReadAsync first.");

        var entry = _columnIndexes.FirstOrDefault(x => x.Value == ordinal);
        return entry.Key ?? throw new ArgumentOutOfRangeException(nameof(ordinal));
    }

    /// <summary>
    ///     Gets column type by ordinal position.
    /// </summary>
    /// <param name="ordinal">The column ordinal.</param>
    /// <returns>The column type.</returns>
    public Type GetFieldType(int ordinal)
    {
        var value = GetFieldValue<object>(ordinal);
        return value?.GetType() ?? typeof(object);
    }

    /// <summary>
    ///     Advances reader to next row.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>True if there are more rows, false otherwise.</returns>
    public async Task<bool> ReadAsync(CancellationToken cancellationToken = default)
    {
        _currentRowIndex++;

        // If we've exhausted current page, try to get more
        while (_currentRowIndex >= _currentRows.Count)
        {
            if (!_feedIterator.HasMoreResults)
                return false;

            try
            {
                var response = await _feedIterator.ReadNextAsync(cancellationToken);
                _currentRows.Clear();
                _currentRowIndex = 0;

                // Parse the stream response
                using var stream = response.Content;
                var rows = await ParseStreamResponseAsync(stream, cancellationToken);

                foreach (var row in rows)
                {
                    _currentRows.Add(row);
                }

                // Build column indexes from first row
                if (_columnIndexes == null && _currentRows.Count > 0)
                    BuildColumnIndexes(_currentRows[0]);
            }
            catch (CosmosException ex)
            {
                throw new InvalidOperationException($"Error reading from Cosmos DB: {ex.Message}", ex);
            }
        }

        return _currentRowIndex < _currentRows.Count;
    }

    /// <summary>
    ///     Advances reader to next result set.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>True if there are more result sets, false otherwise.</returns>
    public Task<bool> NextResultAsync(CancellationToken cancellationToken = default)
    {
        // Cosmos DB doesn't support multiple result sets
        return Task.FromResult(false);
    }

    /// <summary>
    ///     Gets field value by ordinal position.
    /// </summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="ordinal">The column ordinal.</param>
    /// <returns>The field value.</returns>
    public T? GetFieldValue<T>(int ordinal)
    {
        var row = CurrentRow ?? throw new InvalidOperationException("No current row. Call ReadAsync first.");
        var columnName = GetName(ordinal);

        if (!row.TryGetValue(columnName, out var value))
            return default;

        if (value == null)
            return default;

        if (value is T typedValue)
            return typedValue;

        // Try to convert
        var targetType = typeof(T);
        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        try
        {
            return (T?)Convert.ChangeType(value, underlyingType);
        }
        catch (InvalidCastException)
        {
            throw new InvalidCastException(
                $"Cannot cast value of type '{value.GetType().Name}' to '{typeof(T).Name}' for column '{columnName}'.");
        }
    }

    /// <summary>
    ///     Checks if field value is DBNull.
    /// </summary>
    /// <param name="ordinal">The column ordinal.</param>
    /// <returns>True if value is DBNull, false otherwise.</returns>
    public bool IsDBNull(int ordinal)
    {
        var row = CurrentRow ?? throw new InvalidOperationException("No current row. Call ReadAsync first.");
        var columnName = GetName(ordinal);

        if (!row.TryGetValue(columnName, out var value))
            return true;

        return value == null;
    }

    /// <summary>
    ///     Disposes the reader asynchronously.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _feedIterator.Dispose();
        _currentRows.Clear();
        _columnIndexes = null;
        _disposed = true;

        await ValueTask.CompletedTask;
    }

    /// <summary>
    ///     Gets the column ordinal by name.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <returns>The column ordinal.</returns>
    public int GetOrdinal(string columnName)
    {
        if (_columnIndexes == null)
            throw new InvalidOperationException("No data available. Call ReadAsync first.");

        if (!_columnIndexes.TryGetValue(columnName, out var ordinal))
            throw new ArgumentException($"Column '{columnName}' not found.", nameof(columnName));

        return ordinal;
    }

    private static Dictionary<string, object?> ExtractRow(dynamic item)
    {
        var row = new Dictionary<string, object?>();

        // Handle Dictionary types
        if (item is IDictionary<string, object?> dict)
        {
            foreach (var kvp in dict)
            {
                row[kvp.Key] = kvp.Value;
            }
        }
        else if (item is JObject jObj)
        {
            foreach (var prop in jObj.Properties())
            {
                row[prop.Name] = prop.Value.ToObject<object>();
            }
        }
        else
        {
            // Use reflection for other types
            var type = item.GetType();

            foreach (var prop in type.GetProperties())
            {
                try
                {
                    row[prop.Name] = prop.GetValue(item);
                }
                catch
                {
                    row[prop.Name] = null;
                }
            }
        }

        return row;
    }

    private void BuildColumnIndexes(Dictionary<string, object?> sampleRow)
    {
        _columnIndexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var index = 0;

        foreach (var key in sampleRow.Keys)
        {
            _columnIndexes[key] = index++;
        }
    }

    private static async Task<List<Dictionary<string, object?>>> ParseStreamResponseAsync(Stream stream, CancellationToken cancellationToken)
    {
        var rows = new List<Dictionary<string, object?>>();

        using var reader = new StreamReader(stream);
        var content = await reader.ReadToEndAsync(cancellationToken);

        try
        {
            var json = JObject.Parse(content);
            var documents = json["Documents"] as JArray;

            if (documents != null)
            {
                foreach (var doc in documents)
                {
                    var row = new Dictionary<string, object?>();

                    foreach (var prop in doc.Children<JProperty>())
                    {
                        row[prop.Name] = prop.Value.ToObject<object>();
                    }

                    rows.Add(row);
                }
            }
        }
        catch (JsonException)
        {
            // Return empty list if parsing fails
        }

        return rows;
    }
}
