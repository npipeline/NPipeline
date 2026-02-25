using System.Text.Json;
using NPipeline.StorageProviders.Utilities;

namespace NPipeline.Connectors.Checkpointing.Strategies;

/// <summary>
///     Handler for key-based checkpointing.
///     Tracks composite key values for resumable processing.
/// </summary>
public class KeyBasedCheckpointHandler
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    private readonly CheckpointManager _checkpointManager;
    private readonly string[] _keyColumns;
    private readonly string[] _quotedKeyColumns;

    /// <summary>
    ///     Initializes a new instance of the <see cref="KeyBasedCheckpointHandler" /> class.
    /// </summary>
    /// <param name="checkpointManager">The checkpoint manager.</param>
    /// <param name="keyColumns">The column names that form the composite key.</param>
    public KeyBasedCheckpointHandler(CheckpointManager checkpointManager, params string[] keyColumns)
    {
        ArgumentNullException.ThrowIfNull(checkpointManager);

        if (keyColumns == null || keyColumns.Length == 0)
            throw new ArgumentException("At least one key column must be specified.", nameof(keyColumns));

        _checkpointManager = checkpointManager;
        _keyColumns = new string[keyColumns.Length];
        _quotedKeyColumns = new string[keyColumns.Length];

        // Validate and quote all column names to prevent SQL injection
        for (var i = 0; i < keyColumns.Length; i++)
        {
            var column = keyColumns[i];
            DatabaseIdentifierValidator.ValidateIdentifier(column, nameof(keyColumns));
            _keyColumns[i] = column;
            _quotedKeyColumns[i] = DatabaseIdentifierValidator.QuoteIdentifier(column);
        }
    }

    /// <summary>
    ///     Gets the key column names.
    /// </summary>
    public IReadOnlyList<string> KeyColumns => _keyColumns;

    /// <summary>
    ///     Loads the checkpoint and returns the key values.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The key values dictionary, or null if no checkpoint exists.</returns>
    public async Task<Dictionary<string, object?>?> LoadKeyValuesAsync(CancellationToken cancellationToken = default)
    {
        var checkpoint = await _checkpointManager.LoadAsync(cancellationToken);

        if (checkpoint == null)
            return null;

        return DeserializeKeyValues(checkpoint.Value);
    }

    /// <summary>
    ///     Updates the checkpoint with new key values.
    /// </summary>
    /// <param name="keyValues">The key values to store.</param>
    /// <param name="forceSave">Force immediate save.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task UpdateKeyValuesAsync(
        IReadOnlyDictionary<string, object?> keyValues,
        bool forceSave = false,
        CancellationToken cancellationToken = default)
    {
        var serializedValue = SerializeKeyValues(keyValues);
        var metadata = new Dictionary<string, string>
        {
            ["key_columns"] = string.Join(",", _keyColumns),
            ["updated_at"] = DateTimeOffset.UtcNow.ToString("O")
        };

        await _checkpointManager.UpdateAsync(serializedValue, metadata, forceSave, cancellationToken);
    }

    /// <summary>
    ///     Generates a WHERE clause fragment for filtering based on composite key values.
    ///     Uses row-value comparison syntax for composite keys.
    /// </summary>
    /// <param name="keyValues">The key values to compare against.</param>
    /// <param name="parameterPrefix">The prefix for parameter names.</param>
    /// <returns>A tuple containing the WHERE clause and parameter dictionary.</returns>
    public (string WhereClause, Dictionary<string, object?> Parameters) GenerateWhereClause(
        IReadOnlyDictionary<string, object?> keyValues,
        string parameterPrefix = "@key")
    {
        if (keyValues == null || keyValues.Count == 0)
            return (string.Empty, new Dictionary<string, object?>());

        var conditions = new List<string>();
        var parameters = new Dictionary<string, object?>();

        for (var i = 0; i < _keyColumns.Length; i++)
        {
            var column = _keyColumns[i];
            var quotedColumn = _quotedKeyColumns[i];
            var paramName = $"{parameterPrefix}_{i}";

            if (keyValues.TryGetValue(column, out var value))
            {
                // Use quoted column name to prevent SQL injection
                conditions.Add($"{quotedColumn} > {paramName}");
                parameters[paramName] = value;
            }
        }

        var whereClause = conditions.Count > 0
            ? string.Join(" AND ", conditions)
            : string.Empty;

        return (whereClause, parameters);
    }

    /// <summary>
    ///     Generates a WHERE clause using row-value comparison (column1, column2) > (val1, val2).
    ///     This is more efficient for composite key ordering.
    /// </summary>
    /// <param name="keyValues">The key values to compare against.</param>
    /// <param name="parameterPrefix">The prefix for parameter names.</param>
    /// <returns>A tuple containing the WHERE clause and parameter dictionary.</returns>
    public (string WhereClause, Dictionary<string, object?> Parameters) GenerateRowValueWhereClause(
        IReadOnlyDictionary<string, object?> keyValues,
        string parameterPrefix = "@key")
    {
        if (keyValues == null || keyValues.Count == 0)
            return (string.Empty, new Dictionary<string, object?>());

        var columns = new List<string>();
        var parameters = new Dictionary<string, object?>();

        for (var i = 0; i < _keyColumns.Length; i++)
        {
            var column = _keyColumns[i];
            var quotedColumn = _quotedKeyColumns[i];
            var paramName = $"{parameterPrefix}_{i}";

            // Use quoted column name to prevent SQL injection
            columns.Add(quotedColumn);

            if (keyValues.TryGetValue(column, out var value))
                parameters[paramName] = value;
            else
                parameters[paramName] = null;
        }

        var columnList = string.Join(", ", columns);
        var paramList = string.Join(", ", parameters.Keys.Select(k => k));

        var whereClause = $"({columnList}) > ({paramList})";

        return (whereClause, parameters);
    }

    /// <summary>
    ///     Saves the checkpoint.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task SaveAsync(CancellationToken cancellationToken = default)
    {
        await _checkpointManager.SaveAsync(cancellationToken);
    }

    /// <summary>
    ///     Clears the checkpoint.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task ClearAsync(CancellationToken cancellationToken = default)
    {
        await _checkpointManager.ClearAsync(cancellationToken);
    }

    /// <summary>
    ///     Serializes key values to a string.
    /// </summary>
    private static string SerializeKeyValues(IReadOnlyDictionary<string, object?> keyValues)
    {
        return JsonSerializer.Serialize(keyValues, JsonOptions);
    }

    /// <summary>
    ///     Deserializes key values from a string.
    /// </summary>
    private static Dictionary<string, object?>? DeserializeKeyValues(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        try
        {
            var deserialized = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(value, JsonOptions);
            if (deserialized == null)
                return null;

            var result = new Dictionary<string, object?>();

            foreach (var kvp in deserialized)
            {
                result[kvp.Key] = kvp.Value.ValueKind switch
                {
                    JsonValueKind.String => kvp.Value.GetString(),
                    JsonValueKind.Number => kvp.Value.TryGetInt64(out var l) ? l : kvp.Value.GetDouble(),
                    JsonValueKind.True => true,
                    JsonValueKind.False => false,
                    JsonValueKind.Null => null,
                    _ => kvp.Value.ToString()
                };
            }

            return result;
        }
        catch
        {
            return null;
        }
    }
}
