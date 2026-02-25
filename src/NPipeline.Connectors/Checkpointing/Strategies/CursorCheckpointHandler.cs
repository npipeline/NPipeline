using System.Text.Json;

namespace NPipeline.Connectors.Checkpointing.Strategies;

/// <summary>
///     Handler for cursor-based checkpointing.
///     Tracks database cursor positions for resumable processing.
/// </summary>
public class CursorCheckpointHandler
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly CheckpointManager _checkpointManager;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CursorCheckpointHandler" /> class.
    /// </summary>
    /// <param name="checkpointManager">The checkpoint manager.</param>
    public CursorCheckpointHandler(CheckpointManager checkpointManager)
    {
        ArgumentNullException.ThrowIfNull(checkpointManager);
        _checkpointManager = checkpointManager;
    }

    /// <summary>
    ///     Loads the cursor position from the checkpoint.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The cursor position, or null if no checkpoint exists.</returns>
    public async Task<CursorPosition?> LoadCursorPositionAsync(CancellationToken cancellationToken = default)
    {
        var checkpoint = await _checkpointManager.LoadAsync(cancellationToken);

        if (checkpoint == null)
            return null;

        return DeserializeCursorPosition(checkpoint.Value);
    }

    /// <summary>
    ///     Updates the checkpoint with a new cursor position.
    /// </summary>
    /// <param name="cursorPosition">The cursor position to store.</param>
    /// <param name="forceSave">Force immediate save.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task UpdateCursorPositionAsync(
        CursorPosition cursorPosition,
        bool forceSave = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(cursorPosition);

        var serializedValue = SerializeCursorPosition(cursorPosition);

        var metadata = new Dictionary<string, string>
        {
            ["cursor_type"] = cursorPosition.Type,
            ["updated_at"] = DateTimeOffset.UtcNow.ToString("O"),
        };

        if (cursorPosition.RowCount.HasValue)
            metadata["row_count"] = cursorPosition.RowCount.Value.ToString();

        await _checkpointManager.UpdateAsync(serializedValue, metadata, forceSave, cancellationToken);
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
    ///     Serializes a cursor position to a string.
    /// </summary>
    private static string SerializeCursorPosition(CursorPosition position)
    {
        return JsonSerializer.Serialize(position, JsonOptions);
    }

    /// <summary>
    ///     Deserializes a cursor position from a string.
    /// </summary>
    private static CursorPosition? DeserializeCursorPosition(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        try
        {
            return JsonSerializer.Deserialize<CursorPosition>(value, JsonOptions);
        }
        catch
        {
            return null;
        }
    }
}

/// <summary>
///     Represents a cursor position in a database query.
/// </summary>
public sealed record CursorPosition
{
    /// <summary>
    ///     Gets or sets the cursor identifier or name.
    /// </summary>
    public string? CursorId { get; init; }

    /// <summary>
    ///     Gets or sets the cursor type (e.g., "keyset", "offset", "fetch").
    /// </summary>
    public string Type { get; init; } = "keyset";

    /// <summary>
    ///     Gets or sets the current offset position (for offset-based cursors).
    /// </summary>
    public long? Offset { get; init; }

    /// <summary>
    ///     Gets or sets the last seen key values (for keyset pagination).
    /// </summary>
    public Dictionary<string, object?>? LastKeyValues { get; init; }

    /// <summary>
    ///     Gets or sets the total number of rows processed.
    /// </summary>
    public long? RowCount { get; init; }

    /// <summary>
    ///     Gets or sets the fetch size being used.
    /// </summary>
    public int? FetchSize { get; init; }

    /// <summary>
    ///     Gets or sets whether the cursor has reached the end of the result set.
    /// </summary>
    public bool IsExhausted { get; init; }

    /// <summary>
    ///     Creates a keyset-based cursor position.
    /// </summary>
    /// <param name="lastKeyValues">The last seen key values.</param>
    /// <param name="rowCount">Optional row count.</param>
    /// <returns>A new cursor position.</returns>
    public static CursorPosition Keyset(Dictionary<string, object?> lastKeyValues, long? rowCount = null)
    {
        return new CursorPosition
        {
            Type = "keyset",
            LastKeyValues = lastKeyValues,
            RowCount = rowCount,
        };
    }

    /// <summary>
    ///     Creates an offset-based cursor position.
    /// </summary>
    /// <param name="offset">The current offset.</param>
    /// <param name="rowCount">Optional row count.</param>
    /// <param name="fetchSize">The fetch size.</param>
    /// <returns>A new cursor position.</returns>
    public static CursorPosition FromOffset(long offset, long? rowCount = null, int fetchSize = 100)
    {
        return new CursorPosition
        {
            Type = "offset",
            Offset = offset,
            RowCount = rowCount,
            FetchSize = fetchSize,
        };
    }
}
