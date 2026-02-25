using System.Text.Json.Serialization;

namespace NPipeline.Connectors.Checkpointing;

/// <summary>
///     Represents a checkpoint for resumable data processing.
/// </summary>
public record Checkpoint
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="Checkpoint" /> record.
    /// </summary>
    /// <param name="value">The checkpoint value (format depends on strategy).</param>
    /// <param name="timestamp">The timestamp when the checkpoint was created.</param>
    /// <param name="metadata">Optional metadata associated with the checkpoint.</param>
    [JsonConstructor]
    public Checkpoint(string value, DateTimeOffset timestamp, Dictionary<string, string>? metadata = null)
    {
        Value = value ?? throw new ArgumentNullException(nameof(value));
        Timestamp = timestamp;
        Metadata = metadata;
    }

    /// <summary>
    ///     Gets the checkpoint value.
    ///     The format depends on the checkpoint strategy:
    ///     - Offset: numeric string (e.g., "12345")
    ///     - KeyBased: serialized key values (JSON)
    ///     - Cursor: cursor identifier or position
    ///     - CDC: LSN for SQL Server, WAL position for PostgreSQL
    /// </summary>
    public string Value { get; init; }

    /// <summary>
    ///     Gets the timestamp when the checkpoint was created.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    ///     Gets optional metadata associated with the checkpoint.
    ///     Can include additional context like row counts, processing time, etc.
    /// </summary>
    public Dictionary<string, string>? Metadata { get; init; }

    /// <summary>
    ///     Creates a new checkpoint with the current timestamp.
    /// </summary>
    /// <param name="value">The checkpoint value.</param>
    /// <param name="metadata">Optional metadata.</param>
    /// <returns>A new checkpoint instance.</returns>
    public static Checkpoint Create(string value, Dictionary<string, string>? metadata = null)
    {
        return new Checkpoint(value, DateTimeOffset.UtcNow, metadata);
    }

    /// <summary>
    ///     Creates a new offset-based checkpoint.
    /// </summary>
    /// <param name="offset">The numeric offset.</param>
    /// <param name="metadata">Optional metadata.</param>
    /// <returns>A new checkpoint instance.</returns>
    public static Checkpoint FromOffset(long offset, Dictionary<string, string>? metadata = null)
    {
        return new Checkpoint(offset.ToString(), DateTimeOffset.UtcNow, metadata);
    }

    /// <summary>
    ///     Parses the checkpoint value as a numeric offset.
    /// </summary>
    /// <returns>The numeric offset if parsing succeeds; otherwise, null.</returns>
    public long? GetAsOffset()
    {
        return long.TryParse(Value, out var offset)
            ? offset
            : null;
    }
}

/// <summary>
///     Represents a CDC-specific checkpoint with position information.
/// </summary>
public record CdcCheckpoint : Checkpoint
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="CdcCheckpoint" /> record.
    /// </summary>
    /// <param name="position">The CDC position (LSN for SQL Server, WAL position for PostgreSQL).</param>
    /// <param name="timestamp">The timestamp when the checkpoint was created.</param>
    /// <param name="metadata">Optional metadata.</param>
    public CdcCheckpoint(string position, DateTimeOffset timestamp, Dictionary<string, string>? metadata = null)
        : base(position, timestamp, metadata)
    {
    }

    /// <summary>
    ///     Gets or sets the sequence number within the transaction.
    ///     Used for SQL Server CDC to track position within a transaction.
    /// </summary>
    public int? SequenceNumber { get; init; }

    /// <summary>
    ///     Gets or sets the transaction ID associated with this checkpoint.
    /// </summary>
    public string? TransactionId { get; init; }
}

/// <summary>
///     Represents a key-based checkpoint for composite key tracking.
/// </summary>
public record KeyBasedCheckpoint : Checkpoint
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="KeyBasedCheckpoint" /> record.
    /// </summary>
    /// <param name="keyValues">The key values as a dictionary.</param>
    /// <param name="timestamp">The timestamp when the checkpoint was created.</param>
    /// <param name="metadata">Optional metadata.</param>
    public KeyBasedCheckpoint(
        IReadOnlyDictionary<string, object?> keyValues,
        DateTimeOffset timestamp,
        Dictionary<string, string>? metadata = null)
        : base(SerializeKeyValues(keyValues), timestamp, metadata)
    {
        KeyValues = keyValues;
    }

    /// <summary>
    ///     Gets the key values for this checkpoint.
    /// </summary>
    public IReadOnlyDictionary<string, object?> KeyValues { get; init; }

    private static string SerializeKeyValues(IReadOnlyDictionary<string, object?> keyValues)
    {
        var pairs = keyValues.Select(kv => $"{kv.Key}={kv.Value}");
        return string.Join("|", pairs);
    }
}
