using System.Text.Json;
using NPipeline.Connectors.Checkpointing;

namespace NPipeline.Connectors.Postgres.Checkpointing;

/// <summary>
///     Handler for PostgreSQL CDC (Change Data Capture) checkpointing.
///     Tracks WAL (Write-Ahead Log) positions for logical replication.
/// </summary>
public class PostgresCdcCheckpointHandler
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly CheckpointManager _checkpointManager;

    /// <summary>
    ///     Initializes a new instance of the <see cref="PostgresCdcCheckpointHandler" /> class.
    /// </summary>
    /// <param name="checkpointManager">The checkpoint manager.</param>
    /// <param name="slotName">The logical replication slot name.</param>
    /// <param name="publicationName">Optional publication name.</param>
    public PostgresCdcCheckpointHandler(
        CheckpointManager checkpointManager,
        string slotName,
        string? publicationName = null)
    {
        ArgumentNullException.ThrowIfNull(checkpointManager);

        if (string.IsNullOrWhiteSpace(slotName))
            throw new ArgumentException("Slot name cannot be empty.", nameof(slotName));

        _checkpointManager = checkpointManager;
        SlotName = slotName;
        PublicationName = publicationName;
    }

    /// <summary>
    ///     Gets the replication slot name.
    /// </summary>
    public string SlotName { get; }

    /// <summary>
    ///     Gets the publication name.
    /// </summary>
    public string? PublicationName { get; }

    /// <summary>
    ///     Loads the CDC position from the checkpoint.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The CDC position, or null if no checkpoint exists.</returns>
    public async Task<PostgresCdcPosition?> LoadPositionAsync(CancellationToken cancellationToken = default)
    {
        var checkpoint = await _checkpointManager.LoadAsync(cancellationToken);

        if (checkpoint == null)
            return null;

        return DeserializePosition(checkpoint.Value);
    }

    /// <summary>
    ///     Updates the checkpoint with a new CDC position.
    /// </summary>
    /// <param name="position">The CDC position to store.</param>
    /// <param name="forceSave">Force immediate save.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task UpdatePositionAsync(
        PostgresCdcPosition position,
        bool forceSave = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(position);

        var serializedValue = SerializePosition(position);

        var metadata = new Dictionary<string, string>
        {
            ["slot_name"] = SlotName,
            ["updated_at"] = DateTimeOffset.UtcNow.ToString("O"),
        };

        if (!string.IsNullOrEmpty(PublicationName))
            metadata["publication_name"] = PublicationName;

        if (position.TransactionCount.HasValue)
            metadata["transaction_count"] = position.TransactionCount.Value.ToString();

        await _checkpointManager.UpdateAsync(serializedValue, metadata, forceSave, cancellationToken);
    }

    /// <summary>
    ///     Updates the checkpoint from WAL LSN values.
    /// </summary>
    /// <param name="walLsn">The WAL LSN (Log Sequence Number).</param>
    /// <param name="transactionId">Optional transaction ID.</param>
    /// <param name="forceSave">Force immediate save.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task UpdateFromWalLsnAsync(
        string walLsn,
        string? transactionId = null,
        bool forceSave = false,
        CancellationToken cancellationToken = default)
    {
        var position = new PostgresCdcPosition
        {
            WalLsn = walLsn,
            TransactionId = transactionId,
        };

        await UpdatePositionAsync(position, forceSave, cancellationToken);
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
    ///     Serializes a CDC position to a string.
    /// </summary>
    private static string SerializePosition(PostgresCdcPosition position)
    {
        return JsonSerializer.Serialize(position, JsonOptions);
    }

    /// <summary>
    ///     Deserializes a CDC position from a string.
    /// </summary>
    private static PostgresCdcPosition? DeserializePosition(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        try
        {
            return JsonSerializer.Deserialize<PostgresCdcPosition>(value, JsonOptions);
        }
        catch
        {
            return null;
        }
    }
}

/// <summary>
///     Represents a PostgreSQL CDC position (WAL position).
/// </summary>
public sealed record PostgresCdcPosition
{
    /// <summary>
    ///     Gets or sets the WAL LSN (Log Sequence Number).
    ///     Format is typically "XX/XXXXXXXX" (e.g., "0/16B6F48").
    /// </summary>
    public string WalLsn { get; init; } = string.Empty;

    /// <summary>
    ///     Gets or sets the restart LSN for the replication slot.
    ///     Used to resume from a consistent point.
    /// </summary>
    public string? RestartLsn { get; init; }

    /// <summary>
    ///     Gets or sets the transaction ID being processed.
    /// </summary>
    public string? TransactionId { get; init; }

    /// <summary>
    ///     Gets or sets the total number of transactions processed.
    /// </summary>
    public long? TransactionCount { get; init; }

    /// <summary>
    ///     Gets or sets the timestamp of the last processed change.
    /// </summary>
    public DateTimeOffset? LastChangeTimestamp { get; init; }

    /// <summary>
    ///     Gets or sets whether the CDC stream has caught up to current.
    /// </summary>
    public bool IsCaughtUp { get; init; }

    /// <summary>
    ///     Creates a position from a WAL LSN string.
    /// </summary>
    /// <param name="walLsn">The WAL LSN.</param>
    /// <returns>A new CDC position.</returns>
    public static PostgresCdcPosition FromWalLsn(string walLsn)
    {
        return new PostgresCdcPosition { WalLsn = walLsn };
    }

    /// <summary>
    ///     Parses the WAL LSN to a 64-bit integer.
    /// </summary>
    /// <returns>The 64-bit LSN value, or null if parsing fails.</returns>
    public ulong? ParseWalLsnAsInt64()
    {
        if (string.IsNullOrEmpty(WalLsn))
            return null;

        try
        {
            // WAL LSN format is "XX/XXXXXXXX"
            var parts = WalLsn.Split('/');

            if (parts.Length != 2)
                return null;

            var high = Convert.ToUInt64(parts[0], 16);
            var low = Convert.ToUInt64(parts[1], 16);

            return (high << 32) | low;
        }
        catch
        {
            return null;
        }
    }
}
