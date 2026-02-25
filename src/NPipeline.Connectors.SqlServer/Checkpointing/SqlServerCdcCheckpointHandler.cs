using System.Text.Json;
using NPipeline.Connectors.Checkpointing;

namespace NPipeline.Connectors.SqlServer.Checkpointing;

/// <summary>
///     Handler for SQL Server CDC (Change Data Capture) checkpointing.
///     Tracks LSN (Log Sequence Number) for change data capture.
/// </summary>
public class SqlServerCdcCheckpointHandler
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly CheckpointManager _checkpointManager;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SqlServerCdcCheckpointHandler" /> class.
    /// </summary>
    /// <param name="checkpointManager">The checkpoint manager.</param>
    /// <param name="captureInstance">The CDC capture instance name.</param>
    public SqlServerCdcCheckpointHandler(CheckpointManager checkpointManager, string captureInstance)
    {
        ArgumentNullException.ThrowIfNull(checkpointManager);

        if (string.IsNullOrWhiteSpace(captureInstance))
            throw new ArgumentException("Capture instance cannot be empty.", nameof(captureInstance));

        _checkpointManager = checkpointManager;
        CaptureInstance = captureInstance;
    }

    /// <summary>
    ///     Gets the capture instance name.
    /// </summary>
    public string CaptureInstance { get; }

    /// <summary>
    ///     Loads the CDC position from the checkpoint.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The CDC position, or null if no checkpoint exists.</returns>
    public async Task<SqlServerCdcPosition?> LoadPositionAsync(CancellationToken cancellationToken = default)
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
        SqlServerCdcPosition position,
        bool forceSave = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(position);

        var serializedValue = SerializePosition(position);

        var metadata = new Dictionary<string, string>
        {
            ["capture_instance"] = CaptureInstance,
            ["updated_at"] = DateTimeOffset.UtcNow.ToString("O"),
        };

        if (position.ChangeCount.HasValue)
            metadata["change_count"] = position.ChangeCount.Value.ToString();

        await _checkpointManager.UpdateAsync(serializedValue, metadata, forceSave, cancellationToken);
    }

    /// <summary>
    ///     Updates the checkpoint from LSN values.
    /// </summary>
    /// <param name="startLsn">The start LSN.</param>
    /// <param name="seqVal">The sequence value within the transaction.</param>
    /// <param name="operation">The CDC operation type.</param>
    /// <param name="forceSave">Force immediate save.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task UpdateFromLsnAsync(
        byte[] startLsn,
        byte[]? seqVal = null,
        int? operation = null,
        bool forceSave = false,
        CancellationToken cancellationToken = default)
    {
        var position = new SqlServerCdcPosition
        {
            StartLsn = Convert.ToBase64String(startLsn),
            SeqVal = seqVal != null
                ? Convert.ToBase64String(seqVal)
                : null,
            Operation = operation,
        };

        await UpdatePositionAsync(position, forceSave, cancellationToken);
    }

    /// <summary>
    ///     Updates the checkpoint from LSN hex string values.
    /// </summary>
    /// <param name="startLsnHex">The start LSN as hex string (e.g., "0x0000123456789ABC").</param>
    /// <param name="seqValHex">The sequence value as hex string.</param>
    /// <param name="operation">The CDC operation type.</param>
    /// <param name="forceSave">Force immediate save.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task UpdateFromLsnHexAsync(
        string startLsnHex,
        string? seqValHex = null,
        int? operation = null,
        bool forceSave = false,
        CancellationToken cancellationToken = default)
    {
        var position = new SqlServerCdcPosition
        {
            StartLsnHex = startLsnHex,
            SeqValHex = seqValHex,
            Operation = operation,
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
    ///     Gets the LSN range for querying CDC changes from the current checkpoint.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A tuple with from LSN and to LSN values.</returns>
    public async Task<(string? FromLsn, string ToLsn)> GetLsnRangeAsync(CancellationToken cancellationToken = default)
    {
        var position = await LoadPositionAsync(cancellationToken);
        var fromLsn = position?.StartLsnHex;

        // Get the current max LSN from the database
        // This would typically be done via: sys.fn_cdc_get_max_lsn()
        var toLsn = "sys.fn_cdc_get_max_lsn()";

        return (fromLsn, toLsn);
    }

    /// <summary>
    ///     Serializes a CDC position to a string.
    /// </summary>
    private static string SerializePosition(SqlServerCdcPosition position)
    {
        return JsonSerializer.Serialize(position, JsonOptions);
    }

    /// <summary>
    ///     Deserializes a CDC position from a string.
    /// </summary>
    private static SqlServerCdcPosition? DeserializePosition(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        try
        {
            return JsonSerializer.Deserialize<SqlServerCdcPosition>(value, JsonOptions);
        }
        catch
        {
            return null;
        }
    }
}

/// <summary>
///     Represents a SQL Server CDC position (LSN-based).
/// </summary>
public sealed record SqlServerCdcPosition
{
    /// <summary>
    ///     Gets or sets the start LSN (Log Sequence Number) as base64.
    ///     This is the LSN of the commit for the transaction containing the change.
    /// </summary>
    public string? StartLsn { get; init; }

    /// <summary>
    ///     Gets or sets the start LSN as hex string (e.g., "0x0000123456789ABC").
    /// </summary>
    public string? StartLsnHex { get; init; }

    /// <summary>
    ///     Gets or sets the sequence value within the transaction as base64.
    ///     Used to order changes within a single transaction.
    /// </summary>
    public string? SeqVal { get; init; }

    /// <summary>
    ///     Gets or sets the sequence value as hex string.
    /// </summary>
    public string? SeqValHex { get; init; }

    /// <summary>
    ///     Gets or sets the CDC operation type.
    ///     1 = delete, 2 = insert, 3 = update (before), 4 = update (after).
    /// </summary>
    public int? Operation { get; init; }

    /// <summary>
    ///     Gets or sets the total number of changes processed.
    /// </summary>
    public long? ChangeCount { get; init; }

    /// <summary>
    ///     Gets or sets the timestamp of the last processed change.
    /// </summary>
    public DateTimeOffset? LastChangeTimestamp { get; init; }

    /// <summary>
    ///     Gets or sets the transaction ID.
    /// </summary>
    public Guid? TransactionId { get; init; }

    /// <summary>
    ///     Creates a position from a hex LSN string.
    /// </summary>
    /// <param name="lsnHex">The LSN as hex string.</param>
    /// <returns>A new CDC position.</returns>
    public static SqlServerCdcPosition FromLsnHex(string lsnHex)
    {
        return new SqlServerCdcPosition { StartLsnHex = lsnHex };
    }

    /// <summary>
    ///     Creates a position from a binary LSN.
    /// </summary>
    /// <param name="lsn">The LSN as byte array.</param>
    /// <returns>A new CDC position.</returns>
    public static SqlServerCdcPosition FromLsn(byte[] lsn)
    {
        return new SqlServerCdcPosition
        {
            StartLsn = Convert.ToBase64String(lsn),
            StartLsnHex = "0x" + Convert.ToHexString(lsn),
        };
    }

    /// <summary>
    ///     Parses the hex LSN to a byte array.
    /// </summary>
    /// <returns>The LSN as byte array, or null if parsing fails.</returns>
    public byte[]? ParseLsnAsBytes()
    {
        if (!string.IsNullOrEmpty(StartLsn))
            return Convert.FromBase64String(StartLsn);

        if (!string.IsNullOrEmpty(StartLsnHex))
        {
            var hex = StartLsnHex.StartsWith("0x", StringComparison.Ordinal)
                ? StartLsnHex[2..]
                : StartLsnHex;

            return Convert.FromHexString(hex);
        }

        return null;
    }
}
