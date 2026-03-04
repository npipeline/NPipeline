using System.Text.Json;
using NPipeline.Connectors.Checkpointing;
using NPipeline.Connectors.MySql.Configuration;

namespace NPipeline.Connectors.MySql.Checkpointing;

/// <summary>
///     Handler for MySQL binlog/GTID CDC checkpointing.
///     Tracks the replication position for change data capture.
/// </summary>
public class MySqlCdcCheckpointHandler
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    private readonly CheckpointManager _checkpointManager;
    private readonly MySqlConfiguration? _configuration;

    /// <summary>
    ///     Initialises a new <see cref="MySqlCdcCheckpointHandler" />.
    /// </summary>
    /// <param name="checkpointManager">The checkpoint manager.</param>
    /// <param name="configuration">Optional MySQL configuration controlling CDC mode preferences.</param>
    public MySqlCdcCheckpointHandler(
        CheckpointManager checkpointManager,
        MySqlConfiguration? configuration = null)
    {
        _checkpointManager = checkpointManager
            ?? throw new ArgumentNullException(nameof(checkpointManager));
        _configuration = configuration;
    }

    /// <summary>
    ///     Loads the current <see cref="BinlogPosition" /> from the checkpoint store.
    /// </summary>
    public async Task<BinlogPosition?> LoadPositionAsync(
        CancellationToken cancellationToken = default)
    {
        var checkpoint = await _checkpointManager.LoadAsync(cancellationToken).ConfigureAwait(false);

        return checkpoint is null ? null : DeserializePosition(checkpoint.Value);
    }

    /// <summary>
    ///     Persists a new <see cref="BinlogPosition" /> to the checkpoint store.
    /// </summary>
    /// <param name="position">The position to save.</param>
    /// <param name="forceSave">Force immediate flush to storage.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task UpdatePositionAsync(
        BinlogPosition position,
        bool forceSave = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(position);

        var serialized = SerializePosition(position);

        var metadata = new Dictionary<string, string>
        {
            ["mode"] = UseGtid(position) ? "gtid" : "binlog",
            ["updated_at"] = DateTimeOffset.UtcNow.ToString("O"),
        };

        if (position.EventCount.HasValue)
            metadata["event_count"] = position.EventCount.Value.ToString();

        await _checkpointManager
            .UpdateAsync(serialized, metadata, forceSave, cancellationToken)
            .ConfigureAwait(false);
    }

    /// <summary>
    ///     Updates the position from a file/offset pair.
    /// </summary>
    public async Task UpdateFromFileOffsetAsync(
        string fileName,
        long offset,
        bool forceSave = false,
        CancellationToken cancellationToken = default)
    {
        var position = BinlogPosition.FromFileOffset(fileName, offset);
        await UpdatePositionAsync(position, forceSave, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Updates the position from a GTID set string.
    /// </summary>
    public async Task UpdateFromGtidAsync(
        string gtidSet,
        bool forceSave = false,
        CancellationToken cancellationToken = default)
    {
        var position = BinlogPosition.FromGtid(gtidSet);
        await UpdatePositionAsync(position, forceSave, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Flushes the in-memory checkpoint to storage.
    /// </summary>
    public Task SaveAsync(CancellationToken cancellationToken = default) =>
        _checkpointManager.SaveAsync(cancellationToken);

    /// <summary>
    ///     Clears the checkpoint.
    /// </summary>
    public Task ClearAsync(CancellationToken cancellationToken = default) =>
        _checkpointManager.ClearAsync(cancellationToken);

    // -------------------------------------------------------------------------

    private bool UseGtid(BinlogPosition position) =>
        position.GtidSet is not null
        || _configuration?.CdcMode == CdcMode.Gtid
        || (_configuration?.PreferGtidWhenAvailable == true && position.GtidSet is not null);

    private static string SerializePosition(BinlogPosition position) =>
        JsonSerializer.Serialize(position, JsonOptions);

    private static BinlogPosition? DeserializePosition(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        try
        {
            return JsonSerializer.Deserialize<BinlogPosition>(value, JsonOptions);
        }
        catch
        {
            return null;
        }
    }
}
