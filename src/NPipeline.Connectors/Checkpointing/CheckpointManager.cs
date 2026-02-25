using System.Diagnostics;
using NPipeline.Connectors.Configuration;

namespace NPipeline.Connectors.Checkpointing;

/// <summary>
///     Manages checkpoint operations with configurable intervals and automatic saving.
/// </summary>
public class CheckpointManager : IAsyncDisposable
{
    private readonly CheckpointIntervalConfiguration _intervalConfig;
    private readonly SemaphoreSlim _lock = new(1, 1);
    private readonly string _nodeId;
    private readonly string _pipelineId;
    private readonly ICheckpointStorage _storage;
    private bool _disposed;
    private DateTimeOffset _lastSaveTime;

    private long _rowsProcessed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CheckpointManager" /> class.
    /// </summary>
    /// <param name="storage">The checkpoint storage backend.</param>
    /// <param name="pipelineId">The pipeline identifier.</param>
    /// <param name="nodeId">The node identifier.</param>
    /// <param name="strategy">The checkpoint strategy.</param>
    /// <param name="intervalConfig">The checkpoint interval configuration.</param>
    public CheckpointManager(
        ICheckpointStorage storage,
        string pipelineId,
        string nodeId,
        CheckpointStrategy strategy,
        CheckpointIntervalConfiguration? intervalConfig = null)
    {
        ArgumentNullException.ThrowIfNull(storage);

        _storage = storage;
        _pipelineId = pipelineId;
        _nodeId = nodeId;
        Strategy = strategy;
        _intervalConfig = intervalConfig ?? new CheckpointIntervalConfiguration();
        _lastSaveTime = DateTimeOffset.UtcNow;
    }

    /// <summary>
    ///     Gets the current checkpoint value.
    /// </summary>
    public Checkpoint? CurrentCheckpoint { get; private set; }

    /// <summary>
    ///     Gets the checkpoint strategy being used.
    /// </summary>
    public CheckpointStrategy Strategy { get; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        GC.SuppressFinalize(this);

        // Save final checkpoint
        try
        {
            await SaveAsync();
        }
        catch (Exception ex)
        {
            // Log the exception but don't throw during disposal
            // This is important for debugging checkpoint save failures
            Debug.WriteLine(
                $"Warning: Failed to save checkpoint during disposal for pipeline '{_pipelineId}', node '{_nodeId}': {ex.Message}");
        }

        _lock.Dispose();
    }

    /// <summary>
    ///     Loads the checkpoint from storage.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The loaded checkpoint, or null if none exists.</returns>
    public async Task<Checkpoint?> LoadAsync(CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);

        try
        {
            CurrentCheckpoint = await _storage.LoadAsync(_pipelineId, _nodeId, cancellationToken);
            return CurrentCheckpoint;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    ///     Updates the checkpoint with a new value.
    ///     Automatically saves if interval thresholds are met.
    /// </summary>
    /// <param name="value">The new checkpoint value.</param>
    /// <param name="metadata">Optional metadata.</param>
    /// <param name="forceSave">Force immediate save regardless of interval.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task UpdateAsync(
        string value,
        Dictionary<string, string>? metadata = null,
        bool forceSave = false,
        CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);

        try
        {
            CurrentCheckpoint = new Checkpoint(value, DateTimeOffset.UtcNow, metadata);
            _rowsProcessed++;

            if (forceSave || ShouldSaveCheckpoint())
                await SaveInternalAsync(cancellationToken);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    ///     Updates the checkpoint with a numeric offset.
    /// </summary>
    /// <param name="offset">The offset value.</param>
    /// <param name="metadata">Optional metadata.</param>
    /// <param name="forceSave">Force immediate save.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task UpdateOffsetAsync(
        long offset,
        Dictionary<string, string>? metadata = null,
        bool forceSave = false,
        CancellationToken cancellationToken = default)
    {
        await UpdateAsync(offset.ToString(), metadata, forceSave, cancellationToken);
    }

    /// <summary>
    ///     Forces an immediate save of the current checkpoint.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task SaveAsync(CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);

        try
        {
            if (CurrentCheckpoint != null)
                await SaveInternalAsync(cancellationToken);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    ///     Clears the checkpoint from storage.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    public async Task ClearAsync(CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);

        try
        {
            await _storage.DeleteAsync(_pipelineId, _nodeId, cancellationToken);
            CurrentCheckpoint = null;
            _rowsProcessed = 0;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    ///     Gets the current offset value, if applicable.
    /// </summary>
    /// <returns>The offset value, or null if not an offset checkpoint.</returns>
    public long? GetCurrentOffset()
    {
        return CurrentCheckpoint?.GetAsOffset();
    }

    /// <summary>
    ///     Determines if a checkpoint should be saved based on interval configuration.
    /// </summary>
    private bool ShouldSaveCheckpoint()
    {
        // Check row count interval
        if (_intervalConfig.RowCountInterval > 0 && _rowsProcessed % _intervalConfig.RowCountInterval == 0)
            return true;

        // Check time interval
        if (_intervalConfig.TimeInterval > TimeSpan.Zero)
        {
            var timeSinceLastSave = DateTimeOffset.UtcNow - _lastSaveTime;

            if (timeSinceLastSave >= _intervalConfig.TimeInterval)
                return true;
        }

        return false;
    }

    /// <summary>
    ///     Internal save method (must be called within lock).
    /// </summary>
    private async Task SaveInternalAsync(CancellationToken cancellationToken)
    {
        if (CurrentCheckpoint == null)
            return;

        await _storage.SaveAsync(_pipelineId, _nodeId, CurrentCheckpoint, cancellationToken);
        _lastSaveTime = DateTimeOffset.UtcNow;
    }
}

/// <summary>
///     Configuration for checkpoint save intervals.
/// </summary>
public class CheckpointIntervalConfiguration
{
    /// <summary>
    ///     Gets or sets the number of rows to process between checkpoint saves.
    ///     Set to 0 to disable row-based interval saving.
    /// </summary>
    public int RowCountInterval { get; set; } = 100;

    /// <summary>
    ///     Gets or sets the time interval between checkpoint saves.
    ///     Set to TimeSpan.Zero to disable time-based interval saving.
    /// </summary>
    public TimeSpan TimeInterval { get; set; } = TimeSpan.FromSeconds(10);
}
