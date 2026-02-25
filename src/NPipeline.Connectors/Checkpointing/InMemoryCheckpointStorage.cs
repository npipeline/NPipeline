namespace NPipeline.Connectors.Checkpointing;

/// <summary>
///     In-memory checkpoint storage implementation.
///     Checkpoints are stored in memory and lost on process restart.
///     Suitable for transient failure recovery during pipeline execution.
/// </summary>
public class InMemoryCheckpointStorage : ICheckpointStorage, IDisposable
{
    private readonly Dictionary<string, Checkpoint> _checkpoints = new();
    private readonly SemaphoreSlim _lock = new(1, 1);
    private bool _disposed;

    /// <inheritdoc />
    public async Task<Checkpoint?> LoadAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default)
    {
        var key = GetKey(pipelineId, nodeId);

        await _lock.WaitAsync(cancellationToken);
        try
        {
            return _checkpoints.TryGetValue(key, out var checkpoint) ? checkpoint : null;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc />
    public async Task SaveAsync(string pipelineId, string nodeId, Checkpoint checkpoint, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(checkpoint);

        var key = GetKey(pipelineId, nodeId);

        await _lock.WaitAsync(cancellationToken);
        try
        {
            _checkpoints[key] = checkpoint;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc />
    public async Task DeleteAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default)
    {
        var key = GetKey(pipelineId, nodeId);

        await _lock.WaitAsync(cancellationToken);
        try
        {
            _checkpoints.Remove(key);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <inheritdoc />
    public async Task<bool> ExistsAsync(string pipelineId, string nodeId, CancellationToken cancellationToken = default)
    {
        var key = GetKey(pipelineId, nodeId);

        await _lock.WaitAsync(cancellationToken);
        try
        {
            return _checkpoints.ContainsKey(key);
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    ///     Clears all checkpoints from memory.
    /// </summary>
    public async Task ClearAllAsync(CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);
        try
        {
            _checkpoints.Clear();
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    ///     Gets the count of stored checkpoints.
    /// </summary>
    public async Task<int> GetCountAsync(CancellationToken cancellationToken = default)
    {
        await _lock.WaitAsync(cancellationToken);
        try
        {
            return _checkpoints.Count;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    ///     Disposes resources used by the storage.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        _lock.Dispose();
        GC.SuppressFinalize(this);
    }

    private static string GetKey(string pipelineId, string nodeId)
    {
        return $"{pipelineId}:{nodeId}";
    }
}
