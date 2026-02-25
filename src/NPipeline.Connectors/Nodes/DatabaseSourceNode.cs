using System.Runtime.CompilerServices;
using NPipeline.Connectors.Checkpointing;
using NPipeline.Connectors.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Nodes;

/// <summary>
///     Base class for database source nodes.
///     Designed to be inherited by database-specific implementations.
/// </summary>
/// <typeparam name="TReader">The type of database reader.</typeparam>
/// <typeparam name="T">The type of objects emitted by source.</typeparam>
public abstract class DatabaseSourceNode<TReader, T> : SourceNode<T>, IAsyncDisposable
    where TReader : IDatabaseReader
{
    private CheckpointManager? _checkpointManager;
    private InMemoryCheckpointStorage? _inMemoryStorage;

    /// <summary>
    ///     Gets whether to stream results.
    ///     Virtual property for extensions.
    /// </summary>
    protected virtual bool StreamResults => false;

    /// <summary>
    ///     Gets fetch size for streaming.
    ///     Virtual property for extensions.
    /// </summary>
    protected virtual int FetchSize => 100;

    /// <summary>
    ///     Gets delivery semantic.
    ///     Virtual property for extensions.
    /// </summary>
    protected virtual DeliverySemantic DeliverySemantic => DeliverySemantic.AtLeastOnce;

    /// <summary>
    ///     Gets checkpoint strategy.
    ///     Virtual property for extensions.
    /// </summary>
    protected virtual CheckpointStrategy CheckpointStrategy => CheckpointStrategy.None;

    /// <summary>
    ///     Gets a unique identifier for this source node instance for checkpoint tracking.
    ///     Virtual property for extensions.
    /// </summary>
    protected virtual string CheckpointId => GetType().FullName ?? GetType().Name;

    /// <summary>
    ///     Gets the pipeline identifier for checkpoint namespacing.
    ///     Virtual property for extensions.
    /// </summary>
    protected virtual string PipelineId => "default";

    /// <summary>
    ///     Gets the checkpoint storage backend.
    ///     Virtual property for extensions.
    /// </summary>
    protected virtual ICheckpointStorage? CheckpointStorage => null;

    /// <summary>
    ///     Gets the checkpoint interval configuration.
    ///     Virtual property for extensions.
    /// </summary>
    protected virtual CheckpointIntervalConfiguration CheckpointInterval => new();

    /// <summary>
    ///     Gets the offset column for offset-based checkpointing.
    /// </summary>
    protected virtual string? CheckpointOffsetColumn => null;

    /// <summary>
    ///     Gets the key columns for key-based checkpointing.
    /// </summary>
    protected virtual string[]? CheckpointKeyColumns => null;

    /// <summary>
    ///     Disposes resources used by the source node.
    /// </summary>
    public override async ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);

        if (_checkpointManager != null)
            await _checkpointManager.DisposeAsync();

        _inMemoryStorage?.Dispose();

        await base.DisposeAsync();
    }

    /// <summary>
    ///     Gets a database connection asynchronously.
    ///     Abstract method to be implemented by derived classes.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected abstract Task<IDatabaseConnection> GetConnectionAsync(CancellationToken cancellationToken);

    /// <summary>
    ///     Executes query and returns a database reader.
    ///     Abstract method to be implemented by derived classes.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected abstract Task<TReader> ExecuteQueryAsync(IDatabaseConnection connection, CancellationToken cancellationToken);

    /// <summary>
    ///     Maps a database row to an object.
    ///     Abstract method to be implemented by derived classes.
    /// </summary>
    /// <param name="reader">The database reader.</param>
    /// <returns>The mapped object.</returns>
    protected abstract T MapRow(TReader reader);

    /// <summary>
    ///     Attempts to map a database row to an object.
    ///     Override to skip rows on errors or apply custom row-level handling.
    /// </summary>
    /// <param name="reader">The database reader.</param>
    /// <param name="item">The mapped item.</param>
    /// <returns>True when the row should be emitted; otherwise false to skip.</returns>
    protected virtual bool TryMapRow(TReader reader, out T item)
    {
        item = MapRow(reader);
        return true;
    }

    /// <summary>
    ///     Gets the current offset value from the checkpoint.
    ///     Override to provide offset values from specific columns.
    /// </summary>
    /// <param name="reader">The database reader.</param>
    /// <returns>The offset value, or null if not applicable.</returns>
    protected virtual long? GetCurrentOffset(TReader reader)
    {
        return null;
    }

    /// <summary>
    ///     Gets the current key values from the row for key-based checkpointing.
    /// </summary>
    /// <param name="reader">The database reader.</param>
    /// <returns>The key values dictionary, or null if not applicable.</returns>
    protected virtual Dictionary<string, object?>? GetCurrentKeyValues(TReader reader)
    {
        return null;
    }

    /// <summary>
    ///     Initializes the source node and returns a data pipe.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A data pipe containing the data.</returns>
    public override IDataPipe<T> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        // Initialize checkpoint manager
        InitializeCheckpointManager(context);

        if (StreamResults)
        {
            // Create a streaming data pipe with an async enumerable
            var stream = StreamDataAsync(cancellationToken);
            return new StreamingDataPipe<T>(stream, $"{GetType().Name}");
        }

        // Buffer all data in memory
        // Use Thread Pool to avoid potential deadlocks in synchronization contexts
        // ConfigureAwait(false) prevents capturing the synchronization context
        var items = Task.Run(
            () => BufferDataAsync(cancellationToken).ConfigureAwait(false).GetAwaiter().GetResult(),
            cancellationToken).GetAwaiter().GetResult();

        return new InMemoryDataPipe<T>(items, $"{GetType().Name}");
    }

    /// <summary>
    ///     Initializes the checkpoint manager based on configuration.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    protected virtual void InitializeCheckpointManager(PipelineContext context)
    {
        if (CheckpointStrategy == CheckpointStrategy.None)
            return;

        var storage = ResolveCheckpointStorage();
        var pipelineId = context.CurrentNodeId ?? PipelineId;

        _checkpointManager = new CheckpointManager(
            storage,
            pipelineId,
            CheckpointId,
            CheckpointStrategy,
            CheckpointInterval);
    }

    /// <summary>
    ///     Resolves the checkpoint storage backend based on configuration.
    /// </summary>
    /// <returns>The checkpoint storage implementation.</returns>
    protected virtual ICheckpointStorage ResolveCheckpointStorage()
    {
        // If storage is explicitly provided, use it
        if (CheckpointStorage != null)
            return CheckpointStorage;

        // For InMemory strategy, use shared in-memory storage
        if (CheckpointStrategy == CheckpointStrategy.InMemory)
        {
            _inMemoryStorage ??= new InMemoryCheckpointStorage();
            return _inMemoryStorage;
        }

        throw new InvalidOperationException(
            $"CheckpointStorage must be provided when CheckpointStrategy is {CheckpointStrategy}. " +
            "Set CheckpointStorage or use InMemory strategy.");
    }

    /// <summary>
    ///     Streams data from the database asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An async enumerable of data items.</returns>
    private async IAsyncEnumerable<T> StreamDataAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var reader = await ExecuteQueryAsync(connection, cancellationToken).ConfigureAwait(false);

        // Load checkpoint if available
        long startOffset = 0;

        if (_checkpointManager != null)
        {
            var checkpoint = await _checkpointManager.LoadAsync(cancellationToken).ConfigureAwait(false);
            startOffset = checkpoint?.GetAsOffset() ?? 0;
        }

        var currentRow = 0L;

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            currentRow++;

            // Skip rows that were already processed (for offset/cursor-based checkpointing)
            if (currentRow <= startOffset)
                continue;

            if (TryMapRow(reader, out var item))
                yield return item;

            // Update checkpoint based on strategy
            await UpdateCheckpointAsync(reader, currentRow, false, cancellationToken).ConfigureAwait(false);
        }

        // Save final checkpoint and clear if successful
        if (_checkpointManager != null)
            await _checkpointManager.SaveAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    ///     Buffers all data from the database into memory.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A list of data items.</returns>
    private async Task<List<T>> BufferDataAsync(CancellationToken cancellationToken)
    {
        var items = new List<T>();

        await using var connection = await GetConnectionAsync(cancellationToken).ConfigureAwait(false);
        await using var reader = await ExecuteQueryAsync(connection, cancellationToken).ConfigureAwait(false);

        // Load checkpoint if available
        long startOffset = 0;

        if (_checkpointManager != null)
        {
            var checkpoint = await _checkpointManager.LoadAsync(cancellationToken).ConfigureAwait(false);
            startOffset = checkpoint?.GetAsOffset() ?? 0;
        }

        var currentRow = 0L;

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            currentRow++;

            // Skip rows that were already processed (for offset/cursor-based checkpointing)
            if (currentRow <= startOffset)
                continue;

            if (TryMapRow(reader, out var item))
                items.Add(item);

            // Update checkpoint periodically
            await UpdateCheckpointAsync(reader, currentRow, false, cancellationToken).ConfigureAwait(false);
        }

        // Save final checkpoint
        if (_checkpointManager != null)
            await _checkpointManager.SaveAsync(cancellationToken).ConfigureAwait(false);

        return items;
    }

    /// <summary>
    ///     Updates the checkpoint based on the current strategy.
    /// </summary>
    /// <param name="reader">The database reader.</param>
    /// <param name="currentRow">The current row number.</param>
    /// <param name="forceSave">Force immediate save.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    protected virtual async Task UpdateCheckpointAsync(
        TReader reader,
        long currentRow,
        bool forceSave,
        CancellationToken cancellationToken)
    {
        if (_checkpointManager == null)
            return;

        switch (CheckpointStrategy)
        {
            case CheckpointStrategy.InMemory:
            case CheckpointStrategy.Offset:
            {
                var offset = GetCurrentOffset(reader) ?? currentRow;
                await _checkpointManager.UpdateOffsetAsync(offset, null, forceSave, cancellationToken);
                break;
            }

            case CheckpointStrategy.KeyBased:
            {
                var keyValues = GetCurrentKeyValues(reader);

                if (keyValues != null)
                {
                    var serialized = string.Join("|", keyValues.Select(kv => $"{kv.Key}={kv.Value}"));
                    await _checkpointManager.UpdateAsync(serialized, null, forceSave, cancellationToken);
                }

                break;
            }

            case CheckpointStrategy.Cursor:
            {
                await _checkpointManager.UpdateOffsetAsync(currentRow, null, forceSave, cancellationToken);
                break;
            }

            case CheckpointStrategy.CDC:
                // CDC checkpointing is handled by specific implementations
                break;

            case CheckpointStrategy.None:
            default:
                // No checkpointing
                break;
        }
    }

    /// <summary>
    ///     Gets the current checkpoint offset value.
    /// </summary>
    /// <returns>The offset value, or 0 if no checkpoint exists.</returns>
    protected async Task<long> GetCheckpointOffsetAsync(CancellationToken cancellationToken = default)
    {
        if (_checkpointManager == null)
            return 0;

        var checkpoint = await _checkpointManager.LoadAsync(cancellationToken);
        return checkpoint?.GetAsOffset() ?? 0;
    }
}
