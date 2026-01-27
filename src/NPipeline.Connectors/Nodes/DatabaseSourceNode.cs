using NPipeline.Connectors.Abstractions;
using NPipeline.Connectors.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;
using NPipeline.Nodes;

namespace NPipeline.Connectors.Nodes;

/// <summary>
/// Base class for database source nodes.
/// Designed to be inherited by database-specific implementations.
/// </summary>
/// <typeparam name="TReader">The type of database reader.</typeparam>
/// <typeparam name="T">The type of objects emitted by source.</typeparam>
public abstract class DatabaseSourceNode<TReader, T> : SourceNode<T>
    where TReader : IDatabaseReader
{
    private static readonly Dictionary<string, long> _checkpoints = new();

    /// <summary>
    /// Gets whether to stream results.
    /// Virtual property for extensions.
    /// </summary>
    protected virtual bool StreamResults => false;

    /// <summary>
    /// Gets fetch size for streaming.
    /// Virtual property for extensions.
    /// </summary>
    protected virtual int FetchSize => 100;

    /// <summary>
    /// Gets delivery semantic.
    /// Virtual property for extensions.
    /// </summary>
    protected virtual DeliverySemantic DeliverySemantic => DeliverySemantic.AtLeastOnce;

    /// <summary>
    /// Gets checkpoint strategy.
    /// Virtual property for extensions.
    /// </summary>
    protected virtual CheckpointStrategy CheckpointStrategy => CheckpointStrategy.None;

    /// <summary>
    /// Gets a unique identifier for this source node instance for checkpoint tracking.
    /// Virtual property for extensions.
    /// </summary>
    protected virtual string CheckpointId => GetType().FullName ?? GetType().Name;

    /// <summary>
    /// Gets a database connection asynchronously.
    /// Abstract method to be implemented by derived classes.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected abstract Task<IDatabaseConnection> GetConnectionAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Executes query and returns a database reader.
    /// Abstract method to be implemented by derived classes.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected abstract Task<TReader> ExecuteQueryAsync(IDatabaseConnection connection, CancellationToken cancellationToken);

    /// <summary>
    /// Maps a database row to an object.
    /// Abstract method to be implemented by derived classes.
    /// </summary>
    /// <param name="reader">The database reader.</param>
    /// <returns>The mapped object.</returns>
    protected abstract T MapRow(TReader reader);

    /// <summary>
    /// Initializes the source node and returns a data pipe.
    /// </summary>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A data pipe containing the data.</returns>
    public override IDataPipe<T> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        if (StreamResults)
        {
            // Create a streaming data pipe with an async enumerable
            var stream = StreamDataAsync(cancellationToken);
            return new StreamingDataPipe<T>(stream, $"{GetType().Name}");
        }
        else
        {
            // Buffer all data in memory
            var items = BufferDataAsync(cancellationToken).GetAwaiter().GetResult();
            return new InMemoryDataPipe<T>(items, $"{GetType().Name}");
        }
    }

    /// <summary>
    /// Streams data from the database asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An async enumerable of data items.</returns>
    private async IAsyncEnumerable<T> StreamDataAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var connection = await GetConnectionAsync(cancellationToken);
        await using var reader = await ExecuteQueryAsync(connection, cancellationToken);

        var checkpointId = CheckpointId;
        var checkpoint = CheckpointStrategy == CheckpointStrategy.InMemory ? GetCheckpoint(checkpointId) : 0;
        var currentRow = 0L;

        // Skip to checkpoint position
        while (checkpoint > 0 && await reader.ReadAsync(cancellationToken))
        {
            currentRow++;
            if (currentRow >= checkpoint)
            {
                break;
            }
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            currentRow++;
            yield return MapRow(reader);

            // Update checkpoint
            if (CheckpointStrategy == CheckpointStrategy.InMemory)
            {
                SetCheckpoint(checkpointId, currentRow);
            }
        }

        // Clear checkpoint on successful completion
        if (CheckpointStrategy == CheckpointStrategy.InMemory)
        {
            ClearCheckpoint(checkpointId);
        }
    }

    /// <summary>
    /// Buffers all data from the database into memory.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A list of data items.</returns>
    private async Task<List<T>> BufferDataAsync(CancellationToken cancellationToken)
    {
        var items = new List<T>();

        await using var connection = await GetConnectionAsync(cancellationToken);
        await using var reader = await ExecuteQueryAsync(connection, cancellationToken);

        var checkpointId = CheckpointId;
        var checkpoint = CheckpointStrategy == CheckpointStrategy.InMemory ? GetCheckpoint(checkpointId) : 0;
        var currentRow = 0L;

        // Skip to checkpoint position
        while (checkpoint > 0 && await reader.ReadAsync(cancellationToken))
        {
            currentRow++;
            if (currentRow >= checkpoint)
            {
                break;
            }
        }

        while (await reader.ReadAsync(cancellationToken))
        {
            currentRow++;
            items.Add(MapRow(reader));

            // Update checkpoint periodically
            if (CheckpointStrategy == CheckpointStrategy.InMemory && currentRow % 100 == 0)
            {
                SetCheckpoint(checkpointId, currentRow);
            }
        }

        // Clear checkpoint on successful completion
        if (CheckpointStrategy == CheckpointStrategy.InMemory)
        {
            ClearCheckpoint(checkpointId);
        }

        return items;
    }

    /// <summary>
    /// Gets the checkpoint value for the specified checkpoint ID.
    /// </summary>
    /// <param name="checkpointId">The checkpoint identifier.</param>
    /// <returns>The checkpoint value (row number).</returns>
    private static long GetCheckpoint(string checkpointId)
    {
        lock (_checkpoints)
        {
            return _checkpoints.TryGetValue(checkpointId, out var checkpoint) ? checkpoint : 0;
        }
    }

    /// <summary>
    /// Sets the checkpoint value for the specified checkpoint ID.
    /// </summary>
    /// <param name="checkpointId">The checkpoint identifier.</param>
    /// <param name="value">The checkpoint value (row number).</param>
    private static void SetCheckpoint(string checkpointId, long value)
    {
        lock (_checkpoints)
        {
            _checkpoints[checkpointId] = value;
        }
    }

    /// <summary>
    /// Clears the checkpoint for the specified checkpoint ID.
    /// </summary>
    /// <param name="checkpointId">The checkpoint identifier.</param>
    private static void ClearCheckpoint(string checkpointId)
    {
        lock (_checkpoints)
        {
            _checkpoints.Remove(checkpointId);
        }
    }
}
