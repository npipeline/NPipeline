using NPipeline.Connectors.Configuration;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Abstractions;

namespace NPipeline.Connectors.Nodes;

/// <summary>
///     Base class for database sink nodes.
///     Designed to be inherited by database-specific implementations.
/// </summary>
/// <typeparam name="T">The type of objects consumed by sink.</typeparam>
public abstract class DatabaseSinkNode<T> : SinkNode<T>
{
    /// <summary>
    ///     Gets whether to use transactions.
    ///     Virtual property for extensions.
    /// </summary>
    protected virtual bool UseTransaction => false;

    /// <summary>
    ///     Gets batch size for batch writes.
    ///     Virtual property for extensions.
    /// </summary>
    protected virtual int BatchSize => 100;

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
    ///     Gets whether to continue on error.
    ///     Virtual property for extensions.
    /// </summary>
    protected virtual bool ContinueOnError => false;

    /// <summary>
    ///     Gets a database connection asynchronously.
    ///     Abstract method to be implemented by derived classes.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected abstract Task<IDatabaseConnection> GetConnectionAsync(CancellationToken cancellationToken);

    /// <summary>
    ///     Creates a database writer for the connection.
    ///     Abstract method to be implemented by derived classes.
    /// </summary>
    /// <param name="connection">The database connection.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    protected abstract Task<IDatabaseWriter<T>> CreateWriterAsync(IDatabaseConnection connection, CancellationToken cancellationToken);

    /// <summary>
    ///     Executes sink node, writing all items to database.
    /// </summary>
    /// <param name="input">The input data pipe.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    public override async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await using var connection = await GetConnectionAsync(cancellationToken);
        await using var writer = await CreateWriterAsync(connection, cancellationToken);

        var batch = new List<T>(BatchSize);

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            batch.Add(item);

            if (batch.Count >= BatchSize)
            {
                await WriteBatchAsync(writer, batch, cancellationToken);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
            await WriteBatchAsync(writer, batch, cancellationToken);

        await writer.FlushAsync(cancellationToken);
    }

    /// <summary>
    ///     Writes a batch of items to database.
    /// </summary>
    /// <param name="writer">The database writer.</param>
    /// <param name="batch">The batch of items to write.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private async Task WriteBatchAsync(IDatabaseWriter<T> writer, List<T> batch, CancellationToken cancellationToken)
    {
        try
        {
            await writer.WriteBatchAsync(batch, cancellationToken);
        }
        catch (Exception) when (ContinueOnError)
        {
            // Log error but continue
        }
    }
}
