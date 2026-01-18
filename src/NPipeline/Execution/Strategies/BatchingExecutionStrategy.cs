using System.Runtime.CompilerServices;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Observability;
using NPipeline.Pipeline;

namespace NPipeline.Execution.Strategies;

/// <summary>
///     An execution strategy that batches items based on a specified size and time window.
/// </summary>
/// <remarks>
///     <para>
///         Batching collects individual items into groups before processing. This improves efficiency
///         for operations that benefit from processing multiple items together (e.g., bulk database inserts,
///         batch API calls, statistical computations).
///     </para>
///     <para>
///         Batches are emitted when either:
///         - The batch reaches the specified <c>batchSize</c>, OR
///         - The <c>timespan</c> elapses
///         This ensures timely processing even with low throughput.
///     </para>
///     <para>
///         Designed to work with <see cref="BatchingNode{T}" />, which outputs <see cref="IReadOnlyCollection{T}" />.
///         The framework will throw an exception if used with incompatible node types.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Create a batching node with batches of 100 items or 5-second timeout
/// var batchingNode = new BatchingNode&lt;Product&gt;(
///     batchSize: 100,
///     timespan: TimeSpan.FromSeconds(5));
/// 
/// // The batching execution strategy is configured internally
/// // Downstream nodes receive batches
/// builder.Connect(sourceNode, batchingNode);
/// builder.Connect(batchingNode, batchProcessingSink);
/// 
/// // Example bulk insert sink
/// public class BulkInsertSink : SinkNode&lt;IReadOnlyCollection&lt;Product&gt;&gt;
/// {
///     public override async Task ExecuteAsync(
///         IDataPipe&lt;IReadOnlyCollection&lt;Product&gt;&gt; input,
///         PipelineContext context,
///         CancellationToken cancellationToken)
///     {
///         await foreach (var batch in input.WithCancellation(cancellationToken))
///         {
///             // Insert batch of products efficiently
///             await InsertBatchAsync(batch, cancellationToken);
///         }
///     }
/// }
/// </code>
/// </example>
public sealed class BatchingExecutionStrategy : IExecutionStrategy, IStreamExecutionStrategy
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="BatchingExecutionStrategy" /> class.
    /// </summary>
    /// <param name="batchSize">The maximum number of items in a batch. Must be greater than zero.</param>
    /// <param name="timespan">The maximum time to wait before emitting a batch, even if not full.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when batchSize is not greater than zero.</exception>
    public BatchingExecutionStrategy(int batchSize, TimeSpan timespan)
    {
        if (batchSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(batchSize), "Batch size must be greater than zero.");

        BatchSize = batchSize;
        Timespan = timespan;
    }

    /// <summary>
    ///     Gets the maximum number of items in a batch.
    /// </summary>
    public int BatchSize { get; }

    /// <summary>
    ///     Gets the maximum time to wait before emitting a batch, even if not full.
    /// </summary>
    public TimeSpan Timespan { get; }

    /// <inheritdoc />
    /// <remarks>
    ///     This strategy transforms stream from individual items to collections. The output type
    ///     must be <see cref="IReadOnlyCollection{T}" /> where T is the input item type, or an
    ///     <see cref="InvalidOperationException" /> will be thrown.
    /// </remarks>
    public Task<IDataPipe<TOut>> ExecuteAsync<TIn, TOut>(
        IDataPipe<TIn> input,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // This strategy changes the shape of data from TIn to IReadOnlyCollection<TIn>.
        // The node using this strategy is expected to be an ITransformNode<TIn, IReadOnlyCollection<TIn>>.
        if (typeof(TOut) != typeof(IReadOnlyCollection<TIn>))
        {
            throw new InvalidOperationException(
                $"The {nameof(BatchingExecutionStrategy)} can only be used with nodes that output a collection. Expected output type: {typeof(IReadOnlyCollection<TIn>).Name}, but found {typeof(TOut).Name}.");
        }

        var nodeId = context.CurrentNodeId;

        // Get observability scope if available
        IAutoObservabilityScope? observabilityScope = null;

        if (context.Items.TryGetValue(PipelineContextKeys.NodeObservabilityScope(nodeId), out var scopeObj))
            observabilityScope = scopeObj as IAutoObservabilityScope;

        var batchedStream = BatchWithObservabilityAsync(input, BatchSize, Timespan, observabilityScope, cancellationToken);

        // The type system is a bit tricky here. We know TOut is IReadOnlyCollection<TIn>,
        // but we need to cast it to satisfy the compiler.
        var outputPipe = new StreamingDataPipe<TOut>((IAsyncEnumerable<TOut>)batchedStream);

        // Use Task.FromResult for already-completed synchronous result
        return Task.FromResult<IDataPipe<TOut>>(outputPipe);
    }

    /// <summary>
    ///     Executes the batching strategy for stream transform nodes.
    /// </summary>
    public Task<IDataPipe<TOut>> ExecuteAsync<TIn, TOut>(
        IDataPipe<TIn> input,
        IStreamTransformNode<TIn, TOut> node,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // This strategy changes the shape of data from TIn to IReadOnlyCollection<TIn>.
        // The node using this strategy is expected to be an IStreamTransformNode<TIn, IReadOnlyCollection<TIn>>.
        if (typeof(TOut) != typeof(IReadOnlyCollection<TIn>))
        {
            throw new InvalidOperationException(
                $"The {nameof(BatchingExecutionStrategy)} can only be used with nodes that output a collection. Expected output type: {typeof(IReadOnlyCollection<TIn>).Name}, but found {typeof(TOut).Name}.");
        }

        var nodeId = context.CurrentNodeId;

        // Get observability scope if available
        IAutoObservabilityScope? observabilityScope = null;

        if (context.Items.TryGetValue(PipelineContextKeys.NodeObservabilityScope(nodeId), out var scopeObj))
            observabilityScope = scopeObj as IAutoObservabilityScope;

        var batchedStream = BatchWithObservabilityAsync(input, BatchSize, Timespan, observabilityScope, cancellationToken);

        // The type system is a bit tricky here. We know TOut is IReadOnlyCollection<TIn>,
        // but we need to cast it to satisfy the compiler.
        var outputPipe = new StreamingDataPipe<TOut>((IAsyncEnumerable<TOut>)batchedStream);

        // Use Task.FromResult for already-completed synchronous result
        return Task.FromResult<IDataPipe<TOut>>(outputPipe);
    }

    /// <summary>
    ///     Batches items with observability support.
    /// </summary>
    private static async IAsyncEnumerable<IReadOnlyCollection<T>> BatchWithObservabilityAsync<T>(
        IDataPipe<T> input,
        int batchSize,
        TimeSpan timespan,
        IAutoObservabilityScope? observabilityScope,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        try
        {
            var batch = new List<T>(batchSize);
            var lastYieldTime = DateTime.UtcNow;

            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                // Track item processed
                observabilityScope?.IncrementProcessed();

                batch.Add(item);

                // Check if we should emit the batch
                if (batch.Count >= batchSize || DateTime.UtcNow - lastYieldTime >= timespan)
                {
                    // Track item emitted (one batch)
                    observabilityScope?.IncrementEmitted();
                    yield return batch;

                    batch = new List<T>(batchSize);
                    lastYieldTime = DateTime.UtcNow;
                }
            }

            // Emit any remaining items in the final batch
            if (batch.Count > 0)
            {
                // Track item emitted (one batch)
                observabilityScope?.IncrementEmitted();
                yield return batch;
            }
        }
        finally
        {
            // Dispose AutoObservabilityScope after all items are processed, even on failure or cancellation
            observabilityScope?.Dispose();
        }
    }
}
