using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Execution;

/// <summary>
///     An execution strategy that can resume a transform node part-way through its input, which is what node restart
///     (<see cref="NodeRestartOptions" />) needs.
/// </summary>
/// <remarks>
///     <para>
///         A restart does not replay the whole input. It reopens the node at its checkpoint: the first input item
///         whose outcome has not been delivered yet. An item's outcome is delivered when its output has been handed
///         downstream, or when it was skipped or dead-lettered and so has no output. The strategy reports that point
///         through <see cref="RestartCheckpoint" /> as items are delivered; everything below it is released, and
///         everything from it onwards is held so that a restart can process it again.
///     </para>
///     <para>
///         A strategy that reports the checkpoint in input order delivers each output exactly once across restarts.
///         One that delivers out of order, and reports with <see cref="RestartCheckpoint.Complete" />, delivers at
///         least once: outputs delivered past the checkpoint are delivered again after a restart.
///     </para>
///     <para>
///         Node restart on a transform whose strategy does not implement this interface is a build error.
///     </para>
/// </remarks>
public interface IResumableExecutionStrategy : IExecutionStrategy
{
    /// <summary>
    ///     Executes the node over an input that starts part-way through the node's input.
    /// </summary>
    /// <typeparam name="TIn">The input item type.</typeparam>
    /// <typeparam name="TOut">The output item type.</typeparam>
    /// <param name="input">The input, starting at the item with index <paramref name="offset" />.</param>
    /// <param name="offset">The index, within the node's whole input, of the first item of <paramref name="input" />.</param>
    /// <param name="checkpoint">
    ///     Where to report delivered items. Indexes are positions in the node's whole input, so the first item of
    ///     <paramref name="input" /> is <paramref name="offset" />.
    /// </param>
    /// <param name="node">The transform node to execute.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="nodeId">The id of the node being executed.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The node's output from <paramref name="offset" /> onwards.</returns>
    Task<IDataStream<TOut>> ExecuteFromAsync<TIn, TOut>(
        IDataStream<TIn> input,
        long offset,
        RestartCheckpoint checkpoint,
        ITransformNode<TIn, TOut> node,
        PipelineContext context,
        string nodeId,
        CancellationToken cancellationToken);
}
