using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Execution;

/// <summary>
///     Defines the contract for an execution strategy that processes a stream of data through a stream transform node.
/// </summary>
public interface IStreamExecutionStrategy
{
    /// <summary>
    ///     Executes the transformation logic on an input data stream.
    /// </summary>
    /// <typeparam name="TIn">The input item type.</typeparam>
    /// <typeparam name="TOut">The output item type.</typeparam>
    /// <param name="input">The input data pipe.</param>
    /// <param name="node">The stream transform node to execute.</param>
    /// <param name="context">The pipeline context. Use context.Observability.Tracer to access tracing functionality.</param>
    /// <param name="nodeId">
    ///     The id of the node being executed. Passed explicitly rather than read from
    ///     <see cref="PipelineNodeEnvironmentContext.CurrentNodeId" />, which is a single field on a context shared by every node in
    ///     the run and is therefore only meaningful while nodes run one at a time.
    /// </param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>An output data pipe with the transformed items.</returns>
    Task<IDataStream<TOut>> ExecuteAsync<TIn, TOut>(
        IDataStream<TIn> input,
        IStreamTransformNode<TIn, TOut> node,
        PipelineContext context,
        string nodeId,
        CancellationToken cancellationToken);
}
