using NPipeline.Nodes;

namespace NPipeline.ErrorHandling;

/// <summary>
///     Marker interface for a node-level error handler.
/// </summary>
public interface INodeErrorHandler
{
}

/// <summary>
///     Defines the contract for handling errors that occur within a specific node.
/// </summary>
/// <typeparam name="TNode">The type of the node where the error occurred.</typeparam>
/// <typeparam name="TData">The type of the data item that failed.</typeparam>
/// <remarks>
///     <para>
///         Node error handlers allow you to implement custom error recovery strategies at the node level.
///         When a node fails to process an item, this handler is called with the failing item and a
///         <see cref="NodeFailureContext" /> containing the exception, pipeline context, failure attribution,
///         and retry attempt number. The handler returns a <see cref="NodeErrorDecision" /> instructing
///         the pipeline how to proceed.
///     </para>
///     <para>
///         Common error handling strategies include:
///         - <see cref="NodeErrorDecision.Retry" />: Attempt processing again (useful for transient failures)
///         - <see cref="NodeErrorDecision.Skip" />: Skip the failing item and continue (for non-critical items)
///         - <see cref="NodeErrorDecision.DeadLetter" />: Send to dead-letter sink for analysis
///         - <see cref="NodeErrorDecision.Fail" />: Stop the entire pipeline (for critical failures)
///     </para>
/// </remarks>
public interface INodeErrorHandler<in TNode, in TData> : INodeErrorHandler where TNode : INode
{
    /// <summary>
    ///     Handles an error that occurred during node execution.
    /// </summary>
    /// <param name="node">The instance of the node that failed.</param>
    /// <param name="failedItem">The data item that caused the error.</param>
    /// <param name="failure">Structured failure context containing exception, pipeline context, attribution, and retry attempt.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>A <see cref="NodeErrorDecision" /> indicating how to proceed.</returns>
    Task<NodeErrorDecision> HandleAsync(
        TNode node,
        TData failedItem,
        NodeFailureContext failure,
        CancellationToken cancellationToken);
}
