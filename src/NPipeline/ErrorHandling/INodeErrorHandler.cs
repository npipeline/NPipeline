using NPipeline.Nodes;
using NPipeline.Pipeline;

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
///         When a node fails to process an item, this handler is called with the failing item and exception.
///         The handler returns a <see cref="NodeErrorDecision" /> instructing the pipeline how to proceed.
///     </para>
///     <para>
///         Common error handling strategies include:
///         - <see cref="NodeErrorDecision.Retry" />: Attempt processing again (useful for transient failures)
///         - <see cref="NodeErrorDecision.Skip" />: Skip the failing item and continue (for non-critical items)
///         - <see cref="NodeErrorDecision.DeadLetter" />: Send to dead-letter sink for analysis
///         - <see cref="NodeErrorDecision.Fail" />: Stop the entire pipeline (for critical failures)
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Implement a retry handler that retries up to 3 times
/// public class RetryErrorHandler : INodeErrorHandler&lt;MyTransformNode, string&gt;
/// {
///     private int _retryCount = 0;
///     private const int MaxRetries = 3;
/// 
///     public async Task&lt;NodeErrorDecision&gt; HandleAsync(
///         MyTransformNode node,
///         string failedItem,
///         Exception error,
///         PipelineContext context,
///         CancellationToken cancellationToken)
///     {
///         if (_retryCount &lt; MaxRetries &amp;&amp; error is TransientException)
///         {
///             _retryCount++;
///             return NodeErrorDecision.Retry;
///         }
/// 
///         if (error is NonCriticalValidationException)
///         {
///             return NodeErrorDecision.Skip;
///         }
/// 
///         return NodeErrorDecision.DeadLetter;
///     }
/// }
/// 
/// // Apply the handler to a node
/// var node = new MyTransformNode();
/// node.ErrorHandler = new RetryErrorHandler();
/// </code>
/// </example>
public interface INodeErrorHandler<in TNode, in TData> : INodeErrorHandler where TNode : INode
{
    /// <summary>
    ///     Handles an error that occurred during node execution.
    /// </summary>
    /// <param name="node">The instance of the node that failed.</param>
    /// <param name="failedItem">The data item that caused the error.</param>
    /// <param name="error">The exception that was thrown.</param>
    /// <param name="context">The current pipeline context.</param>
    /// <param name="cancellationToken">A token to observe for cancellation requests.</param>
    /// <returns>A <see cref="NodeErrorDecision" /> indicating how to proceed.</returns>
    /// <remarks>
    ///     This method is called asynchronously when a node fails on a specific data item.
    ///     You have access to the node instance, the failing item, the exception, and the pipeline context.
    ///     Your decision will directly impact how the pipeline continues, so consider both the error type
    ///     and the business criticality of the failing item.
    /// </remarks>
    Task<NodeErrorDecision> HandleAsync(
        TNode node,
        TData failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken);
}
