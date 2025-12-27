using NPipeline.ErrorHandling;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing;

/// <summary>
///     A pipeline-level error handler that captures exceptions for test assertions instead of throwing them.
/// </summary>
/// <remarks>
///     This handler is used by <see cref="PipelineTestHarness{TPipeline}" /> to collect errors
///     that occur during pipeline execution, allowing tests to verify error handling behavior
///     without having exceptions propagate out of the RunAsync call.
/// </remarks>
internal sealed class CapturingPipelineErrorHandler : IPipelineErrorHandler
{
    private readonly PipelineErrorDecision _decisionOnError;
    private readonly List<Exception> _errors;

    /// <summary>
    ///     Creates a capturing handler that records errors and returns the specified decision.
    /// </summary>
    /// <param name="errors">The list to record errors into.</param>
    /// <param name="decisionOnError">The decision to return when an error occurs. Defaults to ContinueWithoutNode.</param>
    public CapturingPipelineErrorHandler(List<Exception> errors, PipelineErrorDecision decisionOnError = PipelineErrorDecision.ContinueWithoutNode)
    {
        _errors = errors;
        _decisionOnError = decisionOnError;
    }

    public Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _errors.Add(error);
        return Task.FromResult(_decisionOnError);
    }
}

/// <summary>
///     An error handler that captures exceptions for test assertions instead of throwing them.
/// </summary>
/// <remarks>
///     This handler is used by <see cref="PipelineTestHarness{TPipeline}" /> to collect errors
///     that occur during pipeline execution, allowing tests to verify error handling behavior
///     without having exceptions propagate out of the RunAsync call.
/// </remarks>
internal sealed class CapturingErrorHandler : INodeErrorHandler
{
    private readonly List<Exception> _errors;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CapturingErrorHandler" /> class.
    /// </summary>
    /// <param name="errors">The list to record errors into for later assertion.</param>
    public CapturingErrorHandler(List<Exception> errors)
    {
        _errors = errors;
    }

    /// <summary>
    ///     Records the exception and always returns Skip to continue processing.
    /// </summary>
    public Task<NodeErrorDecision> HandleAsync<TNode, TData>(
        TNode node,
        TData failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
        where TNode : INode
    {
        _errors.Add(error);
        return Task.FromResult(NodeErrorDecision.Skip);
    }
}

/// <summary>
///     Generic capturing error handler for strongly-typed node error handling.
/// </summary>
/// <typeparam name="TNode">The node type that may fail.</typeparam>
/// <typeparam name="TData">The data type being processed.</typeparam>
internal sealed class CapturingErrorHandler<TNode, TData> : INodeErrorHandler<TNode, TData>
    where TNode : INode
{
    private readonly NodeErrorDecision _decisionOnError;
    private readonly List<Exception> _errors;

    /// <summary>
    ///     Creates a capturing handler that records errors and returns the specified decision.
    /// </summary>
    /// <param name="errors">The list to record errors into.</param>
    /// <param name="decisionOnError">The decision to return when an error occurs. Defaults to Skip.</param>
    public CapturingErrorHandler(List<Exception> errors, NodeErrorDecision decisionOnError = NodeErrorDecision.Skip)
    {
        _errors = errors;
        _decisionOnError = decisionOnError;
    }

    public Task<NodeErrorDecision> HandleAsync(
        TNode node,
        TData failedItem,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _errors.Add(error);
        return Task.FromResult(_decisionOnError);
    }
}

/// <summary>
///     A composite pipeline error handler that chains the original handler with a capturing handler.
/// </summary>
/// <remarks>
///     This handler allows the test harness to capture errors for assertions while still executing
///     the original pipeline error handler logic (logging, custom dead-letter routing, etc.).
///     The original handler executes first, then the capturing handler records the error.
/// </remarks>
internal sealed class CompositePipelineErrorHandler(
    IPipelineErrorHandler _originalHandler,
    CapturingPipelineErrorHandler _capturingHandler) : IPipelineErrorHandler
{
    public async Task<PipelineErrorDecision> HandleNodeFailureAsync(
        string nodeId,
        Exception error,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Execute original handler first to preserve its side effects (logging, custom routing, etc.)
        _ = await _originalHandler.HandleNodeFailureAsync(nodeId, error, context, cancellationToken);

        // Then capture the error for test assertions
        var capturingDecision = await _capturingHandler.HandleNodeFailureAsync(nodeId, error, context, cancellationToken);

        // Return the capturing decision since the test harness is controlling error flow
        return capturingDecision;
    }
}
