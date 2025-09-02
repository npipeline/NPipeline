using NPipeline.Execution;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A transform node that wraps a delegate function for simple transformations.
/// </summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
internal sealed class DelegateTransformNode<TIn, TOut> : TransformNode<TIn, TOut>
{
    private readonly Func<TIn, PipelineContext, CancellationToken, Task<TOut>> _transformAsync;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DelegateTransformNode{TIn,TOut}" /> class.
    /// </summary>
    /// <param name="transformAsync">The asynchronous transformation function.</param>
    /// <param name="executionStrategy">An optional execution strategy. Defaults to sequential.</param>
    public DelegateTransformNode(
        Func<TIn, PipelineContext, CancellationToken, Task<TOut>> transformAsync,
        IExecutionStrategy? executionStrategy = null)
        : base(executionStrategy)
    {
        _transformAsync = transformAsync ?? throw new ArgumentNullException(nameof(transformAsync));
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="DelegateTransformNode{TIn,TOut}" /> class
    ///     with a synchronous transformation function.
    /// </summary>
    /// <param name="transform">The synchronous transformation function.</param>
    /// <param name="executionStrategy">An optional execution strategy. Defaults to sequential.</param>
    public DelegateTransformNode(
        Func<TIn, TOut> transform,
        IExecutionStrategy? executionStrategy = null)
        : base(executionStrategy)
    {
        ArgumentNullException.ThrowIfNull(transform);

        _transformAsync = (input, _, _) => Task.FromResult(transform(input));
    }

    /// <inheritdoc />
    public override Task<TOut> ExecuteAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        return _transformAsync(item, context, cancellationToken);
    }
}
