using NPipeline.ErrorHandling;
using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Pipeline;

// <summary>
// Extensions enabling delegate-based transforms and generic error handler wiring.
// These restore the API surface expected by tests (delegate AddTransform overloads and generic WithErrorHandler).
// </summary>
public static class PipelineBuilderDelegateExtensions
{
    // Add a delegate-based transform (synchronous transform)
    public static TransformNodeHandle<TIn, TOut> AddTransform<TIn, TOut>(
        this PipelineBuilder builder,
        string name,
        Func<TIn, TOut> transform)
    {
        ArgumentNullException.ThrowIfNull(transform);

        var handle = builder.AddTransform<DelegateTransformNode<TIn, TOut>, TIn, TOut>(name);
        var node = new DelegateTransformNode<TIn, TOut>(transform);
        builder.RegisterBuilderDisposable(node);
        builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    // Add a delegate-based transform (asynchronous transform with context and CancellationToken)
    public static TransformNodeHandle<TIn, TOut> AddTransform<TIn, TOut>(
        this PipelineBuilder builder,
        string name,
        Func<TIn, PipelineContext, CancellationToken, Task<TOut>> transformAsync)
    {
        ArgumentNullException.ThrowIfNull(transformAsync);

        var handle = builder.AddTransform<DelegateTransformNode<TIn, TOut>, TIn, TOut>(name);
        var node = new DelegateTransformNode<TIn, TOut>(transformAsync);
        builder.RegisterBuilderDisposable(node);
        builder.AddPreconfiguredNodeInstance(handle.Id, node);
        return handle;
    }

    // Generic error handler wiring convenience, forwarding to non-generic WithErrorHandler(NodeHandle, Type)
    public static PipelineBuilder WithErrorHandler<THandler, TIn, TOut>(
        this PipelineBuilder builder,
        TransformNodeHandle<TIn, TOut> handle)
        where THandler : INodeErrorHandler<ITransformNode<TIn, TOut>, TIn>
    {
        return builder.WithErrorHandler(handle, typeof(THandler));
    }
}
