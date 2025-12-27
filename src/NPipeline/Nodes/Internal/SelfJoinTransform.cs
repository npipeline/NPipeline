using System.Linq.Expressions;
using System.Reflection;
using NPipeline.Pipeline;

namespace NPipeline.Nodes.Internal;

/// <summary>
///     Internal transform node that wraps items for self-join scenarios.
///     Uses reflection to create a factory function that instantiates the wrapper type with the input item.
/// </summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output wrapper type. Must be a class with a constructor accepting TIn.</typeparam>
internal sealed class SelfJoinTransform<TIn, TOut> : TransformNode<TIn, TOut>
    where TOut : class
{
    private readonly Func<TIn, TOut> _factory;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SelfJoinTransform{TIn, TOut}" /> class.
    ///     Uses reflection to create a factory function that instantiates TOut with TIn.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when TOut does not have a public constructor that accepts TIn.
    /// </exception>
    public SelfJoinTransform()
    {
        _factory = CreateFactory();
    }

    /// <summary>
    ///     Executes the transformation by wrapping the input item using the cached factory.
    /// </summary>
    /// <param name="item">The input item to wrap.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task representing the wrapped item.</returns>
    public override Task<TOut> ExecuteAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var result = _factory(item);

        Console.WriteLine(
            $"[SelfJoinTransform DEBUG] Wrapping item of type {typeof(TIn).Name} to {typeof(TOut).Name}, result type: {result?.GetType().FullName}");

        return Task.FromResult(result!);
    }

    /// <summary>
    ///     Provides a ValueTask-based execution hook for execution strategies.
    ///     Returns a naturally produced ValueTask to avoid per-item allocations.
    /// </summary>
    /// <param name="item">The input item.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A ValueTask representing the wrapped item.</returns>
    protected internal override ValueTask<TOut> ExecuteValueTaskAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new ValueTask<TOut>(_factory(item));
    }

    private static Func<TIn, TOut> CreateFactory()
    {
        var constructorInfo = typeof(TOut).GetConstructor(
            BindingFlags.Public | BindingFlags.Instance,
            null,
            [typeof(TIn)],
            null);

        if (constructorInfo is null)
        {
            throw new InvalidOperationException(
                $"Type {typeof(TOut).Name} does not have a public constructor that accepts {typeof(TIn).Name}.");
        }

        var inputParameter = Expression.Parameter(typeof(TIn), "item");
        var newExpression = Expression.New(constructorInfo, inputParameter);
        var lambda = Expression.Lambda<Func<TIn, TOut>>(newExpression, inputParameter);
        return lambda.Compile();
    }
}
