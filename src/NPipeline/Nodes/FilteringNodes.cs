using System.Runtime.CompilerServices;
using NPipeline.Attributes.Lineage;
using NPipeline.Execution;
using NPipeline.Execution.Lineage;
using NPipeline.Execution.Strategies;
using NPipeline.Lineage;
using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     Passes through the items that satisfy a predicate and drops the rest.
/// </summary>
/// <typeparam name="T">The item type.</typeparam>
/// <remarks>
///     Filtering is a stream transform because <see cref="ITransformNode{TIn,TOut}" /> must return one output per
///     input and so cannot drop anything. Items are dropped as they are read; nothing is buffered.
/// </remarks>
[TransformCardinality(TransformCardinality.OneToZeroOrOne)]
public sealed class FilterNode<T> : IStreamTransformNode<T, T>, IExecutionStrategyProvider, ILineageProvenanceNode
{
    private readonly Func<T, CancellationToken, ValueTask<bool>> _predicate;

    /// <summary>
    ///     Initializes a filter with a synchronous predicate.
    /// </summary>
    /// <param name="predicate">Returns <see langword="true" /> to keep the item.</param>
    public FilterNode(Func<T, bool> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        _predicate = (item, _) => ValueTask.FromResult(predicate(item));
    }

    /// <summary>
    ///     Initializes a filter with an asynchronous predicate, for a decision that needs I/O.
    /// </summary>
    /// <param name="predicate">Returns <see langword="true" /> to keep the item.</param>
    public FilterNode(Func<T, CancellationToken, ValueTask<bool>> predicate)
    {
        _predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));
    }

    /// <inheritdoc />
    public IExecutionStrategy DefaultExecutionStrategy => StreamPassthroughExecutionStrategy.Instance;

    /// <inheritdoc />
    public async IAsyncEnumerable<T> TransformAsync(
        IAsyncEnumerable<T> items,
        PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var lineage = LineageProvenanceSupport.WriterFor(this, context);
        long index = 0;

        await foreach (var item in items.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (await _predicate(item, cancellationToken).ConfigureAwait(false))
            {
                lineage.ReportOutput(index++);
                yield return item;
            }
            else
                lineage.ReportDone(index++, LineageOutcomeReason.FilteredOut);
        }
    }
}

/// <summary>
///     Expands each input item into zero or more output items.
/// </summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
/// <remarks>
///     The other shape <see cref="ITransformNode{TIn,TOut}" /> cannot express. Each item's results are yielded as
///     they are produced, so a selector returning a lazy sequence stays lazy.
/// </remarks>
[TransformCardinality(TransformCardinality.OneToMany)]
public sealed class SelectManyNode<TIn, TOut> : IStreamTransformNode<TIn, TOut>, IExecutionStrategyProvider, ILineageProvenanceNode
{
    private readonly Func<TIn, CancellationToken, IAsyncEnumerable<TOut>> _selector;

    /// <summary>
    ///     Initializes a select-many with a synchronous selector.
    /// </summary>
    /// <param name="selector">Returns the items to emit for one input item.</param>
    public SelectManyNode(Func<TIn, IEnumerable<TOut>> selector)
    {
        ArgumentNullException.ThrowIfNull(selector);
        _selector = (item, _) => selector(item).ToAsyncEnumerable();
    }

    /// <summary>
    ///     Initializes a select-many with an asynchronous selector, for expansion that needs I/O.
    /// </summary>
    /// <param name="selector">Returns the items to emit for one input item.</param>
    public SelectManyNode(Func<TIn, CancellationToken, IAsyncEnumerable<TOut>> selector)
    {
        _selector = selector ?? throw new ArgumentNullException(nameof(selector));
    }

    /// <inheritdoc />
    public IExecutionStrategy DefaultExecutionStrategy => StreamPassthroughExecutionStrategy.Instance;

    /// <inheritdoc />
    public async IAsyncEnumerable<TOut> TransformAsync(
        IAsyncEnumerable<TIn> items,
        PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var lineage = LineageProvenanceSupport.WriterFor(this, context);
        long index = 0;

        await foreach (var item in items.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            var emitted = false;

            await foreach (var produced in _selector(item, cancellationToken).WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                emitted = true;
                lineage.ReportPartialOutput(index);
                yield return produced;
            }

            lineage.ReportDone(index++, emitted ? LineageOutcomeReason.Emitted : LineageOutcomeReason.ConsumedWithoutEmission);
        }
    }
}
