using NPipeline.DataFlow;

namespace NPipeline.Nodes;

/// <summary>
///     Convenience base class that bridges strongly-typed merge operations to the untyped framework interface.
/// </summary>
/// <typeparam name="TIn">The type of items in all input pipes being merged.</typeparam>
/// <remarks>
///     <para>
///         Custom merge nodes allow you to combine multiple input streams into a single output stream
///         with custom merge logic. Unlike simple concatenation, merge enables sophisticated patterns like:
///         - Interleaving items from multiple sources
///         - Round-robin scheduling
///         - Priority-based selection
///         - Temporal coordination (e.g., matching items from parallel streams)
///     </para>
///     <para>
///         Inherit from this class when you need to implement custom merge logic. The framework
///         handles the complexity of managing multiple input pipes and calling your merge implementation.
///     </para>
///     <para>
///         All input pipes must contain items of the same type <typeparamref name="TIn" />.
///         If you need to merge pipes of different types, consider using a join node or adding
///         a type conversion stage beforehand.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// // Round-robin merge of two streams
/// public class RoundRobinMerge : CustomMergeNode&lt;int&gt;
/// {
///     public override async Task&lt;IDataPipe&lt;int&gt;&gt; MergeAsync(
///         IEnumerable&lt;IDataPipe&gt; pipes,
///         CancellationToken cancellationToken)
///     {
///         var typedPipes = pipes.Cast&lt;IDataPipe&lt;int&gt;&gt;().ToList();
///         return new StreamingDataPipe&lt;int&gt;(RoundRobinIterator(typedPipes, cancellationToken));
///     }
/// 
///     private async IAsyncEnumerable&lt;int&gt; RoundRobinIterator(
///         IList&lt;IDataPipe&lt;int&gt;&gt; pipes,
///         [EnumeratorCancellation] CancellationToken ct)
///     {
///         var enumerators = pipes
///             .Select(p => p.GetAsyncEnumerator(ct))
///             .ToList();
/// 
///         int activeCount = enumerators.Count;
///         while (activeCount &gt; 0)
///         {
///             for (int i = 0; i &lt; enumerators.Count; i++)
///             {
///                 if (enumerators[i] == null) continue;
/// 
///                 if (await enumerators[i].MoveNextAsync())
///                 {
///                     yield return enumerators[i].Current;
///                 }
///                 else
///                 {
///                     await enumerators[i].DisposeAsync();
///                     enumerators[i] = null;
///                     activeCount--;
///                 }
///             }
///         }
///     }
/// }
/// </code>
/// </example>
public abstract class CustomMergeNode<TIn> : ICustomMergeNode<TIn>, ICustomMergeNodeUntyped, INodeTypeMetadata
{
    /// <summary>
    ///     Merges multiple input pipes of type <typeparamref name="TIn" /> into a single output pipe.
    /// </summary>
    /// <param name="pipes">The input data pipes to merge. All pipes must yield items of type <typeparamref name="TIn" />.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A data pipe containing the merged output stream.</returns>
    /// <remarks>
    ///     Your implementation should consume from the input pipes and emit a single output pipe.
    ///     The framework will handle disposal of both input and output pipes.
    /// </remarks>
    public abstract Task<IDataPipe<TIn>> MergeAsync(IEnumerable<IDataPipe> pipes, CancellationToken cancellationToken);

    public virtual ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    public async Task<IDataPipe> MergeAsyncUntyped(IEnumerable<IDataPipe> pipes, CancellationToken cancellationToken)
    {
        return await MergeAsync(pipes, cancellationToken).ConfigureAwait(false);
    }

    // Metadata (used for sink scenario only; transformations would have both in/out but this base
    // class is typically combined with a sink or transform base that already supplies metadata).
    public Type InputType => typeof(TIn);
    public Type? OutputType => null; // For sinks / merge endpoints
}
