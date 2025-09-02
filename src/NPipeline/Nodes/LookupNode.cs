using NPipeline.Pipeline;

namespace NPipeline.Nodes;

/// <summary>
///     A base class for transform nodes that perform a key-based lookup to enrich or transform an item.
///     This pattern is ideal for scenarios where you need to fetch additional data from an external source (like a database, cache, or API) based on a key from
///     the input item.
/// </summary>
/// <typeparam name="TIn">The type of the input data.</typeparam>
/// <typeparam name="TKey">The type of the key used for the lookup. Must not be nullable.</typeparam>
/// <typeparam name="TValue">The type of the value returned by the lookup.</typeparam>
/// <typeparam name="TOut">The type of the output data.</typeparam>
public abstract class LookupNode<TIn, TKey, TValue, TOut> : TransformNode<TIn, TOut> where TKey : notnull
{
    /// <summary>
    ///     Extracts the lookup key from the input item.
    /// </summary>
    /// <param name="input">The input item.</param>
    /// <param name="context">The pipeline context.</param>
    /// <returns>The key to use for the lookup.</returns>
    protected abstract TKey ExtractKey(TIn input, PipelineContext context);

    /// <summary>
    ///     Performs the asynchronous lookup operation to retrieve a value based on the provided key.
    /// </summary>
    /// <param name="key">The key to look up.</param>
    /// <param name="context">The current pipeline context.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous lookup operation. The task result contains the looked-up value, or null if not found.</returns>
    protected abstract Task<TValue?> LookupAsync(TKey key, PipelineContext context, CancellationToken cancellationToken);

    /// <summary>
    ///     Creates the final output item by combining the original input with the value retrieved from the lookup.
    /// </summary>
    /// <param name="input">The original input item.</param>
    /// <param name="lookupValue">The value retrieved from the lookup, or null if no value was found.</param>
    /// <param name="context">The pipeline context.</param>
    /// <returns>The final transformed output item.</returns>
    protected abstract TOut CreateOutput(TIn input, TValue? lookupValue, PipelineContext context);

    /// <summary>
    ///     Orchestrates the lookup and transformation process for a single item.
    ///     This method extracts the key, performs the lookup, and then creates the output.
    /// </summary>
    /// <param name="item">The input item to transform.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The transformed output item.</returns>
    public override async Task<TOut> ExecuteAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        var key = ExtractKey(item, context);
        var lookupValue = await LookupAsync(key, context, cancellationToken).ConfigureAwait(false);
        return CreateOutput(item, lookupValue, context);
    }
}
