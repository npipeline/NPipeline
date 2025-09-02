using NPipeline.Pipeline;

// ReSharper disable once CheckNamespace
namespace NPipeline.Nodes;

/// <summary>
///     A generic, reusable transform node that performs a lookup against an in-memory dictionary.
///     This node is designed to be configured and registered via the `PipelineBuilderLookupExtensions.AddInMemoryLookup` method.
/// </summary>
internal sealed class InMemoryLookupNode<TIn, TKey, TValue, TOut>(InMemoryLookupNode<TIn, TKey, TValue, TOut>.Configuration configuration)
    : LookupNode<TIn, TKey, TValue, TOut>
    where TKey : notnull
{
    protected override TKey ExtractKey(TIn input, PipelineContext context)
    {
        return configuration.KeyExtractor(input);
    }

    protected override Task<TValue?> LookupAsync(TKey key, PipelineContext context, CancellationToken cancellationToken)
    {
        configuration.Data.TryGetValue(key, out var value);
        return Task.FromResult(value);
    }

    protected override TOut CreateOutput(TIn input, TValue? lookupValue, PipelineContext context)
    {
        return configuration.OutputCreator(input, lookupValue);
    }

    // Configuration record to hold the data and logic for the lookup.
    internal sealed record Configuration(
        IReadOnlyDictionary<TKey, TValue> Data,
        Func<TIn, TKey> KeyExtractor,
        Func<TIn, TValue?, TOut> OutputCreator
    );
}
