using NPipeline.Graph;
using NPipeline.Nodes;

namespace NPipeline.Pipeline;

/// <summary>
///     Provides extension methods for the <see cref="PipelineBuilder" /> to simplify the creation of lookup nodes.
/// </summary>
public static class PipelineBuilderLookupExtensions
{
    /// <summary>
    ///     Adds a transform node that performs a lookup on an in-memory dictionary.
    /// </summary>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="name">An optional descriptive name for the node.</param>
    /// <param name="lookupData">The dictionary containing the data for the lookup.</param>
    /// <param name="keyExtractor">A function to extract the lookup key from an input item.</param>
    /// <param name="outputCreator">A function to create the output item from the original input and the looked-up value.</param>
    /// <returns>A handle to the newly added transform node.</returns>
    public static TransformNodeHandle<TIn, TOut> AddInMemoryLookup<TIn, TKey, TValue, TOut>(
        this PipelineBuilder builder,
        string name,
        IReadOnlyDictionary<TKey, TValue> lookupData,
        Func<TIn, TKey> keyExtractor,
        Func<TIn, TValue?, TOut> outputCreator) where TKey : notnull
    {
        // Create the configuration object that holds the data and logic.
        var config = new InMemoryLookupNode<TIn, TKey, TValue, TOut>.Configuration(lookupData, keyExtractor, outputCreator);

        // Create the node instance directly.
        var nodeInstance = new InMemoryLookupNode<TIn, TKey, TValue, TOut>(config);
        builder.RegisterBuilderDisposable(nodeInstance);

        // Register the generic node type with the builder.
        var handle = builder.AddTransform<InMemoryLookupNode<TIn, TKey, TValue, TOut>, TIn, TOut>(name);

        // Add the pre-configured instance to the builder.
        builder.AddPreconfiguredNodeInstance(handle.Id, nodeInstance);

        return handle;
    }
}
