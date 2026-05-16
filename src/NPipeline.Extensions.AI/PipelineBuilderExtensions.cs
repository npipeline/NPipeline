using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.AI.Nodes;
using NPipeline.Graph;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI;

/// <summary>Extension methods on <see cref="PipelineBuilder" /> for registering AI nodes.</summary>
public static class PipelineBuilderExtensions
{
    /// <summary>Adds a per-item AI transform node that sends each item to an LLM and produces a new output type.</summary>
    /// <typeparam name="TIn">The input item type.</typeparam>
    /// <typeparam name="TOut">The output item type deserialized from the LLM response.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="chatClient">The <see cref="IChatClient" /> to use for LLM calls.</param>
    /// <param name="configure">Configuration delegate for the transform options.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node.</returns>
    public static TransformNodeHandle<TIn, TOut> AddAITransform<TIn, TOut>(
        this PipelineBuilder builder,
        IChatClient chatClient,
        Action<AITransformOptionsBuilder<TIn, TOut>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(configure);

        var optionsBuilder = new AITransformOptionsBuilder<TIn, TOut>();
        configure(optionsBuilder);
        var options = optionsBuilder.Build();

        var nodeName = name ?? typeof(AITransformNode<TIn, TOut>).Name;
        var handle = builder.AddTransform<AITransformNode<TIn, TOut>, TIn, TOut>(nodeName);

        var node = new AITransformNode<TIn, TOut>(chatClient) { Options = options };
        builder.AddPreconfiguredNodeInstance(handle.Id, node);

        return handle;
    }

    /// <summary>Adds a batched AI transform node that sends a collection of items to an LLM and produces a collection of output items.</summary>
    /// <typeparam name="TIn">The input item type.</typeparam>
    /// <typeparam name="TOut">The output item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="chatClient">The <see cref="IChatClient" /> to use for LLM calls.</param>
    /// <param name="configure">Configuration delegate for the batched transform options.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node.</returns>
    public static TransformNodeHandle<IReadOnlyCollection<TIn>, IReadOnlyCollection<TOut>> AddAIBatchedTransform<TIn, TOut>(
        this PipelineBuilder builder,
        IChatClient chatClient,
        Action<AIBatchedTransformOptionsBuilder<TIn, TOut>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(configure);

        var optionsBuilder = new AIBatchedTransformOptionsBuilder<TIn, TOut>();
        configure(optionsBuilder);
        var options = optionsBuilder.Build();

        var nodeName = name ?? typeof(AIBatchedTransformNode<TIn, TOut>).Name;
        var handle = builder.AddTransform<AIBatchedTransformNode<TIn, TOut>, IReadOnlyCollection<TIn>, IReadOnlyCollection<TOut>>(nodeName);

        var node = new AIBatchedTransformNode<TIn, TOut>(chatClient) { Options = options };
        builder.AddPreconfiguredNodeInstance(handle.Id, node);

        return handle;
    }

    /// <summary>Adds a stream-level AI transform that internally buffers items, sends batches to an LLM, and fans out results.</summary>
    /// <typeparam name="TIn">The input item type.</typeparam>
    /// <typeparam name="TOut">The output item type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="chatClient">The <see cref="IChatClient" /> to use for LLM calls.</param>
    /// <param name="configure">Configuration delegate for the stream batched transform options.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node.</returns>
    public static TransformNodeHandle<TIn, TOut> AddAIBatchedStreamTransform<TIn, TOut>(
        this PipelineBuilder builder,
        IChatClient chatClient,
        Action<AIBatchedStreamTransformOptionsBuilder<TIn, TOut>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(configure);

        var optionsBuilder = new AIBatchedStreamTransformOptionsBuilder<TIn, TOut>();
        configure(optionsBuilder);
        var options = optionsBuilder.Build();

        var nodeName = name ?? typeof(AIBatchedStreamTransformNode<TIn, TOut>).Name;
        var handle = builder.AddStreamTransform<AIBatchedStreamTransformNode<TIn, TOut>, TIn, TOut>(nodeName);

        var node = new AIBatchedStreamTransformNode<TIn, TOut>(chatClient) { Options = options };
        builder.AddPreconfiguredNodeInstance(handle.Id, node);

        return handle;
    }

    /// <summary>Adds an AI enrichment node that sends each item to an LLM and splices the AI-generated field back into the original item.</summary>
    /// <typeparam name="TIn">The input item type (passed through).</typeparam>
    /// <typeparam name="TField">The AI-generated field type deserialized from the LLM response.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="chatClient">The <see cref="IChatClient" /> to use for LLM calls.</param>
    /// <param name="configure">Configuration delegate for the enrich options.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node.</returns>
    public static TransformNodeHandle<TIn, TIn> AddAIEnrich<TIn, TField>(
        this PipelineBuilder builder,
        IChatClient chatClient,
        Action<AIEnrichOptionsBuilder<TIn, TField>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(configure);

        var optionsBuilder = new AIEnrichOptionsBuilder<TIn, TField>();
        configure(optionsBuilder);
        var options = optionsBuilder.Build();

        var nodeName = name ?? typeof(AIEnrichNode<TIn, TField>).Name;
        var handle = builder.AddTransform<AIEnrichNode<TIn, TField>, TIn, TIn>(nodeName);

        var node = new AIEnrichNode<TIn, TField>(chatClient) { Options = options };
        builder.AddPreconfiguredNodeInstance(handle.Id, node);

        return handle;
    }

    /// <summary>Adds a batched AI enrichment node that sends a collection of items to an LLM and splices AI-generated fields back.</summary>
    /// <typeparam name="TIn">The input item type.</typeparam>
    /// <typeparam name="TField">The AI-generated field type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="chatClient">The <see cref="IChatClient" /> to use for LLM calls.</param>
    /// <param name="configure">Configuration delegate for the batched enrich options.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node.</returns>
    public static TransformNodeHandle<IReadOnlyCollection<TIn>, IReadOnlyCollection<TIn>> AddAIBatchedEnrich<TIn, TField>(
        this PipelineBuilder builder,
        IChatClient chatClient,
        Action<AIBatchedEnrichOptionsBuilder<TIn, TField>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(configure);

        var optionsBuilder = new AIBatchedEnrichOptionsBuilder<TIn, TField>();
        configure(optionsBuilder);
        var options = optionsBuilder.Build();

        var nodeName = name ?? typeof(AIBatchedEnrichNode<TIn, TField>).Name;
        var handle = builder.AddTransform<AIBatchedEnrichNode<TIn, TField>, IReadOnlyCollection<TIn>, IReadOnlyCollection<TIn>>(nodeName);

        var node = new AIBatchedEnrichNode<TIn, TField>(chatClient) { Options = options };
        builder.AddPreconfiguredNodeInstance(handle.Id, node);

        return handle;
    }

    /// <summary>Adds a stream-level AI enrichment node that internally buffers, sends batches to an LLM, splices fields, and fans out enriched items.</summary>
    /// <typeparam name="TIn">The input item type.</typeparam>
    /// <typeparam name="TField">The AI-generated field type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="chatClient">The <see cref="IChatClient" /> to use for LLM calls.</param>
    /// <param name="configure">Configuration delegate for the stream batched enrich options.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node.</returns>
    public static TransformNodeHandle<TIn, TIn> AddAIBatchedStreamEnrich<TIn, TField>(
        this PipelineBuilder builder,
        IChatClient chatClient,
        Action<AIBatchedStreamEnrichOptionsBuilder<TIn, TField>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(configure);

        var optionsBuilder = new AIBatchedStreamEnrichOptionsBuilder<TIn, TField>();
        configure(optionsBuilder);
        var options = optionsBuilder.Build();

        var nodeName = name ?? typeof(AIBatchedStreamEnrichNode<TIn, TField>).Name;
        var handle = builder.AddStreamTransform<AIBatchedStreamEnrichNode<TIn, TField>, TIn, TIn>(nodeName);

        var node = new AIBatchedStreamEnrichNode<TIn, TField>(chatClient) { Options = options };
        builder.AddPreconfiguredNodeInstance(handle.Id, node);

        return handle;
    }
}
