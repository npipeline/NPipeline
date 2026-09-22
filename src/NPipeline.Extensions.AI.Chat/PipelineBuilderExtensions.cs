using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Chat.Configuration;
using NPipeline.Extensions.AI.Chat.Nodes;
using NPipeline.Graph;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Chat;

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
    public static TransformNodeHandle<TIn, TOut> AddChatTransform<TIn, TOut>(
        this PipelineBuilder builder,
        IChatClient chatClient,
        Action<ChatTransformOptionsBuilder<TIn, TOut>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(configure);

        var optionsBuilder = new ChatTransformOptionsBuilder<TIn, TOut>();
        configure(optionsBuilder);
        var options = optionsBuilder.Build();

        var nodeName = name ?? typeof(ChatTransformNode<TIn, TOut>).Name;
        var handle = builder.AddTransform<ChatTransformNode<TIn, TOut>, TIn, TOut>(nodeName);

        var node = new ChatTransformNode<TIn, TOut>(chatClient) { Options = options };
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
    public static TransformNodeHandle<IReadOnlyCollection<TIn>, IReadOnlyCollection<TOut>> AddChatBatchedTransform<TIn, TOut>(
        this PipelineBuilder builder,
        IChatClient chatClient,
        Action<ChatBatchedTransformOptionsBuilder<TIn, TOut>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(configure);

        var optionsBuilder = new ChatBatchedTransformOptionsBuilder<TIn, TOut>();
        configure(optionsBuilder);
        var options = optionsBuilder.Build();

        var nodeName = name ?? typeof(ChatBatchedTransformNode<TIn, TOut>).Name;
        var handle = builder.AddTransform<ChatBatchedTransformNode<TIn, TOut>, IReadOnlyCollection<TIn>, IReadOnlyCollection<TOut>>(nodeName);

        var node = new ChatBatchedTransformNode<TIn, TOut>(chatClient) { Options = options };
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
    public static TransformNodeHandle<TIn, TOut> AddChatBatchedStreamTransform<TIn, TOut>(
        this PipelineBuilder builder,
        IChatClient chatClient,
        Action<ChatBatchedStreamTransformOptionsBuilder<TIn, TOut>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(configure);

        var optionsBuilder = new ChatBatchedStreamTransformOptionsBuilder<TIn, TOut>();
        configure(optionsBuilder);
        var options = optionsBuilder.Build();

        var nodeName = name ?? typeof(ChatBatchedStreamTransformNode<TIn, TOut>).Name;
        var handle = builder.AddStreamTransform<ChatBatchedStreamTransformNode<TIn, TOut>, TIn, TOut>(nodeName);

        var node = new ChatBatchedStreamTransformNode<TIn, TOut>(chatClient)
        {
            Options = options,
        };

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
    public static TransformNodeHandle<TIn, TIn> AddChatEnrichment<TIn, TField>(
        this PipelineBuilder builder,
        IChatClient chatClient,
        Action<ChatEnrichmentOptionsBuilder<TIn, TField>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(configure);

        var optionsBuilder = new ChatEnrichmentOptionsBuilder<TIn, TField>();
        configure(optionsBuilder);
        var options = optionsBuilder.Build();

        var nodeName = name ?? typeof(ChatEnrichmentNode<TIn, TField>).Name;
        var handle = builder.AddTransform<ChatEnrichmentNode<TIn, TField>, TIn, TIn>(nodeName);

        var node = new ChatEnrichmentNode<TIn, TField>(chatClient) { Options = options };
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
    public static TransformNodeHandle<IReadOnlyCollection<TIn>, IReadOnlyCollection<TIn>> AddChatBatchedEnrichment<TIn, TField>(
        this PipelineBuilder builder,
        IChatClient chatClient,
        Action<ChatBatchedEnrichmentOptionsBuilder<TIn, TField>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(configure);

        var optionsBuilder = new ChatBatchedEnrichmentOptionsBuilder<TIn, TField>();
        configure(optionsBuilder);
        var options = optionsBuilder.Build();

        var nodeName = name ?? typeof(ChatBatchedEnrichmentNode<TIn, TField>).Name;
        var handle = builder.AddTransform<ChatBatchedEnrichmentNode<TIn, TField>, IReadOnlyCollection<TIn>, IReadOnlyCollection<TIn>>(nodeName);

        var node = new ChatBatchedEnrichmentNode<TIn, TField>(chatClient) { Options = options };
        builder.AddPreconfiguredNodeInstance(handle.Id, node);

        return handle;
    }

    /// <summary>
    ///     Adds a convenience chain for batch -&gt; AI batched enrich -&gt; unbatch and returns handles for wiring before and after the chain.
    /// </summary>
    /// <typeparam name="T">The input and output item type.</typeparam>
    /// <typeparam name="TField">The AI-generated field type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="chatClient">The <see cref="IChatClient" /> to use for LLM calls.</param>
    /// <param name="batchSize">The batch size used by the batcher.</param>
    /// <param name="batchTimeout">The batch timeout used by the batcher.</param>
    /// <param name="configure">Configuration delegate for the batched enrich options.</param>
    /// <param name="name">Optional base name for generated nodes.</param>
    /// <returns>
    ///     A tuple containing input and output handles that can be connected like a single <c>T -&gt; T</c> stage.
    ///     The input handle targets the first (batch) node and the output handle sources the final (unbatch) node.
    /// </returns>
    public static (TransformNodeHandle<T, T> inputHandle, TransformNodeHandle<T, T> outputHandle) AddChatBatchedEnrichmentWithUnbatch<T, TField>(
        this PipelineBuilder builder,
        IChatClient chatClient,
        int batchSize,
        TimeSpan batchTimeout,
        Action<ChatBatchedEnrichmentOptionsBuilder<T, TField>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(configure);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);

        if (batchTimeout <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(batchTimeout), "Batch timeout must be greater than zero.");

        var baseName = name ?? typeof(ChatBatchedEnrichmentNode<T, TField>).Name;

        var batchHandle = builder.AddBatcher<T>($"{baseName}_batch", batchSize, batchTimeout);
        var enrichHandle = builder.AddChatBatchedEnrichment(chatClient, configure, $"{baseName}_enrich");
        var unbatchHandle = builder.AddReadOnlyCollectionUnbatcher<T>($"{baseName}_unbatch");

        builder.Connect(batchHandle, enrichHandle);
        builder.Connect(enrichHandle, unbatchHandle);

        return (new TransformNodeHandle<T, T>(batchHandle.Id), new TransformNodeHandle<T, T>(unbatchHandle.Id));
    }

    /// <summary>Adds a stream-level AI enrichment node that internally buffers, sends batches to an LLM, splices fields, and fans out enriched items.</summary>
    /// <typeparam name="TIn">The input item type.</typeparam>
    /// <typeparam name="TField">The AI-generated field type.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="chatClient">The <see cref="IChatClient" /> to use for LLM calls.</param>
    /// <param name="configure">Configuration delegate for the stream batched enrich options.</param>
    /// <param name="name">Optional node name for debugging.</param>
    /// <returns>A handle to the registered node.</returns>
    public static TransformNodeHandle<TIn, TIn> AddChatBatchedStreamEnrichment<TIn, TField>(
        this PipelineBuilder builder,
        IChatClient chatClient,
        Action<ChatBatchedStreamEnrichmentOptionsBuilder<TIn, TField>> configure,
        string? name = null)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(chatClient);
        ArgumentNullException.ThrowIfNull(configure);

        var optionsBuilder = new ChatBatchedStreamEnrichmentOptionsBuilder<TIn, TField>();
        configure(optionsBuilder);
        var options = optionsBuilder.Build();

        var nodeName = name ?? typeof(ChatBatchedStreamEnrichmentNode<TIn, TField>).Name;
        var handle = builder.AddStreamTransform<ChatBatchedStreamEnrichmentNode<TIn, TField>, TIn, TIn>(nodeName);

        var node = new ChatBatchedStreamEnrichmentNode<TIn, TField>(chatClient)
        {
            Options = options,
        };

        builder.AddPreconfiguredNodeInstance(handle.Id, node);

        return handle;
    }
}
