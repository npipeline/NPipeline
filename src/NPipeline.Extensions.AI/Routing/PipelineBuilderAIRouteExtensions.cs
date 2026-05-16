using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.AI.Nodes;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Routing;

/// <summary>Extension methods for creating AI-driven conditional route nodes.</summary>
public static class PipelineBuilderAIRouteExtensions
{
    /// <summary>Adds an AI-driven route node that classifies each item via an LLM and routes it conditionally.</summary>
    /// <typeparam name="TIn">The input item type (passed through with enrichment).</typeparam>
    /// <typeparam name="TField">The AI-generated field type deserialized from the LLM response.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="chatClient">The <see cref="IChatClient"/> to use for LLM calls.</param>
    /// <param name="configure">Configuration delegate for the enrich options including system prompt, item template, and result mapper.</param>
    /// <param name="name">Optional base name for the generated nodes.</param>
    /// <returns>An <see cref="AIRouteBuilder{T}"/> for defining route branches.</returns>
    public static AIRouteBuilder<TIn> AddAIRoute<TIn, TField>(
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

        var baseName = name ?? typeof(AIEnrichNode<TIn, TField>).Name;

        var enrichHandle = builder.AddTransform<AIEnrichNode<TIn, TField>, TIn, TIn>($"{baseName}_enrich");
        var enrichNode = new AIEnrichNode<TIn, TField>(chatClient) { Options = options };
        builder.AddPreconfiguredNodeInstance(enrichHandle.Id, enrichNode);

        var routeHandle = builder.AddRoute<TIn>($"{baseName}_route");
        builder.Connect(enrichHandle, routeHandle);

        return new AIRouteBuilder<TIn>(builder, enrichHandle, routeHandle);
    }

    /// <summary>Adds a stream-level AI-driven route node that internally buffers, enriches, and routes each item.</summary>
    /// <typeparam name="TIn">The input item type.</typeparam>
    /// <typeparam name="TField">The AI-generated field type deserialized from the LLM response.</typeparam>
    /// <param name="builder">The pipeline builder.</param>
    /// <param name="chatClient">The <see cref="IChatClient"/> to use for LLM calls.</param>
    /// <param name="configure">Configuration delegate for the stream batched enrich options.</param>
    /// <param name="name">Optional base name for the generated nodes.</param>
    /// <returns>An <see cref="AIRouteBuilder{T}"/> for defining route branches.</returns>
    public static AIRouteBuilder<TIn> AddAIBatchedStreamRoute<TIn, TField>(
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

        var baseName = name ?? typeof(AIBatchedStreamEnrichNode<TIn, TField>).Name;

        var enrichHandle = builder.AddStreamTransform<AIBatchedStreamEnrichNode<TIn, TField>, TIn, TIn>($"{baseName}_enrich");
        var enrichNode = new AIBatchedStreamEnrichNode<TIn, TField>(chatClient) { Options = options };
        builder.AddPreconfiguredNodeInstance(enrichHandle.Id, enrichNode);

        var routeHandle = builder.AddRoute<TIn>($"{baseName}_route");
        builder.Connect(enrichHandle, routeHandle);

        return new AIRouteBuilder<TIn>(builder, enrichHandle, routeHandle);
    }
}
