using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Nodes;

/// <summary>Transforms each input item by sending it to an LLM and deserializing the response into a new output type.</summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type deserialized from the LLM response.</typeparam>
public sealed class AITransformNode<TIn, TOut> : TransformNode<TIn, TOut>
{
    private readonly IChatClient _chatClient;

    /// <summary>Initializes a new instance with the specified <see cref="IChatClient" />.</summary>
    /// <param name="chatClient">The <see cref="IChatClient" /> to use. The caller retains ownership of this client and is responsible for its lifecycle and disposal.</param>
    public AITransformNode(IChatClient chatClient)
    {
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
    }

    /// <summary>Gets or sets the transform options including system prompt and item template.</summary>
    public AITransformOptions<TIn, TOut> Options { get; set; } = new();

    /// <inheritdoc />
    public override async Task<TOut> TransformAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        return await AIInvoker.InvokeTransformAsync<TIn, TOut>(
            _chatClient,
            item,
            Options.SystemPrompt!,
            Options.ItemTemplate!,
            Options.Temperature,
            Options.MaxOutputTokens,
            Options.UseNativeStructuredOutput,
            Options.ConfigureOptions,
            cancellationToken).ConfigureAwait(false);
    }
}
