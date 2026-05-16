using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Nodes;

/// <summary>Transforms a batch of input items by sending them together to an LLM and deserializing the response into a batch of output items.</summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
public sealed class AIBatchedTransformNode<TIn, TOut> : TransformNode<IReadOnlyCollection<TIn>, IReadOnlyCollection<TOut>>
{
    private readonly IChatClient _chatClient;

    /// <summary>Initializes a new instance with the specified <see cref="IChatClient" />.</summary>
    /// <param name="chatClient">The <see cref="IChatClient" /> to use. The caller retains ownership of this client and is responsible for its lifecycle and disposal.</param>
    public AIBatchedTransformNode(IChatClient chatClient)
    {
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
    }

    /// <summary>Gets or sets the batched transform options.</summary>
    public AIBatchedTransformOptions<TIn, TOut> Options { get; set; } = new();

    /// <inheritdoc />
    public override async Task<IReadOnlyCollection<TOut>> TransformAsync(IReadOnlyCollection<TIn> item, PipelineContext context,
        CancellationToken cancellationToken)
    {
        var options = AIOptionGuards.Validate(Options);

        return await AIInvoker.InvokeBatchedTransformAsync<TIn, TOut>(
            _chatClient,
            item,
            options.SystemPrompt!,
            options.BatchTemplate!,
            options.Temperature,
            options.MaxOutputTokens,
            options.UseNativeStructuredOutput,
            options.ConfigureOptions,
            cancellationToken).ConfigureAwait(false);
    }
}
