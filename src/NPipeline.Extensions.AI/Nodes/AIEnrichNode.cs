using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.AI.Exceptions;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Nodes;

/// <summary>Enriches each input item by sending it to an LLM, receiving a typed field back, and splicing the AI result into the original item via a result mapper.</summary>
/// <typeparam name="TIn">The input item type (passed through unchanged except for AI-augmented fields).</typeparam>
/// <typeparam name="TField">The AI-generated field type deserialized from the LLM response.</typeparam>
public sealed class AIEnrichNode<TIn, TField> : TransformNode<TIn, TIn>
{
    private readonly IChatClient _chatClient;

    /// <summary>Initializes a new instance with the specified <see cref="IChatClient" />.</summary>
    /// <param name="chatClient">The <see cref="IChatClient" /> to use. The caller retains ownership of this client and is responsible for its lifecycle and disposal.</param>
    public AIEnrichNode(IChatClient chatClient)
    {
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
    }

    /// <summary>Gets or sets the enrich options including system prompt, item template, and result mapper.</summary>
    public AIEnrichOptions<TIn, TField> Options { get; set; } = new();

    /// <inheritdoc />
    public override async Task<TIn> TransformAsync(TIn item, PipelineContext context, CancellationToken cancellationToken)
    {
        var options = AIOptionGuards.Validate(Options);

        var aiResult = await AIInvoker.InvokeEnrichAsync<TIn, TField>(
            _chatClient,
            item,
            options.SystemPrompt!,
            options.ItemTemplate!,
            options.Temperature,
            options.MaxOutputTokens,
            options.UseNativeStructuredOutput,
            options.ConfigureOptions,
            cancellationToken).ConfigureAwait(false);

        return MapResult(options.ResultMapper!, item, aiResult);
    }

    private static TIn MapResult(ResultMapper<TIn, TField> resultMapper, TIn item, TField aiResult)
    {
        try
        {
            return resultMapper(item, aiResult);
        }
        catch (Exception ex)
        {
            if (ex is AITransformException)
                throw;

            throw new AITransformException("ResultMapper delegate failed.", ex)
            {
                OriginalItem = item,
            };
        }
    }
}
