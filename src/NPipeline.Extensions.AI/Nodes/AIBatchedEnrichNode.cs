using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Nodes;

/// <summary>Enriches a batch of input items by sending them together to an LLM and splicing each AI-generated field back onto its corresponding input item.</summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TField">The AI-generated field type.</typeparam>
public sealed class AIBatchedEnrichNode<TIn, TField> : TransformNode<IReadOnlyCollection<TIn>, IReadOnlyCollection<TIn>>
{
    private readonly IChatClient _chatClient;

    /// <summary>Initializes a new instance with the specified <see cref="IChatClient" />.</summary>
    /// <param name="chatClient">The <see cref="IChatClient" /> to use. The caller retains ownership of this client and is responsible for its lifecycle and disposal.</param>
    public AIBatchedEnrichNode(IChatClient chatClient)
    {
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
    }

    /// <summary>Gets or sets the batched enrich options including result mapper.</summary>
    public AIBatchedEnrichOptions<TIn, TField> Options { get; set; } = new();

    /// <inheritdoc />
    public override async Task<IReadOnlyCollection<TIn>> TransformAsync(IReadOnlyCollection<TIn> item, PipelineContext context,
        CancellationToken cancellationToken)
    {
        var aiResults = await AIInvoker.InvokeBatchedEnrichAsync<TIn, TField>(
            _chatClient,
            item,
            Options.SystemPrompt!,
            Options.BatchTemplate!,
            Options.Temperature,
            Options.MaxOutputTokens,
            Options.UseNativeStructuredOutput,
            Options.ConfigureOptions,
            cancellationToken).ConfigureAwait(false);

        var enriched = new List<TIn>(item.Count);
        using var inputEnumerator = item.GetEnumerator();
        using var resultEnumerator = aiResults.GetEnumerator();

        while (inputEnumerator.MoveNext() && resultEnumerator.MoveNext())
        {
            enriched.Add(Options.ResultMapper!(inputEnumerator.Current, resultEnumerator.Current));
        }

        return enriched;
    }
}
