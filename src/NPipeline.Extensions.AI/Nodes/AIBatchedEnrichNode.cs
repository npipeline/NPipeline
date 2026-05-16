using Microsoft.Extensions.AI;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.AI.Exceptions;
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
        var options = AIOptionGuards.Validate(Options);

        var aiResults = await AIInvoker.InvokeBatchedEnrichAsync<TIn, TField>(
            _chatClient,
            item,
            options.SystemPrompt!,
            options.BatchTemplate!,
            options.Temperature,
            options.MaxOutputTokens,
            options.UseNativeStructuredOutput,
            options.ConfigureOptions,
            cancellationToken).ConfigureAwait(false);

        var enriched = new List<TIn>(item.Count);
        using var inputEnumerator = item.GetEnumerator();
        using var resultEnumerator = aiResults.GetEnumerator();

        while (inputEnumerator.MoveNext())
        {
            if (!resultEnumerator.MoveNext())
            {
                throw new AITransformException(
                    "Batch enrichment produced fewer results than input items.",
                    new InvalidOperationException("Batch enrichment result count was smaller than input count."))
                {
                    OriginalItem = item,
                };
            }

            enriched.Add(MapResult(options.ResultMapper!, inputEnumerator.Current, resultEnumerator.Current));
        }

        if (resultEnumerator.MoveNext())
        {
            throw new AITransformException(
                "Batch enrichment produced more results than input items.",
                new InvalidOperationException("Batch enrichment result count was greater than input count."))
            {
                OriginalItem = item,
            };
        }

        return enriched;
    }

    private static TIn MapResult(ResultMapper<TIn, TField> resultMapper, TIn input, TField aiResult)
    {
        try
        {
            return resultMapper(input, aiResult);
        }
        catch (Exception ex)
        {
            if (ex is AITransformException)
                throw;

            throw new AITransformException("ResultMapper delegate failed.", ex)
            {
                OriginalItem = input,
            };
        }
    }
}
