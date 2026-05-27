using System.Runtime.CompilerServices;
using Microsoft.Extensions.AI;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Extensions.AI.Configuration;
using NPipeline.Extensions.AI.Exceptions;
using NPipeline.Extensions.AI.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Nodes;

/// <summary>
///     A stream-level enrichment that internally buffers items into batches, sends each batch to an LLM, splices AI-generated fields back, and fans out
///     enriched items.
/// </summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TField">The AI-generated field type.</typeparam>
public sealed class AIBatchedStreamEnrichNode<TIn, TField> : IStreamTransformNode<TIn, TIn>
{
    private readonly IChatClient _chatClient;

    /// <summary>Initializes a new instance with the specified <see cref="IChatClient" />.</summary>
    /// <param name="chatClient">The <see cref="IChatClient" /> to use. The caller retains ownership of this client and is responsible for its lifecycle and disposal.</param>
    public AIBatchedStreamEnrichNode(IChatClient chatClient)
    {
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
    }

    /// <summary>Gets or sets the stream batched enrich options including batch size and result mapper.</summary>
    public AIBatchedStreamEnrichOptions<TIn, TField> Options { get; set; } = new();

    /// <summary>
    ///     Gets or sets the execution strategy.
    ///     Defaults to a stream-aware passthrough strategy so this node preserves its native stream batching behavior.
    /// </summary>
    public IExecutionStrategy ExecutionStrategy { get; set; } = AIStreamPassthroughExecutionStrategy.Instance;

    /// <inheritdoc />
    public async IAsyncEnumerable<TIn> TransformAsync(
        IAsyncEnumerable<TIn> items,
        PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var options = AIOptionGuards.Validate(Options);
        var batchSize = options.BatchSize!.Value;
        var timeout = options.BatchTimeout ?? TimeSpan.FromSeconds(5);

        await foreach (var batch in items.BatchAsync(batchSize, timeout, cancellationToken).ConfigureAwait(false))
        {
            var enriched = await ProcessBatchAsync(batch, options, cancellationToken).ConfigureAwait(false);

            foreach (var item in enriched)
            {
                yield return item;
            }
        }
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    private async Task<IReadOnlyCollection<TIn>> ProcessBatchAsync(
        IReadOnlyCollection<TIn> batch,
        AIBatchedStreamEnrichOptions<TIn, TField> options,
        CancellationToken cancellationToken)
    {
        var aiResults = await AIInvoker.InvokeBatchedEnrichAsync<TIn, TField>(
            _chatClient,
            batch,
            options.SystemPrompt!,
            options.BatchTemplate!,
            options.Temperature,
            options.MaxOutputTokens,
            options.UseNativeStructuredOutput,
            options.ConfigureOptions,
            cancellationToken).ConfigureAwait(false);

        var enriched = new List<TIn>(batch.Count);
        using var inputEnumerator = batch.GetEnumerator();
        using var resultEnumerator = aiResults.GetEnumerator();

        while (inputEnumerator.MoveNext())
        {
            if (!resultEnumerator.MoveNext())
            {
                throw new AITransformException(
                    "Batch enrichment produced fewer results than input items.",
                    new InvalidOperationException("Batch enrichment result count was smaller than input count."))
                {
                    OriginalItem = batch,
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
                OriginalItem = batch,
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
