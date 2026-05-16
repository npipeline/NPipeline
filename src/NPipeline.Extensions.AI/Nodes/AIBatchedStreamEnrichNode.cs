using System.Runtime.CompilerServices;
using Microsoft.Extensions.AI;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Extensions.AI.Configuration;
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
    ///     Gets or sets the execution strategy. Required by <see cref="IStreamTransformNode{TIn, TOut}" /> but not consumed by this node; batch processing is
    ///     always sequential.
    /// </summary>
    public IExecutionStrategy ExecutionStrategy { get; set; } = new SequentialExecutionStrategy();

    /// <inheritdoc />
    public async IAsyncEnumerable<TIn> TransformAsync(
        IAsyncEnumerable<TIn> items,
        PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var batchSize = Options.BatchSize!.Value;
        var timeout = Options.BatchTimeout ?? TimeSpan.FromSeconds(5);

        await foreach (var batch in items.BatchAsync(batchSize, timeout, cancellationToken).ConfigureAwait(false))
        {
            var enriched = await ProcessBatchAsync(batch, cancellationToken).ConfigureAwait(false);

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
        CancellationToken cancellationToken)
    {
        var aiResults = await AIInvoker.InvokeBatchedEnrichAsync<TIn, TField>(
            _chatClient,
            batch,
            Options.SystemPrompt!,
            Options.BatchTemplate!,
            Options.Temperature,
            Options.MaxOutputTokens,
            Options.UseNativeStructuredOutput,
            Options.ConfigureOptions,
            cancellationToken).ConfigureAwait(false);

        var enriched = new List<TIn>(batch.Count);
        using var inputEnumerator = batch.GetEnumerator();
        using var resultEnumerator = aiResults.GetEnumerator();

        while (inputEnumerator.MoveNext() && resultEnumerator.MoveNext())
        {
            enriched.Add(Options.ResultMapper!(inputEnumerator.Current, resultEnumerator.Current));
        }

        return enriched;
    }
}
