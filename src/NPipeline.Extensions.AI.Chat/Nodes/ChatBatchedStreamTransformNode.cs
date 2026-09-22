using System.Runtime.CompilerServices;
using Microsoft.Extensions.AI;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Extensions.AI.Chat.Configuration;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.AI.Chat.Nodes;

/// <summary>A stream-level transform that internally buffers items into batches, sends each batch to an LLM, and fans out results as individual items.</summary>
/// <typeparam name="TIn">The input item type.</typeparam>
/// <typeparam name="TOut">The output item type.</typeparam>
public sealed class ChatBatchedStreamTransformNode<TIn, TOut> : IStreamTransformNode<TIn, TOut>, IExecutionStrategyProvider
{
    private readonly IChatClient _chatClient;

    /// <summary>Initializes a new instance with the specified <see cref="IChatClient" />.</summary>
    /// <param name="chatClient">The <see cref="IChatClient" /> to use. The caller retains ownership of this client and is responsible for its lifecycle and disposal.</param>
    public ChatBatchedStreamTransformNode(IChatClient chatClient)
    {
        _chatClient = chatClient ?? throw new ArgumentNullException(nameof(chatClient));
    }

    /// <summary>Gets or sets the stream batched transform options including batch size.</summary>
    public ChatBatchedStreamTransformOptions<TIn, TOut> Options { get; set; } = new();

    /// <summary>
    ///     Gets or sets the execution strategy.
    ///     Defaults to a stream-aware passthrough strategy so this node preserves its native stream batching behavior.
    /// </summary>
    public IExecutionStrategy DefaultExecutionStrategy => StreamPassthroughExecutionStrategy.Instance;

    /// <inheritdoc />
    public async IAsyncEnumerable<TOut> TransformAsync(
        IAsyncEnumerable<TIn> items,
        PipelineContext context,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var options = ChatOptionGuards.Validate(Options);
        var batchSize = options.BatchSize!.Value;
        var timeout = options.BatchTimeout ?? TimeSpan.FromSeconds(5);

        await foreach (var batch in items.BatchAsync(batchSize, timeout, cancellationToken).ConfigureAwait(false))
        {
            var results = await ProcessBatchAsync(batch, options, cancellationToken).ConfigureAwait(false);

            foreach (var result in results)
            {
                yield return result;
            }
        }
    }

    private async Task<IReadOnlyCollection<TOut>> ProcessBatchAsync(
        IReadOnlyCollection<TIn> batch,
        ChatBatchedStreamTransformOptions<TIn, TOut> options,
        CancellationToken cancellationToken) =>
        await ChatInvoker.InvokeBatchedTransformAsync<TIn, TOut>(
            _chatClient,
            batch,
            options.SystemPrompt!,
            options.BatchTemplate!,
            options.Temperature,
            options.MaxOutputTokens,
            options.UseNativeStructuredOutput,
            options.ConfigureOptions,
            cancellationToken).ConfigureAwait(false);
}
