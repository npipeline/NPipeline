using System.Collections.Concurrent;
using NPipeline.ErrorHandling;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace Sample_EdgeResilience;

/// <summary>
///     Reads SKUs, looks up each one's stock level over HTTP, and prints the result. Items that still fail go to a
///     dead-letter sink.
/// </summary>
/// <param name="inventory">The HTTP client the lookup node calls. It carries its own retries.</param>
/// <param name="pipelineRetriesToo">
///     When true, the lookup node keeps the Default profile's item retry (L1) as well, to show how the two layers
///     multiply. When false, which is the recommended setup, L1 is off for that node.
/// </param>
/// <param name="deadLetters">Collects the items that failed.</param>
internal sealed class StockPipeline(HttpClient inventory, bool pipelineRetriesToo, DeadLetterCollector deadLetters) : IPipelineDefinition
{
    public static readonly string[] Skus = ["SKU-1", "SKU-2", "SKU-3", "SKU-4", "SKU-5"];

    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        var source = builder.AddSource(() => Skus, "skus");
        var lookup = builder.AddTransform<StockLookupTransform, string, StockLevel>("stock-lookup");
        _ = builder.AddPreconfiguredNodeInstance(lookup.Id, new StockLookupTransform(inventory));
        var sink = builder.AddSink<StockLevel>(level => Console.WriteLine($"    {level.Sku}: {level.Quantity} in stock"), "print");

        builder.Connect(source, lookup).Connect(lookup, sink);

        // Items that still fail after every retry go to the dead-letter sink instead of failing the pipeline.
        builder.AddDeadLetterSink(deadLetters);

        builder.WithResilience(lookup, options => options with
        {
            // The HTTP client already retried transient failures. A 503 that reaches the node is one NResilience
            // gave up on, and the pipeline's classifier would call it transient and retry it again. Turn item
            // retry off for this node, so each call is retried by exactly one layer.
            ItemRetry = pipelineRetriesToo ? options.ItemRetry : ItemRetryOptions.None,
            OnItemFailure = ItemFailureAction.DeadLetter,
        });
    }
}

/// <summary>
///     A dead-letter sink that keeps what it receives in memory, standing in for a queue or table.
/// </summary>
internal sealed class DeadLetterCollector : IDeadLetterSink
{
    private readonly ConcurrentQueue<DeadLetterEnvelope> _envelopes = new();

    public IReadOnlyList<DeadLetterEnvelope> Envelopes => [.. _envelopes];

    public void Clear()
    {
        _envelopes.Clear();
    }

    public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
    {
        _envelopes.Enqueue(envelope);
        return Task.CompletedTask;
    }
}
