using System.Collections.Concurrent;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Parallelism;
using NPipeline.Lineage;
using NPipeline.Lineage.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;
using NPipeline.Sampling;

namespace NPipeline.Extensions.Lineage.Tests;

/// <summary>
///     Each input item's lineage context (its index, correlation id, and ancestry) must reach the transform's execution
///     strategy, under every strategy, and again when a node restart replays the item.
/// </summary>
public sealed class LineageItemContextTests
{
    private const string LineageSinkKey = "test.lineage.sink";
    private const string StrategyKey = "test.strategy";
    private const string RestartKey = "test.restart";
    private const string StateKey = "test.state";

    // Item 2 always fails permanently and is skipped, so every later item's output would be paired with the wrong input
    // if lineage were mapped by position. Item 3 fails transiently twice, then succeeds. With restart on, item 4 fails
    // once, which fails the node and restarts it.
    private const int SkippedItem = 2;
    private const int RetriedItem = 3;
    private const int RestartItem = 4;

    public enum Strategy
    {
        Sequential,
        BlockingParallel,
        DropOldestParallel,
    }

    [Theory]
    [InlineData(Strategy.Sequential, false)]
    [InlineData(Strategy.BlockingParallel, false)]
    [InlineData(Strategy.DropOldestParallel, false)]
    [InlineData(Strategy.Sequential, true)]
    [InlineData(Strategy.BlockingParallel, true)]
    [InlineData(Strategy.DropOldestParallel, true)]
    public async Task ItemLineageContext_ReachesTheStrategy(Strategy strategy, bool restart)
    {
        var lineage = new CollectingLineageSink();
        var samples = new CollectingSampleRecorder();
        var state = new TransformState();
        var context = new PipelineContext();
        context.Items[LineageSinkKey] = lineage;
        context.Items[StrategyKey] = strategy;
        context.Items[RestartKey] = restart;
        context.Items[StateKey] = state;
        context.Properties[PipelineContextKeys.SampleRecorder] = samples;

        var services = new ServiceCollection();
        services.AddNPipeline(typeof(LineageItemContextTests).Assembly);
        services.AddNPipelineLineage();
        await using var provider = services.BuildServiceProvider();
        await provider.GetRequiredService<IPipelineRunner>().RunAsync<ContextPipeline>(context);

        state.Restarted.Should().Be(restart);

        // Only a failure that ends the item's processing records an error sample: here, the one that restarts the node.
        samples.Errors.Should().HaveCount(restart ? 1 : 0);
        samples.Errors.Should().OnlyContain(e => e.NodeId == "transform" && e.CorrelationId != Guid.Empty
                                                 && e.ExceptionType == typeof(InvalidOperationException).FullName);

        var transformRecords = lineage.Records.Where(static r => r.NodeId == "transform").ToList();

        // The skipped item ends at the transform: its terminal hop is recorded there, with its own input as the data.
        var skipped = transformRecords.Should().ContainSingle(static r => r.OutcomeReason == LineageOutcomeReason.FilteredOut).Which;
        skipped.IsTerminal.Should().BeTrue();
        skipped.Data.Should().Be(SkippedItem);

        // An unordered strategy's restart can deliver an item twice; the duplicate starts fresh lineage.
        if (strategy == Strategy.DropOldestParallel && restart)
            return;

        // Every output carries the lineage of the item that produced it (hops carry the output, item * 10), whatever
        // the order outputs come in and whatever items were dropped before them.
        var emitted = transformRecords.Where(static r => r.OutcomeReason != LineageOutcomeReason.FilteredOut).ToList();
        emitted.Should().HaveCount(5);
        var correlationOf = emitted.ToDictionary(static r => (int)r.Data! / 10, static r => r.CorrelationId);
        correlationOf.Keys.Should().BeEquivalentTo([1, 3, 4, 5, 6]);
        correlationOf.Values.Append(skipped.CorrelationId).Should().OnlyHaveUniqueItems();

        // Each sink record continues the lineage of the transform record for the same data.
        foreach (var record in lineage.Records.Where(static r => r.NodeId == "sink"))
            record.CorrelationId.Should().Be(correlationOf[(int)record.Data! / 10]);

        // The retried item's hop carries its retry count, from the outcome recorded under its input index.
        emitted.Should().ContainSingle(static r => r.RetryCount == 2)
            .Which.CorrelationId.Should().Be(correlationOf[RetriedItem]);

        // The failure that restarted the node is correlated to the item that failed, and the item is processed again
        // after the restart with the same lineage.
        if (restart)
            samples.Errors.Single().CorrelationId.Should().Be(correlationOf[RestartItem]);
    }

    private sealed class TransformState
    {
        public ConcurrentDictionary<int, int> Attempts { get; } = new();

        public bool Restarted { get; set; }
    }

    private sealed class NumbersSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new InMemoryDataStream<int>([1, 2, 3, 4, 5, 6], "numbers");
        }
    }

    private sealed class FlakyTransform : TransformNode<int, int>
    {
        public override async ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            var state = (TransformState)context.Items[StateKey];
            var restart = (bool)context.Items[RestartKey];
            var attempt = state.Attempts.AddOrUpdate(item, 1, static (_, n) => n + 1);

            // Give parallel workers a chance to interleave.
            await Task.Yield();

            if (item == RetriedItem && attempt <= 2)
                throw new TimeoutException("transient");

            if (item == SkippedItem)
                throw new ArgumentException("permanent");

            if (restart && item == RestartItem && !state.Restarted)
            {
                state.Restarted = true;
                throw new InvalidOperationException("restart the node");
            }

            return item * 10;
        }
    }

    /// <summary>
    ///     Skips the item that always fails, and fails for the one that restarts the node.
    /// </summary>
    private sealed class SkipArgumentExceptions : ResiliencePolicyBase
    {
        public override ValueTask<ResilienceDecision> DecideItemFailureAsync<TIn>(ItemFailure<TIn> failure, CancellationToken cancellationToken)
        {
            if (failure.Exception is ArgumentException)
                return ValueTask.FromResult(ResilienceDecision.Skip);

            return base.DecideItemFailureAsync(failure, cancellationToken);
        }
    }

    private sealed class DrainSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
        }
    }

    private sealed class ContextPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            builder.EnableItemLevelLineage(o => o with { SampleEvery = 1, RedactData = false });
            builder.AddLineageSink((ILineageSink)context.Items[LineageSinkKey]);

            var source = builder.AddSource<NumbersSource, int>("source");
            var transform = builder.AddTransform<FlakyTransform, int, int>("transform");

            _ = (Strategy)context.Items[StrategyKey] switch
            {
                Strategy.BlockingParallel => transform.WithBlockingParallelism(builder, maxDegreeOfParallelism: 3, maxQueueLength: 16),
                Strategy.DropOldestParallel => transform.WithDropOldestParallelism(builder, maxDegreeOfParallelism: 3, maxQueueLength: 64),
                _ => transform,
            };

            var restart = (bool)context.Items[RestartKey];

            builder.WithResilience(transform, o => o with
            {
                ItemRetry = new ItemRetryOptions { MaxRetries = 3 },
                NodeRestart = restart ? new NodeRestartOptions { MaxRestarts = 1, Backoff = RetryBackoff.None } : NodeRestartOptions.None,
            });

            builder.AddResiliencePolicy(transform, new SkipArgumentExceptions());
            var sink = builder.AddSink<DrainSink, int>("sink");

            builder.Connect(source, transform).Connect(transform, sink);
        }
    }

    private sealed class CollectingLineageSink : ILineageSink
    {
        private readonly ConcurrentQueue<LineageRecord> _records = new();

        public IReadOnlyList<LineageRecord> Records => [.. _records];

        public Task RecordAsync(LineageRecord record, CancellationToken cancellationToken)
        {
            _records.Enqueue(record);
            return Task.CompletedTask;
        }
    }

    private sealed record SampledError(string NodeId, Guid CorrelationId, string ExceptionType);

    private sealed class CollectingSampleRecorder : IPipelineSampleRecorder
    {
        private readonly ConcurrentQueue<SampledError> _errors = new();

        public IReadOnlyList<SampledError> Errors => [.. _errors];

        public void RecordSample(string nodeId, string direction, Guid correlationId, int[]? ancestryInputIndices, object? serializedRecord,
            DateTimeOffset timestamp, string? pipelineName = null, Guid? runId = null, SampleOutcome outcome = SampleOutcome.Success, int retryCount = 0)
        {
        }

        public void RecordError(string nodeId, string originNodeId, Guid correlationId, int[]? ancestryInputIndices, object? serializedRecord,
            string errorMessage, string? exceptionType, string? stackTrace, int retryCount = 0, string? pipelineName = null, Guid? runId = null,
            DateTimeOffset timestamp = default)
        {
            _errors.Enqueue(new SampledError(nodeId, correlationId, exceptionType ?? string.Empty));
        }
    }
}
