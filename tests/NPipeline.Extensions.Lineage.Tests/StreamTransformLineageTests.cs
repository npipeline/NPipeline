using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution.Lineage;
using NPipeline.Execution.Strategies;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Lineage;
using NPipeline.Lineage.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Lineage.Tests;

/// <summary>
///     A filter and a select-many change how many outputs each input has, so their lineage must be mapped by which input
///     each output came from, not by position.
/// </summary>
public sealed class StreamTransformLineageTests
{
    private const string LineageSinkKey = "test.lineage.sink";
    private const string ProbeKey = "test.probe";

    [Fact]
    public async Task Filter_KeepsEachItemsLineage_AndRecordsWhatItDrops()
    {
        var lineage = await RunAsync<FilterPipeline>();

        // The transform before the filter tags each item's lineage with its data (item * 10).
        var correlationOf = lineage.Where(static r => r.NodeId == "tag").ToDictionary(static r => (int)r.Data!, static r => r.CorrelationId);
        correlationOf.Keys.Should().BeEquivalentTo([10, 20, 30, 40, 50, 60]);

        var filterRecords = lineage.Where(static r => r.NodeId == "filter").ToList();

        // Kept items continue their own lineage.
        filterRecords.Where(static r => r.OutcomeReason == LineageOutcomeReason.Emitted)
            .Select(static r => ((int)r.Data!, r.CorrelationId))
            .Should().BeEquivalentTo([(20, correlationOf[20]), (40, correlationOf[40]), (60, correlationOf[60])]);

        // Dropped items end at the filter, each with its own lineage.
        var dropped = filterRecords.Where(static r => r.OutcomeReason == LineageOutcomeReason.FilteredOut).ToList();
        dropped.Should().OnlyContain(static r => r.IsTerminal);

        dropped.Select(static r => ((int)r.Data!, r.CorrelationId))
            .Should().BeEquivalentTo([(10, correlationOf[10]), (30, correlationOf[30]), (50, correlationOf[50])]);

        lineage.Where(static r => r.NodeId == "sink").Select(static r => ((int)r.Data!, r.CorrelationId))
            .Should().BeEquivalentTo([(20, correlationOf[20]), (40, correlationOf[40]), (60, correlationOf[60])]);
    }

    [Fact]
    public async Task SelectMany_GivesEveryOutputItsInputsLineage()
    {
        var lineage = await RunAsync<SelectManyPipeline>();

        var correlationOf = lineage.Where(static r => r.NodeId == "tag").ToDictionary(static r => (int)r.Data!, static r => r.CorrelationId);

        // Item n * 10 expands into n copies of itself; the item 0 expands into nothing.
        lineage.Where(static r => r.NodeId == "expand" && r.OutcomeReason == LineageOutcomeReason.Emitted)
            .Select(static r => ((int)r.Data!, r.CorrelationId))
            .Should().BeEquivalentTo([
                (10, correlationOf[10]), (20, correlationOf[20]), (20, correlationOf[20]),
                (30, correlationOf[30]), (30, correlationOf[30]), (30, correlationOf[30]),
            ]);

        lineage.Should().ContainSingle(static r => r.NodeId == "expand" && r.OutcomeReason == LineageOutcomeReason.ConsumedWithoutEmission)
            .Which.CorrelationId.Should().Be(correlationOf[0]);
    }

    [Fact]
    public async Task ThrowingSink_DropsTheRunsLineageState()
    {
        // Arrange
        var sink = new CollectingLineageSink();
        var context = new PipelineContext();
        context.Items[LineageSinkKey] = sink;

        var services = new ServiceCollection();
        services.AddNPipeline(typeof(StreamTransformLineageTests).Assembly);
        services.AddNPipelineLineage();
        await using var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredService<IPipelineRunner>();

        // Act - the sink fails before its node's output is ever enumerated.
        var act = async () => await runner.RunAsync<ThrowingSinkPipeline>(context);
        _ = await act.Should().ThrowAsync<Exception>();

        // Assert - a node whose output was never pulled left no state behind for this run.
        context.Lineage.Outcomes.IsTracking("tag").Should().BeFalse();
    }

    [Fact]
    public async Task ReusedContext_StartsEachRunWithItsOwnLineageState()
    {
        // Arrange - the lineage state lives on the run, not in a process-wide registry keyed by pipeline id.
        var context = new PipelineContext();
        context.Items[LineageSinkKey] = new CollectingLineageSink();

        var services = new ServiceCollection();
        services.AddNPipeline(typeof(StreamTransformLineageTests).Assembly);
        services.AddNPipelineLineage();
        await using var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredService<IPipelineRunner>();

        // Act
        await runner.RunAsync<FilterPipeline>(context);
        var firstRun = context.Lineage.Outcomes;
        await runner.RunAsync<FilterPipeline>(context);

        // Assert
        context.Lineage.Outcomes.Should().NotBeSameAs(firstRun);
        firstRun.IsTracking("filter").Should().BeFalse("the first run released its state when it ended");
    }

    [Fact]
    public async Task MapperTransform_LeavesNoQueuedProvenance_WhenTheStrategyCannotDrainIt()
    {
        // Arrange
        var context = new PipelineContext();
        context.Items[LineageSinkKey] = new CollectingLineageSink();
        var probe = new ProbeState();
        context.Items[ProbeKey] = probe;

        var services = new ServiceCollection();
        services.AddNPipeline(typeof(StreamTransformLineageTests).Assembly);
        services.AddNPipelineLineage();
        await using var provider = services.BuildServiceProvider();

        // Act
        await provider.GetRequiredService<IPipelineRunner>().RunAsync<MapperPipeline>(context);

        // Assert - the mapper path never dequeues provenance reports, so the transform must not have queued any.
        _ = probe.Sampled.Should().BeTrue();
        _ = probe.PendingProvenanceAtEnd.Should().Be(0);
    }

    [Fact]
    public async Task StreamingOneToOne_ReleasesEachItemsLineageAsItIsMapped()
    {
        // Arrange
        var context = new PipelineContext();
        context.Items[LineageSinkKey] = new CollectingLineageSink();
        var probe = new ProbeState();
        context.Items[ProbeKey] = probe;

        var services = new ServiceCollection();
        services.AddNPipeline(typeof(StreamTransformLineageTests).Assembly);
        services.AddNPipelineLineage();
        await using var provider = services.BuildServiceProvider();

        // Act - the probe sink samples the transform's tracked inputs once it has read every output, before cleanup.
        await provider.GetRequiredService<IPipelineRunner>().RunAsync<ProbedPipeline>(context);

        // Assert - the first item's lineage was released as it was mapped, so the transform's state stayed bounded by
        // the items in flight rather than holding every item for the whole run.
        _ = probe.Sampled.Should().BeTrue();
        _ = probe.FirstItemTracked.Should().BeFalse();
    }

    [Fact]
    public async Task SlowSink_KeepsTheLineageAdapterFromReadingTheWholeUpstream()
    {
        // Arrange - the sink reads one item, then pauses while the source could run far ahead.
        var context = new PipelineContext();
        context.Items[LineageSinkKey] = new CollectingLineageSink();
        var counter = new ProducedCounter();
        context.Items[ProducedCounter.Key] = counter;

        var services = new ServiceCollection();
        services.AddNPipeline(typeof(StreamTransformLineageTests).Assembly);
        services.AddNPipelineLineage();
        await using var provider = services.BuildServiceProvider();

        // Act
        await provider.GetRequiredService<IPipelineRunner>().RunAsync<BackpressurePipeline>(context);

        // Assert - a bounded adapter lets the source run only a few buffers ahead of the pausing sink.
        _ = counter.ProducedDuringPause.Should().BeLessThan(1_000, "the lineage adapter must apply backpressure");
        _ = counter.Produced.Should().Be(BackpressurePipeline.ItemCount);
    }

    private static async Task<IReadOnlyList<LineageRecord>> RunAsync<TPipeline>()
        where TPipeline : IPipelineDefinition, new()
    {
        var sink = new CollectingLineageSink();
        var context = new PipelineContext();
        context.Items[LineageSinkKey] = sink;

        var services = new ServiceCollection();
        services.AddNPipeline(typeof(StreamTransformLineageTests).Assembly);
        services.AddNPipelineLineage();
        await using var provider = services.BuildServiceProvider();
        await provider.GetRequiredService<IPipelineRunner>().RunAsync<TPipeline>(context);

        return sink.Records;
    }

    private static void EnableLineage(PipelineBuilder builder, PipelineContext context)
    {
        builder.EnableItemLevelLineage(o => o with { SampleEvery = 1, RedactData = false });
        builder.AddLineageSink((ILineageSink)context.Items[LineageSinkKey]);
    }

    private sealed class FilterPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            EnableLineage(builder, context);

            var source = builder.AddSource<NumbersSource, int>("source");
            var tag = builder.AddTransform<TimesTen, int, int>("tag");
            var filter = builder.AddFilter((int v) => v % 20 == 0, "filter");
            var sink = builder.AddSink<DrainSink, int>("sink");

            builder.Connect(source, tag).Connect(tag, filter).Connect(filter, sink);
        }
    }

    private sealed class SelectManyPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            EnableLineage(builder, context);

            var source = builder.AddSource<SmallNumbersSource, int>("source");
            var tag = builder.AddTransform<TimesTen, int, int>("tag");
            var expand = builder.AddSelectMany((int v) => Enumerable.Repeat(v, v / 10), "expand");
            var sink = builder.AddSink<DrainSink, int>("sink");

            builder.Connect(source, tag).Connect(tag, expand).Connect(expand, sink);
        }
    }

    private sealed class ThrowingSinkPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            EnableLineage(builder, context);

            var source = builder.AddSource<NumbersSource, int>("source");
            var tag = builder.AddTransform<TimesTen, int, int>("tag");
            var sink = builder.AddSink<ThrowingSink, int>("sink");

            builder.Connect(source, tag).Connect(tag, sink);
        }
    }

    private sealed class ProbedPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            EnableLineage(builder, context);

            var source = builder.AddSource<ManyNumbersSource, int>("source");

            // A stream passthrough that does not report provenance uses the streaming 1:1 lineage mapping.
            var tag = builder.AddStreamTransform<StreamTimesTen, int, int>("tag");
            tag.WithExecutionStrategy(builder, StreamPassthroughExecutionStrategy.Instance);
            var sink = builder.AddSink<ProbeSink, int>("sink");

            builder.Connect(source, tag).Connect(tag, sink);
        }
    }

    private sealed class MapperPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            EnableLineage(builder, context);

            var source = builder.AddSource<NumbersSource, int>("source");

            // A per-item transform reports provenance, but a declared mapper wins over it, so the reports are never
            // dequeued and must not be queued at all.
            var tag = builder.AddTransform<MappedTimesTen, int, int>("tag");
            var sink = builder.AddSink<ProbeSink, int>("sink");

            builder.Connect(source, tag).Connect(tag, sink);
        }
    }

    private sealed class BackpressurePipeline : IPipelineDefinition
    {
        public const int ItemCount = 20_000;

        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            builder.EnableItemLevelLineage(o => o with { SampleEvery = 100 });
            builder.AddLineageSink((ILineageSink)context.Items[LineageSinkKey]);

            var source = builder.AddSource<CountingSource, int>("source");
            var tag = builder.AddTransform<TimesTen, int, int>("tag");
            var sink = builder.AddSink<PausingSink, int>("sink");

            builder.Connect(source, tag).Connect(tag, sink);
        }
    }

    private sealed class ProducedCounter
    {
        public const string Key = "test.produced";

        private int _produced;

        public int Produced => Volatile.Read(ref _produced);

        public int ProducedDuringPause { get; set; }

        public void Increment() => Interlocked.Increment(ref _produced);
    }

    private sealed class CountingSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new DataStream<int>(Produce((ProducedCounter)context.Items[ProducedCounter.Key], cancellationToken), "counting");

        private static async IAsyncEnumerable<int> Produce(ProducedCounter counter, [EnumeratorCancellation] CancellationToken ct)
        {
            for (var i = 0; i < BackpressurePipeline.ItemCount; i++)
            {
                ct.ThrowIfCancellationRequested();
                counter.Increment();

                if (i % 256 == 0)
                    await Task.Yield();

                yield return i;
            }
        }
    }

    private sealed class PausingSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            var counter = (ProducedCounter)context.Items[ProducedCounter.Key];
            var first = true;

            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                if (!first)
                    continue;

                first = false;
                await Task.Delay(300, cancellationToken);
                counter.ProducedDuringPause = counter.Produced;
            }
        }
    }

    private sealed class NumbersSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new InMemoryDataStream<int>([1, 2, 3, 4, 5, 6], "numbers");
    }

    private sealed class SmallNumbersSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new InMemoryDataStream<int>([1, 0, 2, 3], "numbers");
    }

    private sealed class ManyNumbersSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new InMemoryDataStream<int>(Enumerable.Range(0, 1000).ToList(), "numbers");
    }

    private sealed class TimesTen : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(item * 10);
    }

    private sealed class StreamTimesTen : IStreamTransformNode<int, int>
    {
        public async IAsyncEnumerable<int> TransformAsync(IAsyncEnumerable<int> items, PipelineContext context,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var item in items.WithCancellation(cancellationToken))
                yield return item * 10;
        }
    }

    [LineageMapper(typeof(PositionalMapper))]
    private sealed class MappedTimesTen : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) =>
            ValueTask.FromResult(item * 10);
    }

    private sealed class PositionalMapper : ILineageMapper
    {
        public LineageMappingResult MapInputToOutputs(IReadOnlyList<object> inputPackets, IReadOnlyList<object> outputs, LineageMappingContext context)
        {
            var records = new List<LineageMappingRecord>(outputs.Count);

            for (var i = 0; i < outputs.Count; i++)
                records.Add(new LineageMappingRecord(i, i < inputPackets.Count ? [i] : []));

            return new LineageMappingResult(records);
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

    private sealed class ThrowingSink : SinkNode<int>
    {
        public override Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("sink failed before reading");
    }

    /// <summary>
    ///     Samples the transform's tracked state mid-stream, while the run is still live and most items are unmapped.
    /// </summary>
    private sealed class ProbeSink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            var state = (ProbeState)context.Items[ProbeKey];
            var writer = context.Lineage.Outcomes.GetWriter("tag");
            var read = 0;

            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                _ = item;
                read++;

                // Once a later item is being read, the transform has already mapped and released the first one.
                if (read == 10)
                    state.FirstItemTracked = writer.TryGetInput(0, out _);
            }

            state.PendingProvenanceAtEnd = writer.PendingProvenanceCount;
            state.Sampled = true;
        }
    }

    private sealed class ProbeState
    {
        public bool Sampled { get; set; }
        public bool FirstItemTracked { get; set; }
        public int PendingProvenanceAtEnd { get; set; }
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
}
