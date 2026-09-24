using System.Collections.Concurrent;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
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
            .Should().BeEquivalentTo([(10, correlationOf[10]), (20, correlationOf[20]), (20, correlationOf[20]),
                (30, correlationOf[30]), (30, correlationOf[30]), (30, correlationOf[30])]);

        lineage.Should().ContainSingle(static r => r.NodeId == "expand" && r.OutcomeReason == LineageOutcomeReason.ConsumedWithoutEmission)
            .Which.CorrelationId.Should().Be(correlationOf[0]);
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

    private sealed class NumbersSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new InMemoryDataStream<int>([1, 2, 3, 4, 5, 6], "numbers");
        }
    }

    private sealed class SmallNumbersSource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new InMemoryDataStream<int>([1, 0, 2, 3], "numbers");
        }
    }

    private sealed class TimesTen : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult(item * 10);
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
