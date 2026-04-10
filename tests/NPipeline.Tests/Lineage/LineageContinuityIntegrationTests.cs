using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Extensions.Parallelism;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using System.Runtime.CompilerServices;

namespace NPipeline.Tests.Lineage;

public sealed class LineageContinuityIntegrationTests
{
    private const string LineageSinkContextKey = "testing.lineage.sink";

    [Fact]
    public async Task AggregateLineage_ShouldRemainContinuousAcrossTransformAndAggregate()
    {
        var (context, sink) = CreateContext();

        await RunPipelineAsync<AggregateContinuityPipeline>(context);

        var records = sink.Records;
        records.Should().NotBeEmpty();

        var sourceSegment = Qualified(context, "source");
        var transformSegment = Qualified(context, "transform");
        var aggregateSegment = Qualified(context, "aggregate");

        records.Should().OnlyContain(r =>
            r.TraversalPath.Contains(sourceSegment) &&
            r.TraversalPath.Contains(transformSegment) &&
            r.TraversalPath.Contains(aggregateSegment));

        records.Should().OnlyContain(r =>
            r.LineageHops.Any(h =>
                h.NodeId == "aggregate" &&
                h.Outcome.HasFlag(HopDecisionFlags.Aggregated) &&
                h.OutputEmissionCount == 1));
    }

    [Fact]
    public async Task JoinLineage_ShouldRetainContributorMetadataAndUpstreamPath()
    {
        var (context, sink) = CreateContext();

        await RunPipelineAsync<JoinContinuityPipeline>(context);

        var records = sink.Records;
        records.Should().NotBeEmpty();

        var leftTransformSegment = Qualified(context, "left_transform");
        var rightTransformSegment = Qualified(context, "right_transform");
        var joinSegment = Qualified(context, "join");

        records.Should().OnlyContain(r =>
            r.TraversalPath.Contains(leftTransformSegment) &&
            r.TraversalPath.Contains(rightTransformSegment) &&
            r.TraversalPath.Contains(joinSegment));

        records.Should().OnlyContain(r =>
            r.LineageHops.Any(h =>
                h.NodeId == "join" &&
                h.Outcome.HasFlag(HopDecisionFlags.Joined) &&
                h.InputContributorCount.HasValue &&
                h.InputContributorCount.Value > 1 &&
                h.OutputEmissionCount == 1));
    }

    [Fact]
    public async Task ParallelTransformThenAggregate_ShouldPreserveLineagePath()
    {
        var (context, sink) = CreateContext();

        await RunPipelineAsync<ParallelThenAggregatePipeline>(context);

        var records = sink.Records;
        records.Should().NotBeEmpty();

        var sourceSegment = Qualified(context, "source");
        var parallelSegment = Qualified(context, "parallel_transform");
        var aggregateSegment = Qualified(context, "aggregate");

        records.Should().OnlyContain(r =>
            r.TraversalPath.Contains(sourceSegment) &&
            r.TraversalPath.Contains(parallelSegment) &&
            r.TraversalPath.Contains(aggregateSegment));
    }

    [Fact]
    public async Task Sampling_ShouldKeepContinuousTraversalOnCollectedItems()
    {
        var (context, sink) = CreateContext(new LineageOptions(SampleEvery: 4, DeterministicSampling: true, RedactData: false));

        await RunPipelineAsync<SamplingCompatibilityPipeline>(context);

        var records = sink.Records;
        records.Count.Should().BeGreaterThan(0);

        var sourceSegment = Qualified(context, "source");
        var transformSegment = Qualified(context, "transform");
        var aggregateSegment = Qualified(context, "aggregate");

        records.Should().OnlyContain(r =>
            r.TraversalPath.Contains(sourceSegment) &&
            r.TraversalPath.Contains(transformSegment) &&
            r.TraversalPath.Contains(aggregateSegment));
    }

    [Fact]
    public async Task AggregateWithNoInputLineage_ShouldMintFreshLineage()
    {
        var (context, sink) = CreateContext();

        await RunPipelineAsync<NoInputOutputPipeline>(context);

        var records = sink.Records;
        records.Should().HaveCount(1);

        var aggregateSegment = Qualified(context, "aggregate");
        var sourceSegment = Qualified(context, "source");

        records[0].LineageId.Should().NotBe(Guid.Empty);
        records[0].TraversalPath.Should().Contain(aggregateSegment);
        records[0].TraversalPath.Should().NotContain(sourceSegment);
    }

    private static (PipelineContext Context, CollectingLineageSink Sink) CreateContext(LineageOptions? optionsOverride = null)
    {
        var sink = new CollectingLineageSink();
        var context = new PipelineContext();
        context.Items[LineageSinkContextKey] = sink;

        if (optionsOverride is not null)
            context.Properties[PipelineContextKeys.LineageOptionsOverride] = optionsOverride;

        return (context, sink);
    }

    private static string Qualified(PipelineContext context, string nodeId)
    {
        return $"{context.PipelineId:N}::{nodeId.Replace('_', '-')}";
    }

    private static async Task RunPipelineAsync<TPipeline>(PipelineContext context)
        where TPipeline : IPipelineDefinition, new()
    {
        var services = new ServiceCollection();
        services.AddNPipeline(typeof(LineageContinuityIntegrationTests).Assembly);

        await using var provider = services.BuildServiceProvider();
        var runner = provider.GetRequiredService<IPipelineRunner>();
        await runner.RunAsync<TPipeline>(context);
    }

    private sealed class CollectingLineageSink : ILineageSink
    {
        private readonly List<LineageInfo> _records = [];
        private readonly object _sync = new();

        public IReadOnlyList<LineageInfo> Records
        {
            get
            {
                lock (_sync)
                {
                    return _records.ToList();
                }
            }
        }

        public Task RecordAsync(LineageInfo lineageInfo, CancellationToken cancellationToken)
        {
            lock (_sync)
            {
                _records.Add(lineageInfo);
            }

            return Task.CompletedTask;
        }
    }

    private sealed class NumbersSourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>([1, 2, 3, 4], "numbers");
        }
    }

    private sealed class LeftSourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>([1, 2, 3], "left");
        }
    }

    private sealed class RightSourceNode : SourceNode<long>
    {
        public override IDataStream<long> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new NPipeline.DataFlow.DataStreams.InMemoryDataStream<long>([10L, 20L], "right");
        }
    }

    private sealed class SamplingSourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>(Enumerable.Range(1, 100).ToArray(), "sampling");
        }
    }

    private sealed class EmptySourceNode : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
        {
            return new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>([], "empty");
        }
    }

    private sealed class IncrementTransformNode : TransformNode<int, int>
    {
        public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item + 1);
        }
    }

    private sealed class LeftTransformNode : TransformNode<int, int>
    {
        public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item * 2);
        }
    }

    private sealed class RightTransformNode : TransformNode<long, long>
    {
        public override Task<long> TransformAsync(long item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item + 100);
        }
    }

    private sealed class ParallelTransformNode : TransformNode<int, int>
    {
        public override async Task<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(1, cancellationToken);
            return item * 3;
        }
    }

    private sealed class SumAggregateNode : IAggregateNode
    {
        public async ValueTask<object?> ExecuteAsync(IAsyncEnumerable<object?> inputStream, CancellationToken cancellationToken = default)
        {
            var sum = 0;

            await foreach (var item in inputStream.WithCancellation(cancellationToken))
            {
                if (item is int value)
                    sum += value;
            }

            return sum;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class PassThroughAggregateNode : IAggregateNode
    {
        public ValueTask<object?> ExecuteAsync(IAsyncEnumerable<object?> inputStream, CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult<object?>(inputStream);
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class EmitConstantOnEmptyAggregateNode : IAggregateNode
    {
        public async ValueTask<object?> ExecuteAsync(IAsyncEnumerable<object?> inputStream, CancellationToken cancellationToken = default)
        {
            await foreach (var _ in inputStream.WithCancellation(cancellationToken))
            {
            }

            return 999;
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class CollapsingJoinNode : IJoinNode
    {
        public ValueTask<IAsyncEnumerable<object?>> ExecuteAsync(
            IAsyncEnumerable<object?> inputStream,
            PipelineContext context,
            CancellationToken cancellationToken = default)
        {
            return ValueTask.FromResult<IAsyncEnumerable<object?>>(Execute(inputStream, cancellationToken));
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        private static async IAsyncEnumerable<object?> Execute(
            IAsyncEnumerable<object?> inputStream,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            var total = 0;

            await foreach (var item in inputStream.WithCancellation(cancellationToken))
            {
                if (item is int value)
                {
                    total += value;
                }
                else if (item is long longValue)
                {
                    total += (int)longValue;
                }
            }

            yield return total;
        }
    }

    private sealed class DrainSinkNode<T> : SinkNode<T>
    {
        public override async Task ConsumeAsync(IDataStream<T> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
            }
        }
    }

    private abstract class BaseLineagePipeline : IPipelineDefinition
    {
        protected static void EnableLineage(PipelineBuilder builder, PipelineContext context)
        {
            builder.EnableItemLevelLineage();
            builder.AddLineageSink((ILineageSink)context.Items[LineageSinkContextKey]);
        }

        public abstract void Define(PipelineBuilder builder, PipelineContext context);
    }

    private sealed class AggregateContinuityPipeline : BaseLineagePipeline
    {
        public override void Define(PipelineBuilder builder, PipelineContext context)
        {
            EnableLineage(builder, context);

            var source = builder.AddSource<NumbersSourceNode, int>("source");
            var transform = builder.AddTransform<IncrementTransformNode, int, int>("transform");
            var aggregate = builder.AddAggregate<SumAggregateNode, int, int, int>("aggregate");
            var sink = builder.AddSink<DrainSinkNode<int>, int>("sink");

            builder.Connect(source, transform)
                .Connect(transform, aggregate)
                .Connect(aggregate, sink);
        }
    }

    private sealed class JoinContinuityPipeline : BaseLineagePipeline
    {
        public override void Define(PipelineBuilder builder, PipelineContext context)
        {
            EnableLineage(builder, context);

            var leftSource = builder.AddSource<LeftSourceNode, int>("left_source");
            var rightSource = builder.AddSource<RightSourceNode, long>("right_source");
            var leftTransform = builder.AddTransform<LeftTransformNode, int, int>("left_transform");
            var rightTransform = builder.AddTransform<RightTransformNode, long, long>("right_transform");
            var join = builder.AddJoin<CollapsingJoinNode, int, long, int>("join");
            var sink = builder.AddSink<DrainSinkNode<int>, int>("sink");

            builder.Connect(leftSource, leftTransform)
                .Connect(leftTransform, join)
                .Connect(rightSource, rightTransform)
                .Connect(rightTransform, join)
                .Connect(join, sink);
        }
    }

    private sealed class ParallelThenAggregatePipeline : BaseLineagePipeline
    {
        public override void Define(PipelineBuilder builder, PipelineContext context)
        {
            EnableLineage(builder, context);

            var source = builder.AddSource<NumbersSourceNode, int>("source");
            var parallel = builder.AddTransform<ParallelTransformNode, int, int>("parallel_transform")
                .WithBlockingParallelism(builder, maxDegreeOfParallelism: 4, maxQueueLength: 32);
            var aggregate = builder.AddAggregate<PassThroughAggregateNode, int, int, int>("aggregate");
            var sink = builder.AddSink<DrainSinkNode<int>, int>("sink");

            builder.Connect(source, parallel)
                .Connect(parallel, aggregate)
                .Connect(aggregate, sink);
        }
    }

    private sealed class SamplingCompatibilityPipeline : BaseLineagePipeline
    {
        public override void Define(PipelineBuilder builder, PipelineContext context)
        {
            EnableLineage(builder, context);

            var source = builder.AddSource<SamplingSourceNode, int>("source");
            var transform = builder.AddTransform<IncrementTransformNode, int, int>("transform");
            var aggregate = builder.AddAggregate<PassThroughAggregateNode, int, int, int>("aggregate");
            var sink = builder.AddSink<DrainSinkNode<int>, int>("sink");

            builder.Connect(source, transform)
                .Connect(transform, aggregate)
                .Connect(aggregate, sink);
        }
    }

    private sealed class NoInputOutputPipeline : BaseLineagePipeline
    {
        public override void Define(PipelineBuilder builder, PipelineContext context)
        {
            EnableLineage(builder, context);

            var source = builder.AddSource<EmptySourceNode, int>("source");
            var aggregate = builder.AddAggregate<EmitConstantOnEmptyAggregateNode, int, int, int>("aggregate");
            var sink = builder.AddSink<DrainSinkNode<int>, int>("sink");

            builder.Connect(source, aggregate)
                .Connect(aggregate, sink);
        }
    }
}
