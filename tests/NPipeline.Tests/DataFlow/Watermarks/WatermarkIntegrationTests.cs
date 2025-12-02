using System.Collections.Concurrent;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.DataFlow.Windowing;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.DataFlow.Watermarks;

public sealed class WatermarkIntegrationTests
{
    [Fact]
    public async Task AggregateNode_WithWatermarks_ShouldEmitWindowFinalResults()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<double>>();
        services.AddNPipeline(typeof(WatermarkIntegrationTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<WatermarkAggregationPipeline>(context);

        // Assert - Should only get final window results, not per-item results
        var resultStore = provider.GetRequiredService<ConcurrentQueue<double>>();
        resultStore.Should().HaveCount(2); // Two windows: 10:00-10:05 and 10:05-10:10
        resultStore.Should().BeEquivalentTo([15.0, 25.0]); // Averages of each window
    }

    // Test Data Models
    public sealed record SensorReading(string SensorId, double Value, DateTime ReadingTime) : ITimestamped
    {
        public DateTimeOffset Timestamp => ReadingTime;
    }

    // Test Node Implementation
    private sealed class SensorAverageNode()
        : AdvancedAggregateNode<SensorReading, string, (double sum, int count), double>(WindowAssigner.Tumbling(TimeSpan.FromMinutes(5)))
    {
        public override string GetKey(SensorReading item)
        {
            return item.SensorId;
        }

        public override (double, int) CreateAccumulator()
        {
            return (0, 0);
        }

        public override (double, int) Accumulate((double, int) acc, SensorReading item)
        {
            return (acc.Item1 + item.Value, acc.Item2 + 1);
        }

        public override double GetResult((double, int) acc)
        {
            return acc.Item1 / acc.Item2;
        }
    }

    private sealed class SensorSource : SourceNode<SensorReading>
    {
        public override IDataPipe<SensorReading> Execute(PipelineContext context, CancellationToken cancellationToken)
        {
            var readings = new[]
            {
                new SensorReading("sensor1", 10.0, new DateTime(2023, 1, 1, 10, 0, 0)),
                new SensorReading("sensor1", 20.0, new DateTime(2023, 1, 1, 10, 1, 0)), // Window 1: average 15
                new SensorReading("sensor1", 20.0, new DateTime(2023, 1, 1, 10, 5, 0)), // Window 2
                new SensorReading("sensor1", 30.0, new DateTime(2023, 1, 1, 10, 6, 0)), // Window 2: average 25
            };

            return new StreamingDataPipe<SensorReading>(readings.ToAsyncEnumerable(), "SensorStream");
        }
    }

    private sealed class AverageSink(ConcurrentQueue<double> store) : SinkNode<double>
    {
        public override async Task ExecuteAsync(IDataPipe<double> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                store.Enqueue(item);
            }
        }
    }

    // Test Definition
    private sealed class WatermarkAggregationPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<SensorSource, SensorReading>("sensor_source");
            var aggregateNode = builder.AddAggregate<SensorAverageNode, SensorReading, string, (double, int), double>("aggregate_node");
            var sink = builder.AddSink<AverageSink, double>("sink");

            builder.Connect(source, aggregateNode);
            builder.Connect(aggregateNode, sink);
        }
    }
}
