using System.Collections.Concurrent;
using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.Attributes.Nodes;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.DataFlow.Windowing;
using NPipeline.Extensions.DependencyInjection;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Join;

public sealed class TimeWindowedJoinNodeTests
{
    [Fact]
    public async Task TimeWindowedJoinNode_WithTumblingWindow_ShouldJoinItemsInSameWindow()
    {
        // Arrange
        var services = new ServiceCollection();
        services.AddSingleton<ConcurrentQueue<EnrichedEvent>>();
        services.AddNPipeline(typeof(TimeWindowedJoinNodeTests).Assembly);
        var provider = services.BuildServiceProvider();

        var runner = provider.GetRequiredService<IPipelineRunner>();
        var context = PipelineContext.Default;

        // Act
        await runner.RunAsync<TumblingWindowJoinPipeline>(context);

        // Assert
        var resultStore = provider.GetRequiredService<ConcurrentQueue<EnrichedEvent>>();
        resultStore.Should().HaveCount(2);

        resultStore.Should().BeEquivalentTo([
            new EnrichedEvent(1, "Event1", "Metadata1", new DateTime(2023, 1, 1, 10, 0, 15)),
            new EnrichedEvent(2, "Event2", "Metadata2", new DateTime(2023, 1, 1, 10, 1, 30)),
        ]);
    }

    // Test Data Models
    private sealed record Event(int Id, string Name, DateTime EventTimestamp) : ITimestamped
    {
        public DateTimeOffset Timestamp => EventTimestamp;
    }

    private sealed record EventMetadata(int Id, string Metadata, DateTime MetadataTimestamp) : ITimestamped
    {
        public DateTimeOffset Timestamp => MetadataTimestamp;
    }

    private sealed record EnrichedEvent(int Id, string? Name, string? Metadata, DateTime Timestamp);

    // Test Node Implementations

    private sealed class EventSource : SourceNode<Event>
    {
        public override IDataPipe<Event> Execute(PipelineContext context, CancellationToken cancellationToken)
        {
            var events = new[]
            {
                new Event(1, "Event1", new DateTime(2023, 1, 1, 10, 0, 15)),
                new Event(2, "Event2", new DateTime(2023, 1, 1, 10, 1, 30)),
                new Event(3, "Event3", new DateTime(2023, 1, 1, 10, 2, 45)), // Outside window
            };

            return new StreamingDataPipe<Event>(events.ToAsyncEnumerable(), "EventStream");
        }
    }

    private sealed class EventMetadataSource : SourceNode<EventMetadata>
    {
        public override IDataPipe<EventMetadata> Execute(PipelineContext context, CancellationToken cancellationToken)
        {
            var metadata = new[]
            {
                new EventMetadata(1, "Metadata1", new DateTime(2023, 1, 1, 10, 0, 10)),
                new EventMetadata(2, "Metadata2", new DateTime(2023, 1, 1, 10, 1, 20)),
                new EventMetadata(4, "Metadata4", new DateTime(2023, 1, 1, 10, 3, 0)), // Outside window and no matching event
            };

            return new StreamingDataPipe<EventMetadata>(metadata.ToAsyncEnumerable(), "MetadataStream");
        }
    }

    [KeySelector(typeof(Event), nameof(Event.Id))]
    [KeySelector(typeof(EventMetadata), nameof(EventMetadata.Id))]
    private sealed class EventEnrichmentNode()
        : TimeWindowedJoinNode<int, Event, EventMetadata, EnrichedEvent>(new TumblingWindowAssigner(TimeSpan.FromMinutes(1)))
    {
        public override EnrichedEvent CreateOutput(Event item1, EventMetadata item2)
        {
            return new EnrichedEvent(item1.Id, item1.Name, item2.Metadata, item1.EventTimestamp);
        }

        public override EnrichedEvent CreateOutputFromLeft(Event item1)
        {
            return new EnrichedEvent(item1.Id, item1.Name, null, item1.EventTimestamp);
        }

        public override EnrichedEvent CreateOutputFromRight(EventMetadata item2)
        {
            return new EnrichedEvent(item2.Id, null, item2.Metadata, item2.MetadataTimestamp);
        }
    }

    private sealed class EnrichedEventSink(ConcurrentQueue<EnrichedEvent> store) : SinkNode<EnrichedEvent>
    {
        public override async Task ExecuteAsync(IDataPipe<EnrichedEvent> input, PipelineContext context,
            CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                store.Enqueue(item);
            }
        }
    }

    // Test Definition

    private sealed class TumblingWindowJoinPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var eventSource = builder.AddSource<EventSource, Event>("event_source");
            var metadataSource = builder.AddSource<EventMetadataSource, EventMetadata>("metadata_source");
            var enrichmentNode = builder.AddJoin<EventEnrichmentNode, Event, EventMetadata, EnrichedEvent>("enrichment_node");
            var sink = builder.AddSink<EnrichedEventSink, EnrichedEvent>("sink");

            builder.Connect(eventSource, enrichmentNode);
            builder.Connect(metadataSource, enrichmentNode);
            builder.Connect(enrichmentNode, sink);
        }
    }
}
