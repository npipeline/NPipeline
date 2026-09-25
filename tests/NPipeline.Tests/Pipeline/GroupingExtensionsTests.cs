using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Pipeline;

/// <summary>
///     Unit tests for the intent-driven grouping API (GroupingExtensions).
/// </summary>
public sealed class GroupingExtensionsTests
{
    #region Test Data Models

    private sealed record TestData(string Key, int Value, DateTimeOffset Timestamp);

    #endregion

    #region ForOperationalEfficiency Tests

    [Fact]
    public void ForOperationalEfficiency_ReturnsTransformNodeHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.GroupItems<int>()
            .ForOperationalEfficiency(100, TimeSpan.FromSeconds(10), "test-batcher");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("test-batcher");
    }

    [Fact]
    public void ForOperationalEfficiency_WithCustomName_UsesProvidedName()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.GroupItems<int>()
            .ForOperationalEfficiency(50, TimeSpan.FromSeconds(1), "my-batcher");

        // Assert
        handle.Id.Should().Be("my-batcher");
    }

    [Fact]
    public void ForOperationalEfficiency_WithoutCustomName_GeneratesDefaultName()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.GroupItems<int>()
            .ForOperationalEfficiency(50, TimeSpan.FromSeconds(5));

        // Assert
        handle.Id.Should().StartWith("batch-50x5");
    }

    #endregion

    #region ForTemporalCorrectness Tests

    [Fact]
    public void ForTemporalCorrectness_ReturnsAggregateNodeHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.GroupItems<TestData>()
            .ForTemporalCorrectness<string, int>(
                TimeSpan.FromMinutes(5),
                d => d.Key,
                () => 0,
                (sum, d) => sum + d.Value,
                name: "test-aggregator");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("test-aggregator");
    }

    [Fact]
    public void ForTemporalCorrectness_WithCustomName_UsesProvidedName()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.GroupItems<TestData>()
            .ForTemporalCorrectness<string, int>(
                TimeSpan.FromHours(1),
                d => d.Key,
                () => 0,
                (sum, d) => sum + d.Value,
                name: "hourly-stats");

        // Assert
        handle.Id.Should().Be("hourly-stats");
    }

    [Fact]
    public void ForTemporalCorrectness_WithoutCustomName_GeneratesDefaultName()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.GroupItems<TestData>()
            .ForTemporalCorrectness<string, int>(
                TimeSpan.FromMinutes(30),
                d => d.Key,
                () => 0,
                (sum, d) => sum + d.Value);

        // Assert
        handle.Id.Should().StartWith("aggregate-1800");
    }

    [Fact]
    public void ForTemporalCorrectness_WithTimestampExtractor_CreatesNodeSuccessfully()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.GroupItems<TestData>()
            .ForTemporalCorrectness<string, int>(
                TimeSpan.FromMinutes(5),
                d => d.Key,
                () => 0,
                (sum, d) => sum + d.Value,
                d => d.Timestamp,
                "timestamped-agg");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("timestamped-agg");
    }

    [Fact]
    public async Task ForTemporalCorrectness_WithoutExtractor_OnPlainRecord_DoesNotThrow()
    {
        // C02: the documented arrival-time fallback. A plain record without ITimestamped and without an
        // extractor must aggregate without throwing.
        var items = new List<TestData>
        {
            new("a", 1, DateTimeOffset.UtcNow),
            new("a", 2, DateTimeOffset.UtcNow),
            new("b", 3, DateTimeOffset.UtcNow),
        };

        var results = new List<int>();

        var definition = new InlinePipelineDefinition(b =>
        {
            var source = b.AddSource<InlineSource, TestData>("source");
            _ = b.AddPreconfiguredNodeInstance(source.Id, new InlineSource(items));
            var aggregate = b.GroupItems<TestData>()
                .ForTemporalCorrectness<string, int>(
                    TimeSpan.FromMinutes(5),
                    d => d.Key,
                    () => 0,
                    (sum, d) => sum + d.Value,
                    name: "agg");
            var sink = b.AddSink<CollectingSink, int>("sink");
            _ = b.AddPreconfiguredNodeInstance(sink.Id, new CollectingSink(results));
            _ = b.Connect(source, aggregate).Connect(aggregate, sink);
        });

        var act = () => PipelineRunner.Create().RunAsync(definition, new PipelineContext());

        await act.Should().NotThrowAsync();
        // Arrival time puts all three items in the same window: "a" sums to 3 and "b" to 3.
        results.Should().Equal(3, 3);
    }

    private sealed class InlinePipelineDefinition(Action<PipelineBuilder> define) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context) => define(builder);
    }

    private sealed class InlineSource(List<TestData> items) : SourceNode<TestData>
    {
        public override IDataStream<TestData> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new DataStream<TestData>(items.ToAsyncEnumerable(), "Inline");
    }

    private sealed class CollectingSink(List<int> results) : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                results.Add(item);
            }
        }
    }

    #endregion

    #region ForRollingWindow Tests

    [Fact]
    public void ForRollingWindow_ReturnsAggregateNodeHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.GroupItems<TestData>()
            .ForRollingWindow<string, int>(
                TimeSpan.FromMinutes(15),
                TimeSpan.FromMinutes(5),
                d => d.Key,
                () => 0,
                (sum, d) => sum + d.Value,
                name: "rolling-agg");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("rolling-agg");
    }

    [Fact]
    public void ForRollingWindow_WithCustomName_UsesProvidedName()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.GroupItems<TestData>()
            .ForRollingWindow<string, int>(
                TimeSpan.FromMinutes(15),
                TimeSpan.FromMinutes(5),
                d => d.Key,
                () => 0,
                (sum, d) => sum + d.Value,
                name: "sliding-counter");

        // Assert
        handle.Id.Should().Be("sliding-counter");
    }

    [Fact]
    public void ForRollingWindow_WithoutCustomName_GeneratesDefaultName()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.GroupItems<TestData>()
            .ForRollingWindow<string, int>(
                TimeSpan.FromMinutes(15),
                TimeSpan.FromMinutes(5),
                d => d.Key,
                () => 0,
                (sum, d) => sum + d.Value);

        // Assert
        handle.Id.Should().StartWith("sliding-900s-by-300");
    }

    #endregion
}
