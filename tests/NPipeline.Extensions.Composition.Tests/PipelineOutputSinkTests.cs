using AwesomeAssertions;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Pipeline;
using Xunit;

namespace NPipeline.Extensions.Composition.Tests;

public class PipelineOutputSinkTests
{
    [Fact]
    public async Task ExecuteAsync_WithValidItem_ShouldStoreInContext()
    {
        // Arrange
        var sink = new PipelineOutputSink<int>();
        var context = new PipelineContext();
        var expectedValue = 42;
        var dataStream = new InMemoryDataStream<int>([expectedValue], "TestPipe");

        // Act
        await sink.ConsumeAsync(dataStream, context, CancellationToken.None);

        // Assert
        context.Parameters.Should().ContainKey(CompositeContextKeys.OutputItem);
        context.Parameters[CompositeContextKeys.OutputItem].Should().Be(expectedValue);
    }

    [Fact]
    public async Task ExecuteAsync_WithMultipleItems_ShouldStoreFirstItemOnly()
    {
        // Arrange
        var sink = new PipelineOutputSink<int>();
        var context = new PipelineContext();
        var items = new[] { 1, 2, 3, 4, 5 };
        var dataStream = new InMemoryDataStream<int>(items, "TestPipe");

        // Act
        await sink.ConsumeAsync(dataStream, context, CancellationToken.None);

        // Assert
        context.Parameters.Should().ContainKey(CompositeContextKeys.OutputItem);
        context.Parameters[CompositeContextKeys.OutputItem].Should().Be(1);
    }

    [Fact]
    public async Task ExecuteAsync_WithReferenceType_ShouldStoreInContext()
    {
        // Arrange
        var sink = new PipelineOutputSink<string>();
        var context = new PipelineContext();
        var expectedValue = "test output";
        var dataStream = new InMemoryDataStream<string>([expectedValue], "TestPipe");

        // Act
        await sink.ConsumeAsync(dataStream, context, CancellationToken.None);

        // Assert
        context.Parameters.Should().ContainKey(CompositeContextKeys.OutputItem);
        context.Parameters[CompositeContextKeys.OutputItem].Should().Be(expectedValue);
    }

    [Fact]
    public async Task ExecuteAsync_WithComplexType_ShouldStoreInContext()
    {
        // Arrange
        var sink = new PipelineOutputSink<TestData>();
        var context = new PipelineContext();
        var expectedValue = new TestData { Id = 1, Name = "Test" };
        var dataStream = new InMemoryDataStream<TestData>([expectedValue], "TestPipe");

        // Act
        await sink.ConsumeAsync(dataStream, context, CancellationToken.None);

        // Assert
        context.Parameters.Should().ContainKey(CompositeContextKeys.OutputItem);
        var storedValue = context.Parameters[CompositeContextKeys.OutputItem] as TestData;
        storedValue.Should().NotBeNull();
        storedValue!.Id.Should().Be(1);
        storedValue.Name.Should().Be("Test");
    }

    [Fact]
    public async Task ExecuteAsync_WithEmptyPipe_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var sink = new PipelineOutputSink<int>();
        var context = new PipelineContext();
        var dataStream = new InMemoryDataStream<int>([], "TestPipe");

        // Act & Assert
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            sink.ConsumeAsync(dataStream, context, CancellationToken.None));
    }

    private sealed class TestData
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
