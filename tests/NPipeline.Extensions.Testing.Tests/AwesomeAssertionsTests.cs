using NPipeline.Extensions.Testing.AwesomeAssertions;
using NPipeline.Pipeline;
using NPipeline.Tests.Common;

namespace NPipeline.Extensions.Testing.Tests;

public class AwesomeAssertionsTests
{
    [Fact]
    public async Task InMemorySink_ShouldHaveReceived_Assertion_Should_Work()
    {
        // Arrange
        var sink = new InMemorySinkNode<int>();
        var context = PipelineContext.Default;
        var data = new InMemoryDataPipe<int>([1, 2, 3]);

        // Act
        await sink.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        sink.ShouldHaveReceived(3);
    }

    [Fact]
    public async Task InMemorySink_ShouldContain_Assertion_Should_Work()
    {
        // Arrange
        var sink = new InMemorySinkNode<int>();
        var context = PipelineContext.Default;
        var data = new InMemoryDataPipe<int>([1, 2, 3]);

        // Act
        await sink.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        sink.ShouldContain(x => x == 2);
    }
}
