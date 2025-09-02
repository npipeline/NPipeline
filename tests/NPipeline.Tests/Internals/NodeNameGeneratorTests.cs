using AwesomeAssertions;
using NPipeline.Extensions.Testing;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Internals;

public sealed class NodeNameGeneratorTests
{
    [Fact]
    public void Sanitize_ShouldProduceLowercaseId_ForMixedCaseName()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var name = "TestVisualization";

        // Act
        var handle = builder.AddSource<InMemorySourceNode<string>, string>(name);

        // Assert
        handle.Id.Should().Be("testvisualization");
    }
}
