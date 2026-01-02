using AwesomeAssertions;
using NPipeline.Extensions.Nodes.Core;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests;

public sealed class PipelineBuilderExtensionsTests
{
    private sealed class TestValidationNode : ValidationNode<string>
    {
    }

    [Fact]
    public void AddValidationNode_ShouldRegisterNode()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddValidationNode<string, TestValidationNode>("testvalidation");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("testvalidation");
    }

    [Fact]
    public void AddValidationNode_WithoutName_ShouldUseTypeName()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddValidationNode<string, TestValidationNode>();

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("testvalidationnode");
    }

    [Fact]
    public void AddFilteringNode_ShouldRegisterNode()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddFilteringNode<string>("testfiltering");

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be("testfiltering");
    }

    [Fact]
    public void AddFilteringNode_WithoutName_ShouldUseTypeName()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddFilteringNode<string>();

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().StartWith("filteringnode");
    }

    [Fact]
    public void AddValidationNode_WithNullBuilder_ShouldThrowArgumentNullException()
    {
        // Arrange
        PipelineBuilder builder = null!;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddValidationNode<string, TestValidationNode>());
    }

    [Fact]
    public void AddFilteringNode_WithNullBuilder_ShouldThrowArgumentNullException()
    {
        // Arrange
        PipelineBuilder builder = null!;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() =>
            builder.AddFilteringNode<string>());
    }

    [Fact]
    public void AddValidationNode_MultipleNodes_ShouldAllowChaining()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle1 = builder.AddValidationNode<string, TestValidationNode>("validation1");
        var handle2 = builder.AddValidationNode<string, TestValidationNode>("validation2");

        // Assert
        handle1.Should().NotBeNull();
        handle2.Should().NotBeNull();
        handle1.Id.Should().NotBe(handle2.Id);
    }

    [Fact]
    public void AddFilteringNode_MultipleNodes_ShouldAllowChaining()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle1 = builder.AddFilteringNode<string>("filtering1");
        var handle2 = builder.AddFilteringNode<string>("filtering2");

        // Assert
        handle1.Should().NotBeNull();
        handle2.Should().NotBeNull();
        handle1.Id.Should().NotBe(handle2.Id);
    }

    [Fact]
    public void Extensions_ShouldReturnCorrectHandleTypes()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var validationHandle = builder.AddValidationNode<string, TestValidationNode>();
        var filteringHandle = builder.AddFilteringNode<string>();

        // Assert
        validationHandle.Should().BeOfType<Graph.TransformNodeHandle<string, string>>();
        filteringHandle.Should().BeOfType<Graph.TransformNodeHandle<string, string>>();
    }
}
