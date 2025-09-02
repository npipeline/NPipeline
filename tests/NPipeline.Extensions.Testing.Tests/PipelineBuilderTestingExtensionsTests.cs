using System.Reflection;
using FluentAssertions;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing.Tests;

public class PipelineBuilderTestingExtensionsTests
{
    [Fact]
    public void AddInMemorySource_Generic_ShouldAddSourceNode()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddInMemorySource<int>();

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().NotBeNullOrEmpty();
        GetNodes(builder).Should().ContainKey(handle.Id);
        GetNodes(builder)[handle.Id].NodeType.Should().Be<InMemorySourceNode<int>>();
    }

    [Fact]
    public void AddInMemorySource_WithName_ShouldAddNamedSourceNode()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var name = "testSource";

        // Act
        var handle = builder.AddInMemorySource<int>(name);

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be(name.ToLowerInvariant());
        GetNodes(builder).Should().ContainKey(handle.Id);
        GetNodes(builder)[handle.Id].NodeType.Should().Be<InMemorySourceNode<int>>();
    }

    [Fact]
    public void AddInMemorySource_WithItems_ShouldAddSourceNodeWithItems()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var items = new[] { 1, 2, 3 };

        // Act
        var handle = builder.AddInMemorySource(items);

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().NotBeNullOrEmpty();
        GetNodes(builder).Should().ContainKey(handle.Id);
        GetNodes(builder)[handle.Id].NodeType.Should().Be<InMemorySourceNode<int>>();

        // Verify that the preconfigured node instance is created
        HasPreconfiguredNodeInstance(builder, handle.Id).Should().BeTrue();
    }

    [Fact]
    public void AddInMemorySource_WithNameAndItems_ShouldAddNamedSourceNodeWithItems()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var name = "testSource";
        var items = new[] { 1, 2, 3 };

        // Act
        var handle = builder.AddInMemorySource(name, items);

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be(name.ToLowerInvariant());
        GetNodes(builder).Should().ContainKey(handle.Id);
        GetNodes(builder)[handle.Id].NodeType.Should().Be<InMemorySourceNode<int>>();

        // Verify that the preconfigured node instance is created
        HasPreconfiguredNodeInstance(builder, handle.Id).Should().BeTrue();
    }

    [Fact]
    public void AddInMemorySourceWithDataFromContext_ShouldAddContextBackedSourceNode()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };

        // Act
        var handle = builder.AddInMemorySourceWithDataFromContext(context, items);

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().NotBeNullOrEmpty();
        GetNodes(builder).Should().ContainKey(handle.Id);
        GetNodes(builder)[handle.Id].NodeType.Should().Be<InMemorySourceNode<int>>();

        // Verify that the preconfigured node instance is created
        HasPreconfiguredNodeInstance(builder, handle.Id).Should().BeTrue();

        // Verify that the items are stored in the context
        context.Items.Should().ContainKey($"NPipeline.Testing.SourceData::{handle.Id}");
        context.Items.Should().ContainKey("NPipeline.Testing.SourceData::System.Int32");
    }

    [Fact]
    public void AddInMemorySourceWithDataFromContext_WithName_ShouldAddNamedContextBackedSourceNode()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var context = PipelineContext.Default;
        var name = "testSource";
        var items = new[] { 1, 2, 3 };

        // Act
        var handle = builder.AddInMemorySourceWithDataFromContext(context, name, items);

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be(name.ToLowerInvariant());
        GetNodes(builder).Should().ContainKey(handle.Id);
        GetNodes(builder)[handle.Id].NodeType.Should().Be<InMemorySourceNode<int>>();

        // Verify that the preconfigured node instance is created
        HasPreconfiguredNodeInstance(builder, handle.Id).Should().BeTrue();

        // Verify that the items are stored in the context (uses sanitized node id)
        context.Items.Should().ContainKey($"NPipeline.Testing.SourceData::{handle.Id}");
        context.Items.Should().ContainKey("NPipeline.Testing.SourceData::System.Int32");
    }

    [Fact]
    public void AddPassThroughTransform_Generic_ShouldAddTransformNode()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddPassThroughTransform<string, object>();

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().NotBeNullOrEmpty();
        GetNodes(builder).Should().ContainKey(handle.Id);
        GetNodes(builder)[handle.Id].NodeType.Should().Be<PassThroughTransformNode<string, object>>();
    }

    [Fact]
    public void AddPassThroughTransform_WithName_ShouldAddNamedTransformNode()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var name = "testTransform";

        // Act
        var handle = builder.AddPassThroughTransform<string, object>(name);

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be(name.ToLowerInvariant());
        GetNodes(builder).Should().ContainKey(handle.Id);
        GetNodes(builder)[handle.Id].NodeType.Should().Be<PassThroughTransformNode<string, object>>();
    }

    [Fact]
    public void AddInMemorySink_Generic_ShouldAddSinkNode()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddInMemorySink<int>();

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().NotBeNullOrEmpty();
        GetNodes(builder).Should().ContainKey(handle.Id);
        GetNodes(builder)[handle.Id].NodeType.Should().Be<InMemorySinkNode<int>>();

        // Verify that the preconfigured node instance is created
        HasPreconfiguredNodeInstance(builder, handle.Id).Should().BeTrue();
    }

    [Fact]
    public void AddInMemorySink_WithContext_ShouldAddSinkNodeAndRegisterInContext()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var context = PipelineContext.Default;

        // Act
        var handle = builder.AddInMemorySink<int>(context);

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().NotBeNullOrEmpty();
        GetNodes(builder).Should().ContainKey(handle.Id);
        GetNodes(builder)[handle.Id].NodeType.Should().Be<InMemorySinkNode<int>>();

        // Verify that the preconfigured node instance is created
        HasPreconfiguredNodeInstance(builder, handle.Id).Should().BeTrue();

        // Verify that the sink is registered in the context
        context.Items.Should().ContainKey(typeof(InMemorySinkNode<int>).FullName!);
        context.Items.Should().ContainKey(typeof(int).FullName!);

        var sink = context.GetSink<InMemorySinkNode<int>>();
        sink.Should().NotBeNull();
    }

    [Fact]
    public void AddInMemorySink_WithName_ShouldAddNamedSinkNode()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var name = "testSink";

        // Act
        var handle = builder.AddInMemorySink<int>(name);

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be(name.ToLowerInvariant());
        GetNodes(builder).Should().ContainKey(handle.Id);
        GetNodes(builder)[handle.Id].NodeType.Should().Be<InMemorySinkNode<int>>();

        // Verify that the preconfigured node instance is created
        HasPreconfiguredNodeInstance(builder, handle.Id).Should().BeTrue();
    }

    [Fact]
    public void AddInMemorySink_WithNameAndContext_ShouldAddNamedSinkNodeAndRegisterInContext()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var context = PipelineContext.Default;
        var name = "testSink";

        // Act
        var handle = builder.AddInMemorySink<int>(name, context);

        // Assert
        handle.Should().NotBeNull();
        handle.Id.Should().Be(name.ToLowerInvariant());
        GetNodes(builder).Should().ContainKey(handle.Id);
        GetNodes(builder)[handle.Id].NodeType.Should().Be<InMemorySinkNode<int>>();

        // Verify that the preconfigured node instance is created
        HasPreconfiguredNodeInstance(builder, handle.Id).Should().BeTrue();

        // Verify that the sink is registered in the context
        context.Items.Should().ContainKey(typeof(InMemorySinkNode<int>).FullName!);
        context.Items.Should().ContainKey(typeof(int).FullName!);

        var sink = context.GetSink<InMemorySinkNode<int>>();
        sink.Should().NotBeNull();
    }

    [Fact]
    public void AddInMemorySource_WithNullItems_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => builder.AddInMemorySource((IEnumerable<int>)null!));
    }

    [Fact]
    public void AddInMemorySource_WithNullContext_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var items = new[] { 1, 2, 3 };

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => builder.AddInMemorySourceWithDataFromContext(null!, items));
    }

    [Fact]
    public void AddInMemorySink_WithNullContext_ShouldThrowArgumentNullException()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => builder.AddInMemorySink<int>((PipelineContext)null!));
    }

    [Fact]
    public void AddInMemorySource_WithEmptyItems_ShouldCreateNodeSuccessfully()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var items = Array.Empty<int>();

        // Act
        var handle = builder.AddInMemorySource(items);

        // Assert
        handle.Should().NotBeNull();
        HasPreconfiguredNodeInstance(builder, handle.Id).Should().BeTrue();
    }

    [Fact]
    public void AllExtensionMethods_ShouldCreateUniqueNodeIds()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };

        // Act
        var source1 = builder.AddInMemorySource<int>();
        var source2 = builder.AddInMemorySource<int>("namedSource");
        var source3 = builder.AddInMemorySource(items);
        var source4 = builder.AddInMemorySource("namedSourceWithItems", items);
        var source5 = builder.AddInMemorySourceWithDataFromContext(context, items);
        var source6 = builder.AddInMemorySourceWithDataFromContext(context, "namedContextSource", items);

        var transform1 = builder.AddPassThroughTransform<int, string>();
        var transform2 = builder.AddPassThroughTransform<int, string>("namedTransform");

        var sink1 = builder.AddInMemorySink<int>();
        var sink2 = builder.AddInMemorySink<int>("namedSink");
        var sink3 = builder.AddInMemorySink<int>(context);
        var sink4 = builder.AddInMemorySink<int>("namedSinkWithContext", context);

        // Assert
        var allIds = new[]
        {
            source1.Id, source2.Id, source3.Id, source4.Id, source5.Id, source6.Id,
            transform1.Id, transform2.Id,
            sink1.Id, sink2.Id, sink3.Id, sink4.Id,
        };

        allIds.Distinct().Should().HaveCount(allIds.Length);
    }

    // Helper methods to access private fields using reflection
    private static Dictionary<string, NodeDefinition> GetNodes(PipelineBuilder builder)
    {
        var nodeStateProperty = typeof(PipelineBuilder).GetProperty("NodeState", BindingFlags.NonPublic | BindingFlags.Instance);
        var nodeState = nodeStateProperty!.GetValue(builder)!;
        var nodesField = nodeState.GetType().GetProperty("Nodes");
        return (Dictionary<string, NodeDefinition>)nodesField!.GetValue(nodeState)!;
    }

    private static bool HasPreconfiguredNodeInstance(PipelineBuilder builder, string nodeId)
    {
        var nodeStateProperty = typeof(PipelineBuilder).GetProperty("NodeState", BindingFlags.NonPublic | BindingFlags.Instance);
        var nodeState = nodeStateProperty!.GetValue(builder)!;
        var instancesField = nodeState.GetType().GetProperty("PreconfiguredNodeInstances");
        var instances = (Dictionary<string, INode>)instancesField!.GetValue(nodeState)!;
        return instances.ContainsKey(nodeId);
    }
}
