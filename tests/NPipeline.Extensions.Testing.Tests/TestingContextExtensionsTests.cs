using FluentAssertions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing.Tests;

public class TestingContextExtensionsTests
{
    [Fact]
    public void SetSourceData_WithItemsAndNodeId_ShouldStoreDataUnderNodeScopedKey()
    {
        // Arrange
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };
        var nodeId = "testNode";

        // Act
        context.SetSourceData(items, nodeId);

        // Assert
        context.Items.Should().ContainKey($"NPipeline.Testing.SourceData::{nodeId}");
        var storedItems = context.Items[$"NPipeline.Testing.SourceData::{nodeId}"];
        storedItems.Should().BeEquivalentTo(items);
    }

    [Fact]
    public void SetSourceData_WithItemsAndNodeId_ShouldStoreDataUnderTypeScopedKey()
    {
        // Arrange
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };
        var nodeId = "testNode";

        // Act
        context.SetSourceData(items, nodeId);

        // Assert
        context.Items.Should().ContainKey(PipelineContextKeys.TestingSourceData(typeof(int).FullName!));
        var storedItems = context.Items[PipelineContextKeys.TestingSourceData(typeof(int).FullName!)];
        storedItems.Should().BeEquivalentTo(items);
    }

    [Fact]
    public void SetSourceData_WithItemsOnly_ShouldStoreDataUnderTypeScopedKey()
    {
        // Arrange
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };

        // Act
        context.SetSourceData(items);

        // Assert
        context.Items.Should().ContainKey(PipelineContextKeys.TestingSourceData(typeof(int).FullName!));
        var storedItems = context.Items[PipelineContextKeys.TestingSourceData(typeof(int).FullName!)];
        storedItems.Should().BeEquivalentTo(items);
    }

    [Fact]
    public void SetSourceData_WithNullContext_ShouldThrowArgumentNullException()
    {
        // Arrange
        var items = new[] { 1, 2, 3 };

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => ((PipelineContext)null!).SetSourceData(items));
    }

    [Fact]
    public void SetSourceData_WithNullItems_ShouldThrowArgumentNullException()
    {
        // Arrange
        var context = PipelineContext.Default;

        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => context.SetSourceData<int>(null!));
    }

    [Fact]
    public void GetSink_WithSinkRegisteredByKey_ShouldReturnSink()
    {
        // Arrange
        var context = PipelineContext.Default;
        var sink = new InMemorySinkNode<int>();
        context.Items[typeof(InMemorySinkNode<int>).FullName!] = sink;

        // Act
        var result = context.GetSink<InMemorySinkNode<int>>();

        // Assert
        result.Should().Be(sink);
    }

    [Fact]
    public void GetSink_WithSinkRegisteredByType_ShouldReturnSink()
    {
        // Arrange
        var context = PipelineContext.Default;
        var sink = new InMemorySinkNode<int>();
        context.Items["SomeKey"] = sink;

        // Act
        var result = context.GetSink<InMemorySinkNode<int>>();

        // Assert
        result.Should().Be(sink);
    }

    [Fact]
    public void GetSink_WithMultipleSinksRegistered_ShouldReturnFirstMatchingSink()
    {
        // Arrange
        var context = PipelineContext.Default;
        var sink1 = new InMemorySinkNode<int>();
        var sink2 = new InMemorySinkNode<int>();
        context.Items["Key1"] = sink1;
        context.Items["Key2"] = sink2;

        // Act
        var result = context.GetSink<InMemorySinkNode<int>>();

        // Assert
        result.Should().Be(sink1);
    }

    [Fact]
    public void GetSink_WithParentContext_ShouldReturnSinkFromParent()
    {
        // Arrange
        var parentContext = PipelineContext.Default;
        var sink = new InMemorySinkNode<int>();
        parentContext.Items[typeof(InMemorySinkNode<int>).FullName!] = sink;

        var context = PipelineContext.Default;
        context.Items[PipelineContextKeys.TestingParentContext] = parentContext;

        // Act
        var result = context.GetSink<InMemorySinkNode<int>>();

        // Assert
        result.Should().Be(sink);
    }

    [Fact]
    public void GetSink_WithParentContextAndSinkInCurrentContext_ShouldReturnSinkFromCurrentContext()
    {
        // Arrange
        var parentSink = new InMemorySinkNode<int>();
        var parentContext = PipelineContext.Default;
        parentContext.Items[typeof(InMemorySinkNode<int>).FullName!] = parentSink;

        var currentSink = new InMemorySinkNode<int>();
        var context = PipelineContext.Default;
        context.Items[PipelineContextKeys.TestingParentContext] = parentContext;
        context.Items[typeof(InMemorySinkNode<int>).FullName!] = currentSink;

        // Act
        var result = context.GetSink<InMemorySinkNode<int>>();

        // Assert
        result.Should().Be(currentSink);
    }

    [Fact]
    public void GetSink_WithParentContextAndSinkByTypeInParent_ShouldReturnSinkFromParent()
    {
        // Arrange
        var parentContext = PipelineContext.Default;
        var sink = new InMemorySinkNode<int>();
        parentContext.Items["SomeKey"] = sink;

        var context = PipelineContext.Default;
        context.Items[PipelineContextKeys.TestingParentContext] = parentContext;

        // Act
        var result = context.GetSink<InMemorySinkNode<int>>();

        // Assert
        result.Should().Be(sink);
    }

    [Fact]
    public void GetSink_WithNullContext_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        Assert.Throws<ArgumentNullException>(() => ((PipelineContext)null!).GetSink<InMemorySinkNode<int>>());
    }

    [Fact]
    public void GetSink_WithNoSinkRegistered_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var context = PipelineContext.Default;

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => context.GetSink<InMemorySinkNode<int>>());
        exception.Message.Should().Contain("Could not find an instance of 'InMemorySinkNode<int>'");
    }

    [Fact]
    public void GetSink_WithParentContextButNoSinkAnywhere_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var parentContext = PipelineContext.Default;
        var context = PipelineContext.Default;
        context.Items[PipelineContextKeys.TestingParentContext] = parentContext;

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => context.GetSink<InMemorySinkNode<int>>());
        exception.Message.Should().Contain("Could not find an instance of 'InMemorySinkNode<int>'");
    }

    [Fact]
    public void SetSourceData_WithEmptyNodeId_ShouldNotStoreNodeScopedKey()
    {
        // Arrange
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };

        // Act
        context.SetSourceData(items, "");

        // Assert
        context.Items.Should().NotContainKey("NPipeline.Testing.SourceData::");
        context.Items.Should().ContainKey("NPipeline.Testing.SourceData::System.Int32");
    }

    [Fact]
    public void SetSourceData_WithWhitespaceNodeId_ShouldNotStoreNodeScopedKey()
    {
        // Arrange
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };

        // Act
        context.SetSourceData(items, "   ");

        // Assert
        context.Items.Should().NotContainKey("NPipeline.Testing.SourceData::   ");
        context.Items.Should().ContainKey("NPipeline.Testing.SourceData::System.Int32");
    }

    [Fact]
    public void SetSourceData_WithNullNodeId_ShouldNotStoreNodeScopedKey()
    {
        // Arrange
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };

        // Act
        context.SetSourceData(items);

        // Assert
        context.Items.Should().NotContainKey("NPipeline.Testing.SourceData::");
        context.Items.Should().ContainKey("NPipeline.Testing.SourceData::System.Int32");
    }

    [Fact]
    public void SetSourceData_WithDifferentTypes_ShouldStoreUnderDifferentTypeKeys()
    {
        // Arrange
        var context = PipelineContext.Default;
        var intItems = new[] { 1, 2, 3 };
        var stringItems = new[] { "a", "b", "c" };

        // Act
        context.SetSourceData(intItems);
        context.SetSourceData(stringItems);

        // Assert
        context.Items.Should().ContainKey(PipelineContextKeys.TestingSourceData(typeof(int).FullName!));
        context.Items.Should().ContainKey(PipelineContextKeys.TestingSourceData(typeof(string).FullName!));
        context.Items[PipelineContextKeys.TestingSourceData(typeof(int).FullName!)].Should().BeEquivalentTo(intItems);
        context.Items[PipelineContextKeys.TestingSourceData(typeof(string).FullName!)].Should().BeEquivalentTo(stringItems);
    }

    [Fact]
    public void GetSink_WithDifferentTypes_ShouldReturnCorrectType()
    {
        // Arrange
        var context = PipelineContext.Default;
        var intSink = new InMemorySinkNode<int>();
        var stringSink = new InMemorySinkNode<string>();
        context.Items["IntSink"] = intSink;
        context.Items["StringSink"] = stringSink;

        // Act
        var retrievedIntSink = context.GetSink<InMemorySinkNode<int>>();
        var retrievedStringSink = context.GetSink<InMemorySinkNode<string>>();

        // Assert
        retrievedIntSink.Should().Be(intSink);
        retrievedStringSink.Should().Be(stringSink);
    }

    [Fact]
    public void SetSourceData_WithEnumerableItems_ShouldConvertToList()
    {
        // Arrange
        var context = PipelineContext.Default;
        var items = new List<int> { 1, 2, 3 };

        // Act
        context.SetSourceData(items);

        // Assert
        context.Items[PipelineContextKeys.TestingSourceData(typeof(int).FullName!)].Should().BeAssignableTo<IReadOnlyList<int>>();
        context.Items[PipelineContextKeys.TestingSourceData(typeof(int).FullName!)].Should().BeEquivalentTo(items);
    }
}
