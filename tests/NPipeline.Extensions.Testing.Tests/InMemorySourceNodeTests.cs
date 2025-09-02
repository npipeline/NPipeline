using FluentAssertions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing.Tests;

public class InMemorySourceNodeTests
{
    [Fact]
    public void ParameterlessConstructor_ShouldCreateNode_ThatUsesContext()
    {
        // Arrange & Act
        var node = new InMemorySourceNode<int>();

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void ConstructorWithItems_ShouldCreateNode_WithEmbeddedItems()
    {
        // Arrange
        var items = new[] { 1, 2, 3 };

        // Act
        var node = new InMemorySourceNode<int>(items);

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void ConstructorWithContextAndNodeId_ShouldResolveItemsFromContext()
    {
        // Arrange
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };
        context.SetSourceData(items, "testNode");

        // Act
        var node = new InMemorySourceNode<int>(context, "testNode");

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void ConstructorWithContext_ShouldResolveItemsFromTypeContext()
    {
        // Arrange
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };
        context.SetSourceData(items);

        // Act
        var node = new InMemorySourceNode<int>(context);

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public async Task ExecuteAsync_WithEmbeddedItems_ShouldReturnDataPipeWithItems()
    {
        // Arrange
        var items = new[] { 1, 2, 3 };
        var node = new InMemorySourceNode<int>(items);
        var context = PipelineContext.Default;

        // Act
        var result = node.ExecuteAsync(context, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        var resultList = new List<int>();

        await foreach (var item in result)
        {
            resultList.Add(item);
        }

        resultList.Should().BeEquivalentTo(items);
    }

    [Fact]
    public async Task ExecuteAsync_WithNodeScopedContextData_ShouldReturnDataPipeWithItems()
    {
        // Arrange
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };
        context.SetSourceData(items, "testNode");

        // Set CurrentNodeId through reflection since it's internal setter
        context.GetType().GetProperty(nameof(PipelineContext.CurrentNodeId))?.SetValue(context, "testNode");
        var node = new InMemorySourceNode<int>();

        // Act
        var result = node.ExecuteAsync(context, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        var resultList = new List<int>();

        await foreach (var item in result)
        {
            resultList.Add(item);
        }

        resultList.Should().BeEquivalentTo(items);
    }

    [Fact]
    public async Task ExecuteAsync_WithTypeScopedContextData_ShouldReturnDataPipeWithItems()
    {
        // Arrange
        var context = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };
        context.SetSourceData(items);

        // Set CurrentNodeId through reflection since it's internal setter
        context.GetType().GetProperty(nameof(PipelineContext.CurrentNodeId))?.SetValue(context, "testNode");
        var node = new InMemorySourceNode<int>();

        // Act
        var result = node.ExecuteAsync(context, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        var resultList = new List<int>();

        await foreach (var item in result)
        {
            resultList.Add(item);
        }

        resultList.Should().BeEquivalentTo(items);
    }

    [Fact]
    public async Task ExecuteAsync_WithParentContext_ShouldResolveFromParent()
    {
        // Arrange
        var parentContext = PipelineContext.Default;
        var items = new[] { 1, 2, 3 };
        parentContext.SetSourceData(items, "testNode");

        var context = PipelineContext.Default;
        context.Items[PipelineContextKeys.TestingParentContext] = parentContext;

        // Set CurrentNodeId through reflection since it's internal setter
        context.GetType().GetProperty(nameof(PipelineContext.CurrentNodeId))?.SetValue(context, "testNode");
        var node = new InMemorySourceNode<int>();

        // Act
        var result = node.ExecuteAsync(context, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        var resultList = new List<int>();

        await foreach (var item in result)
        {
            resultList.Add(item);
        }

        resultList.Should().BeEquivalentTo(items);
    }

    [Fact]
    public void ExecuteAsync_WithNoSourceData_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var context = PipelineContext.Default;

        // Set CurrentNodeId through reflection since it's internal setter
        context.GetType().GetProperty(nameof(PipelineContext.CurrentNodeId))?.SetValue(context, "testNode");
        var node = new InMemorySourceNode<int>();

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => node.ExecuteAsync(context, CancellationToken.None));

        exception.Message.Should().Contain("No source data configured");
        exception.Message.Should().Contain("testNode");
    }

    [Fact]
    public void ConstructorWithContextAndNullItems_ShouldCreateNodeWithEmptyList()
    {
        // Arrange
        var context = PipelineContext.Default;

        // Don't set any source data

        // Act
        var node = new InMemorySourceNode<int>(context, "testNode");

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public void ConstructorWithNullContext_ShouldThrowArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() => new InMemorySourceNode<int>((PipelineContext)null!));
    }

    [Fact]
    public void ConstructorWithNullItems_ShouldThrowArgumentNullException()
    {
        // Arrange & Act & Assert
        Assert.Throws<ArgumentNullException>(() => new InMemorySourceNode<int>((IEnumerable<int>)null!));
    }
}
