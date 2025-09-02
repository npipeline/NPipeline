using AwesomeAssertions;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Nodes.Batching;

/// <summary>
///     Tests for UnbatchingNode&lt;T&gt;
///     marker node.
///     UnbatchingNode is a marker node that indicates unbatching logic is needed.
///     The actual unbatching is handled by UnbatchingExecutionStrategy, not by the node itself.
///     Covers 2 statements in UnbatchingNode&lt;T&gt;.
/// </summary>
public sealed class UnbatchingNodeTests
{
    [Fact]
    public void UnbatchingNode_IsMarkerNode_CanBeConstructed()
    {
        // Arrange & Act
        UnbatchingNode<int> node = new();

        // Assert - node should be a valid marker
        _ = node.Should().NotBeNull();
        _ = node.Should().BeOfType<UnbatchingNode<int>>();
    }

    [Fact]
    public void UnbatchingNode_WithDifferentType_Works()
    {
        // Arrange & Act
        UnbatchingNode<string> node = new();

        // Assert
        _ = node.Should().NotBeNull();
        _ = node.Should().BeOfType<UnbatchingNode<string>>();
    }

    [Fact]
    public async Task UnbatchingNode_DirectExecution_Throws()
    {
        // Arrange
        UnbatchingNode<int> node = new();
        var context = PipelineContext.Default;
        int[] batchItem = [1, 2, 3];

        // Act & Assert - calling ExecuteAsync directly should throw
        // because UnbatchingNode is a marker that should not be executed directly
        _ = await node.Invoking(n => n.ExecuteAsync(batchItem, context, CancellationToken.None))
            .Should().ThrowAsync<NotSupportedException>();
    }

    [Fact]
    public async Task UnbatchingNode_DirectExecution_WithStrings_Throws()
    {
        // Arrange
        UnbatchingNode<string> node = new();
        var context = PipelineContext.Default;
        string[] batchItem = ["a", "b", "c"];

        // Act & Assert
        _ = await node.Invoking(n => n.ExecuteAsync(batchItem, context, CancellationToken.None))
            .Should().ThrowAsync<NotSupportedException>();
    }

    [Fact]
    public async Task UnbatchingNode_Disposal_Works()
    {
        // Arrange
        UnbatchingNode<int> node = new();

        // Act & Assert - disposal should not throw
        _ = await node.Invoking(n => n.DisposeAsync().AsTask()).Should().NotThrowAsync();
    }
}
