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
    public async Task UnbatchingNode_DirectExecution_Works()
    {
        // Arrange
        UnbatchingNode<int> node = new();
        var context = PipelineContext.Default;
        var batches = new List<int[]> { new[] { 1, 2, 3 }, new[] { 4, 5, 6 } }.ToAsyncEnumerable();

        // Act - calling ExecuteAsync with stream of batches should work
        var results = new List<int>();

        await foreach (var item in node.ExecuteAsync(batches, context, CancellationToken.None))
        {
            results.Add(item);
        }

        // Assert
        results.Should().HaveCount(6);
        results.Should().BeEquivalentTo(new[] { 1, 2, 3, 4, 5, 6 });
    }

    [Fact]
    public async Task UnbatchingNode_DirectExecution_WithStrings_Works()
    {
        // Arrange
        UnbatchingNode<string> node = new();
        var context = PipelineContext.Default;
        var batches = new List<string[]> { new[] { "a", "b", "c" }, new[] { "d", "e" } }.ToAsyncEnumerable();

        // Act
        var results = new List<string>();

        await foreach (var item in node.ExecuteAsync(batches, context, CancellationToken.None))
        {
            results.Add(item);
        }

        // Assert
        results.Should().HaveCount(5);
        results.Should().BeEquivalentTo("a", "b", "c", "d", "e");
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
