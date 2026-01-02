using AwesomeAssertions;
using NPipeline.Extensions.Nodes.Core;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests.Core;

public sealed class PropertyTransformationNodeTests
{
    [Fact]
    public async Task ExecuteAsync_WithSingleTransformation_ShouldTransformProperty()
    {
        // Arrange
        var node = new TestTransformationNode();
        node.Register(x => x.Name, name => name.ToUpperInvariant());

        var data = new TestData { Name = "alice" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("ALICE");
        result.Should().BeSameAs(data); // In-place mutation
    }

    [Fact]
    public async Task ExecuteAsync_WithMultipleTransformations_ShouldApplyInOrder()
    {
        // Arrange
        var node = new TestTransformationNode();
        node.Register(x => x.Name, name => name.ToUpperInvariant());
        node.Register(x => x.Age, age => age + 10);

        var data = new TestData { Name = "bob", Age = 30 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("BOB");
        result.Age.Should().Be(40);
    }

    [Fact]
    public async Task ExecuteAsync_WithRegisterMany_ShouldTransformAllProperties()
    {
        // Arrange
        var node = new TestTransformationNode();

        node.RegisterMany<string>(
            [x => x.Name, x => x.Email],
            str => str.ToUpperInvariant());

        var data = new TestData { Name = "alice", Email = "alice@test.com" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("ALICE");
        result.Email.Should().Be("ALICE@TEST.COM");
    }

    [Fact]
    public async Task ExecuteAsync_WithNoTransformations_ShouldReturnUnchanged()
    {
        // Arrange
        var node = new TestTransformationNode();
        var data = new TestData { Name = "unchanged", Age = 25 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Should().BeSameAs(data);
        result.Name.Should().Be("unchanged");
        result.Age.Should().Be(25);
    }

    [Fact]
    public async Task ExecuteAsync_WithCancellationRequested_ShouldThrowOperationCanceledException()
    {
        // Arrange
        var node = new TestTransformationNode();
        node.Register(x => x.Name, name => name.ToUpperInvariant());

        var data = new TestData { Name = "test" };
        var context = PipelineContext.Default;
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await node.ExecuteAsync(data, context, cts.Token));
    }

    [Fact]
    public void Register_ShouldAllowChaining()
    {
        // Arrange
        var node = new TestTransformationNode();

        // Act
        var result = node.Register(x => x.Name, name => name.ToUpperInvariant())
            .Register(x => x.Age, age => age + 1);

        // Assert
        result.Should().BeSameAs(node);
    }

    [Fact]
    public void RegisterMany_ShouldAllowChaining()
    {
        // Arrange
        var node = new TestTransformationNode();

        // Act
        var result = node.RegisterMany<string>(
            [x => x.Name, x => x.Email],
            str => str.ToUpperInvariant());

        // Assert
        result.Should().BeSameAs(node);
    }

    [Fact]
    public async Task ExecuteAsync_WithComplexTransformation_ShouldApplyCorrectly()
    {
        // Arrange
        var node = new TestTransformationNode();

        node.Register(x => x.Name, name =>
            string.IsNullOrWhiteSpace(name)
                ? "Unknown"
                : name.Trim());

        node.Register(x => x.Age, age => Math.Max(0, age));

        var data = new TestData { Name = "  ", Age = -5 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("Unknown");
        result.Age.Should().Be(0);
    }

    private sealed class TestTransformationNode : PropertyTransformationNode<TestData>
    {
    }

    private sealed class TestData
    {
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public string Email { get; set; } = string.Empty;
    }
}
