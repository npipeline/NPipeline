using AwesomeAssertions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests;

public sealed class FilteringNodeTests
{
    [Fact]
    public async Task ExecuteAsync_WithPassingPredicate_ShouldReturnItem()
    {
        // Arrange
        var node = new FilteringNode<TestData>();
        node.Where(x => x.Age >= 18, x => $"Age {x.Age} is below minimum");

        var data = new TestData { Age = 25 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Should().BeSameAs(data);
    }

    [Fact]
    public async Task ExecuteAsync_WithFailingPredicate_ShouldThrowFilteringException()
    {
        // Arrange
        var node = new FilteringNode<TestData>();
        node.Where(x => x.Age >= 18, x => $"Age {x.Age} is below minimum");

        var data = new TestData { Age = 15 };
        var context = PipelineContext.Default;

        // Act & Assert
        var ex = await Assert.ThrowsAsync<FilteringException>(async () =>
            await node.ExecuteAsync(data, context, CancellationToken.None));

        ex.Reason.Should().Contain("Age 15 is below minimum");
    }

    [Fact]
    public async Task ExecuteAsync_WithMultiplePredicatesAllPass_ShouldReturnItem()
    {
        // Arrange
        var node = new FilteringNode<TestData>();

        node.Where(x => x.Age >= 18)
            .Where(x => !string.IsNullOrEmpty(x.Name))
            .Where(x => x.IsActive);

        var data = new TestData { Age = 25, Name = "Alice", IsActive = true };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Should().BeSameAs(data);
    }

    [Fact]
    public async Task ExecuteAsync_WithMultiplePredicatesFirstFails_ShouldThrowOnFirstFailure()
    {
        // Arrange
        var node = new FilteringNode<TestData>();

        node.Where(x => x.Age >= 18, _ => "Age check failed")
            .Where(x => !string.IsNullOrEmpty(x.Name), _ => "Name check failed");

        var data = new TestData { Age = 15, Name = "Alice" };
        var context = PipelineContext.Default;

        // Act & Assert
        var ex = await Assert.ThrowsAsync<FilteringException>(async () =>
            await node.ExecuteAsync(data, context, CancellationToken.None));

        ex.Reason.Should().Be("Age check failed");
    }

    [Fact]
    public async Task ExecuteAsync_WithNoPredicates_ShouldReturnItem()
    {
        // Arrange
        var node = new FilteringNode<TestData>();
        var data = new TestData { Name = "Test" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Should().BeSameAs(data);
    }

    [Fact]
    public async Task ExecuteAsync_WithoutReasonFactory_ShouldUseDefaultMessage()
    {
        // Arrange
        var node = new FilteringNode<TestData>();
        node.Where(x => x.Age >= 18);

        var data = new TestData { Age = 15 };
        var context = PipelineContext.Default;

        // Act & Assert
        var ex = await Assert.ThrowsAsync<FilteringException>(async () =>
            await node.ExecuteAsync(data, context, CancellationToken.None));

        ex.Reason.Should().Contain("did not meet filter criteria");
    }

    [Fact]
    public async Task ExecuteAsync_WithCancellationRequested_ShouldThrowOperationCanceledException()
    {
        // Arrange
        var node = new FilteringNode<TestData>();
        node.Where(x => x.Age >= 18);

        var data = new TestData { Age = 25 };
        var context = PipelineContext.Default;
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await node.ExecuteAsync(data, context, cts.Token));
    }

    [Fact]
    public void Where_ShouldAllowChaining()
    {
        // Arrange
        var node = new FilteringNode<TestData>();

        // Act
        var result = node.Where(x => x.Age >= 18)
            .Where(x => !string.IsNullOrEmpty(x.Name))
            .Where(x => x.IsActive);

        // Assert
        result.Should().BeSameAs(node);
    }

    [Fact]
    public async Task Constructor_WithPredicate_ShouldInitializeWithRule()
    {
        // Arrange & Act
        var node = new FilteringNode<TestData>(x => x.Age >= 18, x => "Too young");
        var data = new TestData { Age = 25 };
        var context = PipelineContext.Default;

        // Assert - Should not throw
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);
        result.Should().BeSameAs(data);
    }

    [Fact]
    public async Task ExecuteAsync_WithComplexPredicate_ShouldEvaluateCorrectly()
    {
        // Arrange
        var node = new FilteringNode<TestData>();

        node.Where(x => x.Age >= 18 && x.Age <= 65,
            x => $"Age {x.Age} is outside working range 18-65");

        var youngData = new TestData { Age = 15 };
        var validData = new TestData { Age = 30 };
        var oldData = new TestData { Age = 70 };
        var context = PipelineContext.Default;

        // Act & Assert
        var validResult = await node.ExecuteAsync(validData, context, CancellationToken.None);
        validResult.Should().BeSameAs(validData);

        await Assert.ThrowsAsync<FilteringException>(async () =>
            await node.ExecuteAsync(youngData, context, CancellationToken.None));

        await Assert.ThrowsAsync<FilteringException>(async () =>
            await node.ExecuteAsync(oldData, context, CancellationToken.None));
    }

    private sealed class TestData
    {
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public bool IsActive { get; set; }
    }
}
