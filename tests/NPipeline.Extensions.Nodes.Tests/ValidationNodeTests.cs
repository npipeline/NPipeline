using AwesomeAssertions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests;

public sealed class ValidationNodeTests
{
    [Fact]
    public async Task ExecuteAsync_WithValidData_ShouldReturnItem()
    {
        // Arrange
        var node = new TestValidationNode();
        node.Register(x => x.Name, name => !string.IsNullOrEmpty(name), "NotEmpty");

        var data = new TestData { Name = "Alice" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Should().BeSameAs(data);
        result.Name.Should().Be("Alice");
    }

    [Fact]
    public async Task ExecuteAsync_WithInvalidData_ShouldThrowValidationException()
    {
        // Arrange
        var node = new TestValidationNode();
        node.Register(x => x.Name, name => !string.IsNullOrEmpty(name), "NotEmpty");

        var data = new TestData { Name = "" };
        var context = PipelineContext.Default;

        // Act & Assert
        var ex = await Assert.ThrowsAsync<ValidationException>(async () =>
            await node.ExecuteAsync(data, context, CancellationToken.None));

        ex.PropertyPath.Should().Contain("Name");
        ex.RuleName.Should().Be("NotEmpty");
        ex.Value.Should().Be("");
    }

    [Fact]
    public async Task ExecuteAsync_WithCustomMessage_ShouldIncludeMessageInException()
    {
        // Arrange
        var node = new TestValidationNode();

        node.Register(
            x => x.Age,
            age => age >= 18,
            "MinAge",
            value => $"Age must be at least 18, got {value}");

        var data = new TestData { Age = 15 };
        var context = PipelineContext.Default;

        // Act & Assert
        var ex = await Assert.ThrowsAsync<ValidationException>(async () =>
            await node.ExecuteAsync(data, context, CancellationToken.None));

        ex.Message.Should().Contain("Age must be at least 18");
        ex.Message.Should().Contain("15");
    }

    [Fact]
    public async Task ExecuteAsync_WithMultipleRules_ShouldValidateAll()
    {
        // Arrange
        var node = new TestValidationNode();
        node.Register(x => x.Name, name => !string.IsNullOrEmpty(name), "NotEmpty");
        node.Register(x => x.Age, age => age >= 0, "NonNegative");

        var data = new TestData { Name = "Alice", Age = 25 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Should().BeSameAs(data);
    }

    [Fact]
    public async Task ExecuteAsync_WithMultipleRulesFirstFails_ShouldThrowOnFirstFailure()
    {
        // Arrange
        var node = new TestValidationNode();
        node.Register(x => x.Name, name => !string.IsNullOrEmpty(name), "NotEmpty");
        node.Register(x => x.Age, age => age >= 0, "NonNegative");

        var data = new TestData { Name = "", Age = 25 };
        var context = PipelineContext.Default;

        // Act & Assert
        var ex = await Assert.ThrowsAsync<ValidationException>(async () =>
            await node.ExecuteAsync(data, context, CancellationToken.None));

        ex.RuleName.Should().Be("NotEmpty");
    }

    [Fact]
    public async Task ExecuteAsync_WithRegisterMany_ShouldValidateAllProperties()
    {
        // Arrange
        var node = new TestValidationNode();

        node.RegisterMany(
            [x => x.Name, x => x.Email],
            str => !string.IsNullOrEmpty(str),
            "NotEmpty");

        var data = new TestData { Name = "Alice", Email = "alice@test.com" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Should().BeSameAs(data);
    }

    [Fact]
    public async Task ExecuteAsync_WithRegisterManyOneFails_ShouldThrowValidationException()
    {
        // Arrange
        var node = new TestValidationNode();

        node.RegisterMany<string>(
            [x => x.Name, x => x.Email],
            str => !string.IsNullOrEmpty(str),
            "NotEmpty");

        var data = new TestData { Name = "Alice", Email = "" };
        var context = PipelineContext.Default;

        // Act & Assert
        var ex = await Assert.ThrowsAsync<ValidationException>(async () =>
            await node.ExecuteAsync(data, context, CancellationToken.None));

        ex.PropertyPath.Should().Contain("Email");
        ex.RuleName.Should().Be("NotEmpty");
    }

    [Fact]
    public async Task ExecuteAsync_WithNoRules_ShouldReturnItem()
    {
        // Arrange
        var node = new TestValidationNode();
        var data = new TestData { Name = "Test" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Should().BeSameAs(data);
    }

    [Fact]
    public async Task ExecuteAsync_WithCancellationRequested_ShouldThrowOperationCanceledException()
    {
        // Arrange
        var node = new TestValidationNode();
        node.Register(x => x.Name, name => !string.IsNullOrEmpty(name), "NotEmpty");

        var data = new TestData { Name = "Test" };
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
        var node = new TestValidationNode();

        // Act
        var result = node.Register(x => x.Name, name => !string.IsNullOrEmpty(name), "NotEmpty")
            .Register(x => x.Age, age => age >= 0, "NonNegative");

        // Assert
        result.Should().BeSameAs(node);
    }

    [Fact]
    public void RegisterMany_ShouldAllowChaining()
    {
        // Arrange
        var node = new TestValidationNode();

        // Act
        var result = node.RegisterMany<string>(
            [x => x.Name, x => x.Email],
            str => !string.IsNullOrEmpty(str),
            "NotEmpty");

        // Assert
        result.Should().BeSameAs(node);
    }

    private sealed class TestValidationNode : ValidationNode<TestData>
    {
    }

    private sealed class TestData
    {
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public string Email { get; set; } = string.Empty;
    }
}
