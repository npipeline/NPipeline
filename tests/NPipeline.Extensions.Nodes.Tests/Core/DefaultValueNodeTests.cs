using AwesomeAssertions;
using NPipeline.Extensions.Nodes.Core;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Nodes.Tests.Core;

/// <summary>
/// Tests for <see cref="DefaultValueNode{T}"/> covering all default value operations.
/// 
/// Scenarios tested:
/// - DefaultIfNull: Setting defaults for null values
/// - DefaultIfNullOrEmpty: Setting defaults for null or empty strings
/// - DefaultIfNullOrWhitespace: Setting defaults for null, empty, or whitespace strings
/// - DefaultIfDefault: Setting defaults for default(T) values
/// - DefaultIfCondition: Setting defaults based on custom conditions
/// - DefaultIfZero: Setting defaults for zero values (int, decimal, double)
/// - DefaultIfEmpty: Setting defaults for empty collections
/// - Chaining: Multiple default operations in sequence
/// - Cancellation: Pipeline cancellation support
/// - No-ops: Nodes with no registered defaults
/// </summary>
public sealed class DefaultValueNodeTests
{
    [Fact]
    public async Task DefaultIfNull_WithNullValue_ShouldSetDefault()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfNull(x => x.Name, "DefaultName");

        var data = new TestData { Name = null };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("DefaultName");
    }

    [Fact]
    public async Task DefaultIfNull_WithNonNullValue_ShouldNotChange()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfNull(x => x.Name, "DefaultName");

        var data = new TestData { Name = "John" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("John");
    }

    [Fact]
    public async Task DefaultIfNullOrEmpty_WithEmptyString_ShouldSetDefault()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfNullOrEmpty(x => x.Name, "DefaultName");

        var data = new TestData { Name = "" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("DefaultName");
    }

    [Fact]
    public async Task DefaultIfNullOrEmpty_WithNullString_ShouldSetDefault()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfNullOrEmpty(x => x.Name, "DefaultName");

        var data = new TestData { Name = null };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("DefaultName");
    }

    [Fact]
    public async Task DefaultIfNullOrWhitespace_WithWhitespaceString_ShouldSetDefault()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfNullOrWhitespace(x => x.Name, "DefaultName");

        var data = new TestData { Name = "   " };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("DefaultName");
    }

    [Fact]
    public async Task DefaultIfNullOrWhitespace_WithValidString_ShouldNotChange()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfNullOrWhitespace(x => x.Name, "DefaultName");

        var data = new TestData { Name = "John" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("John");
    }

    [Fact]
    public async Task DefaultIfDefault_WithDefaultValue_ShouldSetDefault()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfDefault(x => x.Age, 18);

        var data = new TestData { Age = 0 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Age.Should().Be(18);
    }

    [Fact]
    public async Task DefaultIfDefault_WithNonDefaultValue_ShouldNotChange()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfDefault(x => x.Age, 18);

        var data = new TestData { Age = 25 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Age.Should().Be(25);
    }

    [Fact]
    public async Task DefaultIfCondition_WithConditionTrue_ShouldSetDefault()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfCondition(x => x.Age, age => age < 18, 18);

        var data = new TestData { Age = 10 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Age.Should().Be(18);
    }

    [Fact]
    public async Task DefaultIfCondition_WithConditionFalse_ShouldNotChange()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfCondition(x => x.Age, age => age < 18, 18);

        var data = new TestData { Age = 25 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Age.Should().Be(25);
    }

    [Fact]
    public async Task DefaultIfZero_WithZeroInteger_ShouldSetDefault()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfZero(x => x.Age, 18);

        var data = new TestData { Age = 0 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Age.Should().Be(18);
    }

    [Fact]
    public async Task DefaultIfZero_WithNonZeroInteger_ShouldNotChange()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfZero(x => x.Age, 18);

        var data = new TestData { Age = 25 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Age.Should().Be(25);
    }

    [Fact]
    public async Task DefaultIfZero_WithZeroDecimal_ShouldSetDefault()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfZero(x => x.Price, 9.99m);

        var data = new TestData { Price = 0m };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Price.Should().Be(9.99m);
    }

    [Fact]
    public async Task DefaultIfZero_WithZeroDouble_ShouldSetDefault()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfZero(x => x.Rating, 5.0);

        var data = new TestData { Rating = 0.0 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Rating.Should().Be(5.0);
    }

    [Fact]
    public async Task DefaultIfEmpty_WithEmptyCollection_ShouldSetDefault()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        var defaultTags = new[] { "default" };
        node.DefaultIfEmpty(x => x.Tags, defaultTags);

        var data = new TestData { Tags = Array.Empty<string>() };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Tags.Should().Equal("default");
    }

    [Fact]
    public async Task DefaultIfEmpty_WithNullCollection_ShouldSetDefault()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        var defaultTags = new[] { "default" };
        node.DefaultIfEmpty(x => x.Tags, defaultTags);

        var data = new TestData { Tags = null! };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Tags.Should().Equal("default");
    }

    [Fact]
    public async Task DefaultIfEmpty_WithNonEmptyCollection_ShouldNotChange()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        var defaultTags = new[] { "default" };
        node.DefaultIfEmpty(x => x.Tags, defaultTags);

        var data = new TestData { Tags = new[] { "tag1", "tag2" } };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Tags.Should().Equal("tag1", "tag2");
    }

    [Fact]
    public async Task ChainedOperations_WithMultipleDefaults_ShouldApplyAllDefaults()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfNull(x => x.Name, "UnknownName")
            .DefaultIfZero(x => x.Age, 18)
            .DefaultIfNullOrEmpty(x => x.Email, "noemail@example.com");

        var data = new TestData { Name = null, Age = 0, Email = "" };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Name.Should().Be("UnknownName");
        result.Age.Should().Be(18);
        result.Email.Should().Be("noemail@example.com");
    }

    [Fact]
    public async Task ExecuteAsync_WithCancellation_ShouldThrowOperationCanceledException()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfNull(x => x.Name, "Default");

        var data = new TestData();
        var context = PipelineContext.Default;
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await node.ExecuteAsync(data, context, cts.Token));
    }

    [Fact]
    public async Task ExecuteAsync_WithNoOperations_ShouldReturnUnchanged()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        var data = new TestData { Name = "Test", Age = 25 };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.Should().BeSameAs(data);
        result.Name.Should().Be("Test");
        result.Age.Should().Be(25);
    }

    [Fact]
    public async Task DefaultIfNull_WithIntegerProperty_ShouldSetDefault()
    {
        // Arrange
        var node = new DefaultValueNode<TestData>();
        node.DefaultIfNull(x => x.OptionalValue, 999);

        var data = new TestData { OptionalValue = null };
        var context = PipelineContext.Default;

        // Act
        var result = await node.ExecuteAsync(data, context, CancellationToken.None);

        // Assert
        result.OptionalValue.Should().Be(999);
    }

    private sealed class TestData
    {
        public string? Name { get; set; }
        public int Age { get; set; }
        public string? Email { get; set; }
        public decimal Price { get; set; }
        public double Rating { get; set; }
        public IEnumerable<string> Tags { get; set; } = [];
        public int? OptionalValue { get; set; }
    }
}
