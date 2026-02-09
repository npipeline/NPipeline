using AwesomeAssertions;
using NPipeline.Pipeline;
using Xunit;

namespace NPipeline.Extensions.Composition.Tests;

public class PipelineInputSourceTests
{
    [Fact]
    public async Task Initialize_WithValidInputInContext_ShouldReturnInputItem()
    {
        // Arrange
        var source = new PipelineInputSource<int>();
        var context = new PipelineContext();
        var expectedValue = 42;
        context.Parameters[CompositeContextKeys.InputItem] = expectedValue;

        // Act
        var result = source.Initialize(context, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        var items = await result.ToListAsync();
        items.Should().HaveCount(1);
        items[0].Should().Be(expectedValue);
    }

    [Fact]
    public void Initialize_WithMissingInputInContext_ShouldThrowInvalidOperationException()
    {
        // Arrange
        var source = new PipelineInputSource<int>();
        var context = new PipelineContext();

        // Act & Assert
        Assert.Throws<InvalidOperationException>(() =>
            source.Initialize(context, CancellationToken.None));
    }

    [Fact]
    public void Initialize_WithWrongTypeInContext_ShouldThrowInvalidCastException()
    {
        // Arrange
        var source = new PipelineInputSource<int>();
        var context = new PipelineContext();
        context.Parameters[CompositeContextKeys.InputItem] = "not an int";

        // Act & Assert
        Assert.Throws<InvalidCastException>(() =>
            source.Initialize(context, CancellationToken.None));
    }

    [Fact]
    public void Initialize_WithNullInputInContext_ShouldThrowInvalidCastException()
    {
        // Arrange
        var source = new PipelineInputSource<int>();
        var context = new PipelineContext();
        context.Parameters[CompositeContextKeys.InputItem] = null!;

        // Act & Assert
        Assert.Throws<InvalidCastException>(() =>
            source.Initialize(context, CancellationToken.None));
    }

    [Fact]
    public async Task Initialize_WithNullReferenceInput_ShouldReturnNullItem()
    {
        // Arrange
        var source = new PipelineInputSource<string?>();
        var context = new PipelineContext();
        context.Parameters[CompositeContextKeys.InputItem] = null!;

        // Act
        var result = source.Initialize(context, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        var items = await result.ToListAsync();
        items.Should().HaveCount(1);
        items[0].Should().BeNull();
    }

    [Fact]
    public async Task Initialize_WithNullNullableValueType_ShouldReturnNullItem()
    {
        // Arrange
        var source = new PipelineInputSource<int?>();
        var context = new PipelineContext();
        context.Parameters[CompositeContextKeys.InputItem] = null!;

        // Act
        var result = source.Initialize(context, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        var items = await result.ToListAsync();
        items.Should().HaveCount(1);
        items[0].Should().BeNull();
    }

    [Fact]
    public async Task Initialize_WithReferenceType_ShouldReturnInputItem()
    {
        // Arrange
        var source = new PipelineInputSource<string>();
        var context = new PipelineContext();
        var expectedValue = "test input";
        context.Parameters[CompositeContextKeys.InputItem] = expectedValue;

        // Act
        var result = source.Initialize(context, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        var items = await result.ToListAsync();
        items.Should().HaveCount(1);
        items[0].Should().Be(expectedValue);
    }

    [Fact]
    public async Task Initialize_WithComplexType_ShouldReturnInputItem()
    {
        // Arrange
        var source = new PipelineInputSource<TestData>();
        var context = new PipelineContext();
        var expectedValue = new TestData { Id = 1, Name = "Test" };
        context.Parameters[CompositeContextKeys.InputItem] = expectedValue;

        // Act
        var result = source.Initialize(context, CancellationToken.None);

        // Assert
        result.Should().NotBeNull();
        var items = await result.ToListAsync();
        items.Should().HaveCount(1);
        items[0].Should().Be(expectedValue);
        items[0].Id.Should().Be(1);
        items[0].Name.Should().Be("Test");
    }

    private sealed class TestData
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
