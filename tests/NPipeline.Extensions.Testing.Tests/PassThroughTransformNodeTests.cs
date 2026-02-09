using AwesomeAssertions;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing.Tests;

public class PassThroughTransformNodeTests
{
    [Fact]
    public async Task ExecuteAsync_WithValidTypeConversion_ShouldReturnOriginalItem()
    {
        // Arrange
        var node = new PassThroughTransformNode<string, object>();
        var context = PipelineContext.Default;
        var item = "test";

        // Act
        var result = await node.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        result.Should().Be(item);
    }

    [Fact]
    public async Task ExecuteAsync_WithReferenceTypeConversion_ShouldReturnOriginalItem()
    {
        // Arrange
        var node = new PassThroughTransformNode<object, string>();
        var context = PipelineContext.Default;
        var item = "test" as object;

        // Act
        var result = await node.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        result.Should().Be(item as string);
    }

    [Fact]
    public async Task ExecuteAsync_WithValueTypeConversion_ShouldReturnOriginalItem()
    {
        // Arrange
        var node = new PassThroughTransformNode<int, long>();
        var context = PipelineContext.Default;
        var item = 42;

        // Act
        var result = await node.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        result.Should().Be(item);
    }

    [Fact]
    public async Task ExecuteAsync_WithNullReferenceType_ShouldReturnNull()
    {
        // Arrange
        var node = new PassThroughTransformNode<string, object>();
        var context = PipelineContext.Default;
        string? item = null;

        // Act
        var result = await node.ExecuteAsync(item!, context, CancellationToken.None);

        // Assert
        result.Should().BeNull();
    }

    [Fact]
    public async Task ExecuteAsync_WithNullableValueType_ShouldReturnNull()
    {
        // Arrange
        var node = new PassThroughTransformNode<int?, int>();
        var context = PipelineContext.Default;
        int? item = null;

        // Act
        var result = await node.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        result.Should().Be(0); // Default value for int when null is cast
    }

    [Fact]
    public async Task ExecuteAsync_WithInvalidCast_ShouldThrowInvalidCastException()
    {
        // Arrange
        var node = new PassThroughTransformNode<string, int>();
        var context = PipelineContext.Default;
        var item = "not a number";

        // Act & Assert
        await Assert.ThrowsAsync<InvalidCastException>(() => node.ExecuteAsync(item, context, CancellationToken.None));
    }

    [Fact]
    public async Task ExecuteAsync_WithIncompatibleTypes_ShouldThrowInvalidCastException()
    {
        // Arrange
        var node = new PassThroughTransformNode<List<string>, Dictionary<string, int>>();
        var context = PipelineContext.Default;
        var item = new List<string> { "test" };

        // Act & Assert
        await Assert.ThrowsAsync<InvalidCastException>(() => node.ExecuteAsync(item, context, CancellationToken.None));
    }

    [Fact]
    public async Task ExecuteAsync_WithSameTypes_ShouldReturnSameInstance()
    {
        // Arrange
        var node = new PassThroughTransformNode<string, string>();
        var context = PipelineContext.Default;
        var item = "test";

        // Act
        var result = await node.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        result.Should().Be(item);
        result.Should().BeSameAs(item);
    }

    [Fact]
    public async Task ExecuteAsync_WithComplexType_ShouldReturnSameInstance()
    {
        // Arrange
        var node = new PassThroughTransformNode<TestObject, TestObject>();
        var context = PipelineContext.Default;
        var item = new TestObject { Name = "test", Value = 42 };

        // Act
        var result = await node.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        result.Should().Be(item);
        result.Should().BeSameAs(item);
        result.Name.Should().Be("test");
        result.Value.Should().Be(42);
    }

    [Fact]
    public async Task ExecuteAsync_WithComplexTypeConversion_ShouldReturnSameInstance()
    {
        // Arrange
        var node = new PassThroughTransformNode<TestObject, object>();
        var context = PipelineContext.Default;
        var item = new TestObject { Name = "test", Value = 42 };

        // Act
        var result = await node.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        result.Should().Be(item);
        result.Should().BeSameAs(item);

        var testObject = result as TestObject;
        testObject.Should().NotBeNull();
        testObject!.Name.Should().Be("test");
        testObject.Value.Should().Be(42);
    }

    [Fact]
    public async Task ExecuteAsync_WithCancellation_ShouldRespectCancellation()
    {
        // Arrange
        var node = new PassThroughTransformNode<string, object>();
        var context = PipelineContext.Default;
        var item = "test";
        var cts = new CancellationTokenSource();
        cts.Cancel();

        // Act & Assert
        await Assert.ThrowsAsync<OperationCanceledException>(() => node.ExecuteAsync(item, context, cts.Token));
    }

    [Fact]
    public void Constructor_ShouldCreateNodeSuccessfully()
    {
        // Arrange & Act
        var node = new PassThroughTransformNode<string, object>();

        // Assert
        node.Should().NotBeNull();
    }

    [Fact]
    public async Task ExecuteAsync_WithContext_ShouldNotUseContext()
    {
        // Arrange
        var node = new PassThroughTransformNode<string, object>();
        var context = PipelineContext.Default;
        context.Items["test"] = "value";
        var item = "test";

        // Act
        var result = await node.ExecuteAsync(item, context, CancellationToken.None);

        // Assert
        result.Should().Be(item);

        // The context should not be modified by the transform
        context.Items.Should().ContainKey("test");
        context.Items["test"].Should().Be("value");
    }

    [Fact]
    public async Task ExecuteAsync_WithNullContext_ShouldStillWork()
    {
        // Arrange
        var node = new PassThroughTransformNode<string, object>();
        var item = "test";

        // Act & Assert
        // Even though context is null, the transform should still work
        // since it doesn't use the context
        var result = await node.ExecuteAsync(item, null!, CancellationToken.None);
        result.Should().Be(item);
    }

    // Helper class for testing complex types
    private sealed class TestObject
    {
        public string? Name { get; set; }
        public int Value { get; set; }
    }
}
