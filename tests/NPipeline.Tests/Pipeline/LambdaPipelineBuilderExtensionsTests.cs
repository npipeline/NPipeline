using System.Runtime.CompilerServices;
using AwesomeAssertions;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Pipeline;

/// <summary>
///     Tests for lambda-based pipeline builder extensions.
///     Validates that the fluent API for creating simple pipelines works correctly.
/// </summary>
public sealed class LambdaPipelineBuilderExtensionsTests
{
    [Fact]
    public void AddTransform_WithLambda_ReturnsValidHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddTransform((int x) => x * 2, "double");

        // Assert
        _ = handle.Should().NotBeNull();
    }

    [Fact]
    public void AddTransform_WithAsyncLambda_ReturnsValidHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddTransform(
            async (int x, CancellationToken ct) =>
            {
                await Task.Delay(1, ct);
                return x * 2;
            },
            "asyncDouble");

        // Assert
        _ = handle.Should().NotBeNull();
    }

    [Fact]
    public void AddSource_WithSyncFactory_ReturnsValidHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var handle = builder.AddSource(() => new[] { 1, 2, 3 }, "numbers");

        // Assert
        _ = handle.Should().NotBeNull();
    }

    [Fact]
    public void AddSource_WithAsyncFactory_ReturnsValidHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Use local async function instead of lambda
        static async IAsyncEnumerable<int> AsyncFactory([EnumeratorCancellation] CancellationToken ct)
        {
            await Task.Delay(1, ct);

            for (var i = 1; i <= 3; i++)
            {
                yield return i;
            }
        }

        // Act
        var handle = builder.AddSource(AsyncFactory, "asyncNumbers");

        // Assert
        _ = handle.Should().NotBeNull();
    }

    [Fact]
    public void AddSink_WithAction_ReturnsValidHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var items = new List<int>();

        // Act
        var handle = builder.AddSink((int item) => items.Add(item), "collector");

        // Assert
        _ = handle.Should().NotBeNull();
    }

    [Fact]
    public void AddSink_WithAsyncConsumer_ReturnsValidHandle()
    {
        // Arrange
        var builder = new PipelineBuilder();
        var items = new List<int>();

        // Act
        var handle = builder.AddSink(
            async (int item, CancellationToken ct) =>
            {
                await Task.Delay(1, ct);
                items.Add(item);
            },
            "asyncCollector");

        // Assert
        _ = handle.Should().NotBeNull();
    }

    [Fact]
    public void AddTransform_WithNullSyncTransform_Throws()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Func<int, int> nullTransform = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() =>
            builder.AddTransform(nullTransform, "test"));
    }

    [Fact]
    public void AddTransform_WithNullAsyncTransform_Throws()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Func<int, CancellationToken, ValueTask<int>> nullTransform = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() =>
            builder.AddTransform(nullTransform, "test"));
    }

    [Fact]
    public void AddSource_WithNullSyncFactory_Throws()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Func<IEnumerable<int>> nullFactory = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() =>
            builder.AddSource(nullFactory, "test"));
    }

    [Fact]
    public void AddSource_WithNullAsyncFactory_Throws()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Func<CancellationToken, IAsyncEnumerable<int>> nullFactory = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() =>
            builder.AddSource(nullFactory, "test"));
    }

    [Fact]
    public void AddSink_WithNullSyncConsumer_Throws()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Action<int> nullConsumer = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() =>
            builder.AddSink(nullConsumer, "test"));
    }

    [Fact]
    public void AddSink_WithNullAsyncConsumer_Throws()
    {
        // Arrange
        var builder = new PipelineBuilder();
        Func<int, CancellationToken, ValueTask> nullConsumer = null!;

        // Act & Assert
        _ = Assert.Throws<ArgumentNullException>(() =>
            builder.AddSink(nullConsumer, "test"));
    }

    [Fact]
    public void SimplePipeline_WithLambdaNodes_BuildsSuccessfully()
    {
        // Arrange
        var builder = new PipelineBuilder();

        // Act
        var source = builder.AddSource(() => new[] { 1, 2, 3 }, "source");
        var transform = builder.AddTransform((int x) => x * 2, "double");
        var sink = builder.AddSink((int x) => { }, "sink");

        _ = builder.Connect(source, transform);
        _ = builder.Connect(transform, sink);

        var graph = builder.Build();

        // Assert
        _ = graph.Should().NotBeNull();
    }
}
