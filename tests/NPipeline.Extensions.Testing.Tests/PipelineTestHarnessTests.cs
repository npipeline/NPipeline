using AwesomeAssertions;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing.Tests;

/// <summary>
///     Tests for PipelineTestHarness functionality.
/// </summary>
public class PipelineTestHarnessTests
{
    [Fact]
    public async Task RunAsync_WithSuccessfulPipeline_ReturnsSuccessfulResult()
    {
        // Arrange
        var result = await new PipelineTestHarness<SimplePipeline>()
            .RunAsync();

        // Assert
        result.AssertSuccess();
        result.AssertNoErrors();
        result.Duration.Should().BeGreaterThan(TimeSpan.Zero);
    }

    [Fact]
    public async Task RunAsync_WithFailingPipeline_CapturesError()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<FailingPipeline>()
            .CaptureErrors()
            .RunAsync();

        // Assert - the error is captured and allows execution to continue
        // When ContinueWithoutNode decision is used, the error from the failed node is captured,
        // then the downstream sink also fails when it tries to process the failed connection
        result.Errors.Should().HaveCount(2);
    }

    [Fact]
    public async Task WithParameter_AddsParameterToContext()
    {
        // Arrange
        var expectedKey = "test_param";
        var expectedValue = "test_value";

        // Act
        var result = await new PipelineTestHarness<SimplePipeline>()
            .WithParameter(expectedKey, expectedValue)
            .RunAsync();

        // Assert
        result.Context.Parameters[expectedKey].Should().Be(expectedValue);
    }

    [Fact]
    public async Task WithParameters_AddsMultipleParametersToContext()
    {
        // Arrange
        var parameters = new Dictionary<string, object>
        {
            { "param1", "value1" },
            { "param2", 42 },
            { "param3", true },
        };

        // Act
        var result = await new PipelineTestHarness<SimplePipeline>()
            .WithParameters(parameters)
            .RunAsync();

        // Assert
        result.Context.Parameters["param1"].Should().Be("value1");
        result.Context.Parameters["param2"].Should().Be(42);
        result.Context.Parameters["param3"].Should().Be(true);
    }

    [Fact]
    public async Task WithContextItem_AddsItemToContext()
    {
        // Arrange
        var expectedKey = "test_item";
        var expectedValue = new object();

        // Act
        var result = await new PipelineTestHarness<SimplePipeline>()
            .WithContextItem(expectedKey, expectedValue)
            .RunAsync();

        // Assert
        result.Context.Items[expectedKey].Should().Be(expectedValue);
    }

    [Fact]
    public async Task AssertCompletedWithin_PassesForFastPipeline()
    {
        // Act
        var result = await new PipelineTestHarness<SimplePipeline>()
            .RunAsync();

        // Assert - should not throw
        result.AssertCompletedWithin(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task GetSink_ReturnsInMemorySinkFromContext()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<PipelineWithSink>()
            .RunAsync();

        // Assert
        var sink = result.GetSink<InMemorySinkNode<int>>();
        sink.Should().NotBeNull();
    }

    [Fact]
    public async Task TryGetContextItem_ReturnsTrueForExistingItem()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<SimplePipeline>()
            .WithContextItem("test_key", "test_value")
            .RunAsync();

        // Assert
        var success = result.TryGetContextItem<string>("test_key", out var value);
        success.Should().BeTrue();
        value.Should().Be("test_value");
    }

    [Fact]
    public async Task TryGetContextItem_ReturnsFalseForMissingItem()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<SimplePipeline>()
            .RunAsync();

        // Assert
        var success = result.TryGetContextItem<string>("nonexistent_key", out var value);
        success.Should().BeFalse();
        value.Should().BeNull();
    }

    // --- Test Pipeline Definitions ---

    private sealed class SimplePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource(new[] { 1, 2, 3 });
            var sink = builder.AddInMemorySink<int>(context);
            builder.Connect(source, sink);
        }
    }

    private sealed class FailingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource(new[] { 1 });
            var failing = builder.AddTransform<AlwaysFailsTransform, int, int>();
            var sink = builder.AddInMemorySink<int>(context);

            builder.Connect(source, failing);
            builder.Connect(failing, sink);
        }
    }

    private sealed class PipelineWithSink : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource(new[] { 1, 2, 3 });
            var sink = builder.AddInMemorySink<int>(context);
            builder.Connect(source, sink);
        }
    }

    private sealed class AlwaysFailsTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            throw new InvalidOperationException("Expected failure for test");
        }
    }
}
