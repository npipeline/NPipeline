using AwesomeAssertions;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing.Tests;

/// <summary>
///     Integration tests demonstrating real-world pipeline scenarios using PipelineTestHarness.
///     These tests showcase common patterns for testing complete pipeline definitions.
/// </summary>
public sealed class PipelineTestHarnessIntegrationTests
{
    [Fact]
    public async Task SimplePipeline_WithInMemorySourceAndSink_ProcessesDataCorrectly()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<SimpleTransformPipeline>()
            .RunAsync();

        // Assert
        _ = result
            .AssertSuccess()
            .AssertNoErrors()
            .AssertCompletedWithin(TimeSpan.FromSeconds(5));

        var sink = result.GetSink<InMemorySinkNode<string>>();
        var items = await sink.Completion;
        _ = items.Should().HaveCount(3);
        _ = items.Should().BeEquivalentTo("HELLO", "WORLD", "TEST");
    }

    [Fact]
    public async Task Pipeline_WithMultipleTransforms_ChainsCorrectly()
    {
        // Arrange & Act - Chain: Source → First Transform → Second Transform → Sink
        var result = await new PipelineTestHarness<ChainedTransformsPipeline>()
            .RunAsync();

        // Assert
        _ = result.AssertSuccess();

        var sink = result.GetSink<InMemorySinkNode<int>>();
        var items = await sink.Completion;

        // Input: [1, 2, 3] → Multiply by 2: [2, 4, 6] → Add 1: [3, 5, 7]
        _ = items.Should().BeEquivalentTo(new[] { 3, 5, 7 });
    }

    [Fact]
    public async Task Pipeline_WithMultipleTransformsInSequence_ProcessesAllItems()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<TripleTransformPipeline>()
            .RunAsync();

        // Assert
        _ = result.AssertSuccess();

        var sink = result.GetSink<InMemorySinkNode<int>>();
        var items = await sink.Completion;

        // Input: [1, 2, 3] → +10: [11, 12, 13] → *2: [22, 24, 26] → -1: [21, 23, 25]
        _ = items.Should().BeEquivalentTo(new[] { 21, 23, 25 });
    }

    [Fact]
    public async Task Pipeline_WithErrorCapturing_CapturesErrors()
    {
        // Arrange & Act - Use error capturing to prevent exception from propagating
        var result = await new PipelineTestHarness<ErrorProducingPipeline>()
            .CaptureErrors() // Enable error capturing
            .RunAsync();

        // Assert - Errors were captured even though exception didn't propagate
        _ = result.Errors.Should().NotBeEmpty("pipeline executed with error capturing enabled");
    }

    [Fact]
    public async Task Pipeline_ExecutionTiming_IsTracked()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<SimpleTransformPipeline>()
            .RunAsync();

        // Assert
        _ = result.Duration.Should().BeGreaterThan(TimeSpan.Zero);
        _ = result.Duration.Should().BeLessThan(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Pipeline_ContextItems_AreAccessibleInResults()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<SimpleTransformPipeline>()
            .WithContextItem("test_key", "test_value")
            .RunAsync();

        // Assert
        _ = result
            .TryGetContextItem<string>("test_key", out var value)
            .Should()
            .BeTrue();

        _ = value.Should().Be("test_value");
    }

    [Fact]
    public async Task Pipeline_WithContextItems_PreservesCustomValues()
    {
        // Arrange & Act
        var customObject = new object();

        var result = await new PipelineTestHarness<SimpleTransformPipeline>()
            .WithContextItem("custom_obj", customObject)
            .RunAsync();

        // Assert
        var retrieved = result.Context.Items["custom_obj"];
        _ = retrieved.Should().Be(customObject);
    }

    [Fact]
    public async Task Pipeline_WithMultipleContextItems_MaintainsAll()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<SimpleTransformPipeline>()
            .WithContextItem("key1", "value1")
            .WithContextItem("key2", 42)
            .WithContextItem("key3", true)
            .RunAsync();

        // Assert
        _ = result.Context.Items["key1"].Should().Be("value1");
        _ = result.Context.Items["key2"].Should().Be(42);
        _ = result.Context.Items["key3"].Should().Be(true);
    }

    [Fact]
    public async Task Pipeline_FluentAssertions_ChainMultipleChecks()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<SimpleTransformPipeline>()
            .RunAsync();

        // Assert - Fluent chaining allows multiple assertions in sequence
        _ = result
            .AssertSuccess()
            .AssertNoErrors()
            .AssertCompletedWithin(TimeSpan.FromSeconds(5));

        // Still able to access sink after assertions
        var sink = result.GetSink<InMemorySinkNode<string>>();
        var items = await sink.Completion;
        _ = items.Should().NotBeEmpty();
    }

    [Fact]
    public async Task Pipeline_ErrorCapturing_DefaultDecision()
    {
        // Arrange & Act - Default decision is ContinueWithoutNode
        var result = await new PipelineTestHarness<ErrorProducingPipeline>()
            .CaptureErrors() // Uses default PipelineErrorDecision.ContinueWithoutNode
            .RunAsync();

        // Assert
        _ = result.Errors.Should().NotBeEmpty();
    }

    [Fact]
    public async Task Pipeline_SuccessfulExecution_HasNoErrors()
    {
        // Arrange & Act
        var result = await new PipelineTestHarness<SimpleTransformPipeline>()
            .CaptureErrors()
            .RunAsync();

        // Assert
        _ = result.Success.Should().BeTrue();
        _ = result.Errors.Should().BeEmpty();
    }

    [Fact]
    public async Task Pipeline_WithParameters_PassesValuesCorrectly()
    {
        // Arrange
        var multiplier = 5;

        // Act
        var result = await new PipelineTestHarness<ParameterizedTransformPipeline>()
            .WithParameter("multiplier", multiplier)
            .RunAsync();

        // Assert
        _ = result.AssertSuccess();

        var sink = result.GetSink<InMemorySinkNode<int>>();
        var items = await sink.Completion;

        // Input: [1, 2, 3] → Multiply by 5: [5, 10, 15]
        _ = items.Should().BeEquivalentTo(new[] { 5, 10, 15 });
    }

    [Fact]
    public async Task Pipeline_WithLargeDataSet_ProcessesAllItems()
    {
        // Arrange
        var largeDataSet = Enumerable.Range(1, 1000).ToArray();

        // Act
        var result = await new PipelineTestHarness<SimpleTransformPipeline>()
            .WithParameter("source_data", largeDataSet)
            .RunAsync();

        // Assert
        _ = result.AssertSuccess();
        _ = result.Duration.Should().BeLessThan(TimeSpan.FromSeconds(10));
    }

    // ============= Pipeline Definitions =============

    private sealed class SimpleTransformPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource(new[] { "hello", "world", "test" });
            var transform = builder.AddTransform<ToUpperTransform, string, string>();
            var sink = builder.AddInMemorySink<string>(context);

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
        }
    }

    private sealed class ChainedTransformsPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource(new[] { 1, 2, 3 });
            var multiplyTransform = builder.AddTransform<MultiplyByTwoTransform, int, int>();
            var addTransform = builder.AddTransform<AddOneTransform, int, int>();
            var sink = builder.AddInMemorySink<int>(context);

            builder.Connect(source, multiplyTransform);
            builder.Connect(multiplyTransform, addTransform);
            builder.Connect(addTransform, sink);
        }
    }

    private sealed class TripleTransformPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource(new[] { 1, 2, 3 });
            var addTransform = builder.AddTransform<AddTenTransform, int, int>();
            var multiplyTransform = builder.AddTransform<MultiplyByTwoTransform, int, int>();
            var subtractTransform = builder.AddTransform<SubtractOneTransform, int, int>();
            var sink = builder.AddInMemorySink<int>(context);

            builder.Connect(source, addTransform);
            builder.Connect(addTransform, multiplyTransform);
            builder.Connect(multiplyTransform, subtractTransform);
            builder.Connect(subtractTransform, sink);
        }
    }

    private sealed class ErrorProducingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource(new[] { 1, 2, 3 });
            var errorTransform = builder.AddTransform<FailOnTwoTransform, int, int>();
            var sink = builder.AddInMemorySink<int>(context);

            builder.Connect(source, errorTransform);
            builder.Connect(errorTransform, sink);
        }
    }

    private sealed class ParameterizedTransformPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource(new[] { 1, 2, 3 });
            var transform = builder.AddTransform<ParameterizedMultiplyTransform, int, int>();
            var sink = builder.AddInMemorySink<int>(context);

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
        }
    }

    // ============= Transform Nodes =============

    private sealed class ToUpperTransform : TransformNode<string, string>
    {
        public override Task<string> ExecuteAsync(string item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item.ToUpperInvariant());
        }
    }

    private sealed class MultiplyByTwoTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item * 2);
        }
    }

    private sealed class AddOneTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item + 1);
        }
    }

    private sealed class AddTenTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item + 10);
        }
    }

    private sealed class SubtractOneTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item - 1);
        }
    }

    private sealed class FailOnTwoTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (item == 2)
                throw new InvalidOperationException("Intentional failure on item 2");

            return Task.FromResult(item);
        }
    }

    private sealed class ParameterizedMultiplyTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            if (!context.Parameters.TryGetValue("multiplier", out var multiplierObj) || multiplierObj is not int multiplier)
                throw new InvalidOperationException("Missing or invalid 'multiplier' parameter");

            return Task.FromResult(item * multiplier);
        }
    }
}
