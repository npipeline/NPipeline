using AwesomeAssertions;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Testing.Tests;

/// <summary>
///     Example tests demonstrating the new PipelineTestHarness API.
/// </summary>
public class PipelineTestHarnessExampleTests
{
    [Fact]
    public async Task Example_SimplePipeline_WithInMemorySourceAndSink()
    {
        // This example shows how to use PipelineTestHarness with in-memory data
        var result = await new PipelineTestHarness<SimplePipeline>()
            .RunAsync();

        result.AssertSuccess();
        result.AssertNoErrors();
    }

    [Fact]
    public async Task Example_PipelineWithParameters()
    {
        // This example shows passing parameters to the pipeline
        var inputData = new[] { "apple", "banana", "cherry" };

        var result = await new PipelineTestHarness<TransformPipeline>()
            .WithParameter("input_data", inputData)
            .RunAsync();

        result.AssertSuccess();
    }

    [Fact]
    public async Task Example_PipelineWithErrorCapturing()
    {
        // This example shows capturing errors for assertions
        var result = await new PipelineTestHarness<ErrorProducingPipeline>()
            .CaptureErrors()
            .RunAsync();

        // The pipeline executed (didn't throw), but errors were captured
        // With ContinueWithoutNode decision, the failing transform and downstream sink both fail
        result.Errors.Count.Should().BeGreaterThan(0);
    }

    [Fact]
    public async Task Example_PipelineWithExecutionTiming()
    {
        // This example shows asserting on execution duration
        var result = await new PipelineTestHarness<SimplePipeline>()
            .RunAsync();

        result.AssertSuccess();
        result.AssertCompletedWithin(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task Example_PipelineWithInMemorySinkRetrieval()
    {
        // This example shows retrieving results from an in-memory sink
        var result = await new PipelineTestHarness<SinkResultPipeline>()
            .RunAsync();

        result.AssertSuccess();

        // Get the sink and verify results
        var sink = result.GetSink<InMemorySinkNode<int>>();
        var items = await sink.Completion;

        items.Should().HaveCount(3);
        items.Should().BeEquivalentTo(new[] { 2, 4, 6 });
    }

    // --- Pipeline Definitions for Examples ---

    private sealed class SimplePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource(new[] { 1, 2, 3 });
            var sink = builder.AddInMemorySink<int>(context);
            builder.Connect(source, sink);
        }
    }

    private sealed class TransformPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource(new[] { 1, 2, 3 });
            var transform = builder.AddTransform<DoubleTransform, int, int>();
            var sink = builder.AddInMemorySink<int>(context);

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
        }
    }

    private sealed class ErrorProducingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource(new[] { 1, 2, 3 });
            var errorNode = builder.AddTransform<AlwaysFailsTransform, int, int>();
            var sink = builder.AddInMemorySink<int>(context);

            builder.Connect(source, errorNode);
            builder.Connect(errorNode, sink);
        }
    }

    private sealed class SinkResultPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddInMemorySource(new[] { 1, 2, 3 });
            var transform = builder.AddTransform<DoubleTransform, int, int>();
            var sink = builder.AddInMemorySink<int>(context);

            builder.Connect(source, transform);
            builder.Connect(transform, sink);
        }
    }

    // --- Helper Nodes ---

    private sealed class DoubleTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item * 2);
        }
    }

    private sealed class AlwaysFailsTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            throw new InvalidOperationException("This transform always fails for testing");
        }
    }
}
