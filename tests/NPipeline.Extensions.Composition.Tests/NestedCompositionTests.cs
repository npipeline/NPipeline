using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit;

namespace NPipeline.Extensions.Composition.Tests;

/// <summary>
///     Tests for nested composite nodes.
/// </summary>
public class NestedCompositionTests
{
    [Fact]
    public async Task NestedComposite_TwoLevelsDeep_ShouldExecuteSuccessfully()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();

        // Act
        await runner.RunAsync<OuterPipeline>(context);

        // Assert
        CollectorSink.CollectedValues.Should().HaveCount(3);
        CollectorSink.CollectedValues.Should().Equal(4, 8, 12); // Each input * 2 * 2
    }

    [Fact]
    public async Task NestedComposite_ThreeLevelsDeep_ShouldExecuteSuccessfully()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();

        // Act
        await runner.RunAsync<DeepNestingPipeline>(context);

        // Assert
        CollectorSink.CollectedValues.Should().HaveCount(2);
        CollectorSink.CollectedValues.Should().Equal(8, 16); // Each input * 2 * 2 * 2
    }

    [Fact]
    public async Task NestedComposite_WithContextInheritance_ShouldPropagateContextThroughLevels()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();
        context.Parameters["NestedValue"] = "InheritedThroughLevels";

        // Act
        await runner.RunAsync<NestedContextAwarePipeline>(context);

        // Assert
        ContextCaptureTransform.CapturedValue.Should().Be("InheritedThroughLevels");
    }

    // Test helper nodes and pipelines

    private sealed class IntSource : ISourceNode<int>
    {
        public IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            return new InMemoryDataPipe<int>([1, 2, 3], "IntSource");
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class SmallIntSource : ISourceNode<int>
    {
        public IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            return new InMemoryDataPipe<int>([1, 2], "SmallIntSource");
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class CollectorSink : ISinkNode<int>
    {
        public static readonly List<int> CollectedValues = [];

        public async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            CollectedValues.Clear();

            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                CollectedValues.Add(item);
            }
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class DoubleTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(input * 2);
        }
    }

    // Inner sub-pipeline (multiplies by 2)
    private sealed class InnerSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<DoubleTransform, int, int>("double");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    // Middle sub-pipeline (contains another composite node, then doubles again)
    private sealed class MiddleSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");

            var composite = builder.AddComposite<int, int, InnerSubPipeline>(
                "inner-composite",
                CompositeContextConfiguration.Default);

            var transform = builder.AddTransform<DoubleTransform, int, int>("double-again");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, composite);
            builder.Connect(composite, transform);
            builder.Connect(transform, output);
        }
    }

    // Outer pipeline (uses middle sub-pipeline)
    private sealed class OuterPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<IntSource, int>("source");

            var composite = builder.AddComposite<int, int, MiddleSubPipeline>(
                "middle-composite",
                CompositeContextConfiguration.Default);

            var sink = builder.AddSink<CollectorSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    // Three levels deep

    private sealed class DeepestSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<DoubleTransform, int, int>("double");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class MiddleDeepSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");

            var composite = builder.AddComposite<int, int, DeepestSubPipeline>(
                "deepest-composite",
                CompositeContextConfiguration.Default);

            var transform = builder.AddTransform<DoubleTransform, int, int>("double-again");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, composite);
            builder.Connect(composite, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class OuterDeepSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");

            var composite = builder.AddComposite<int, int, MiddleDeepSubPipeline>(
                "middle-deep-composite",
                CompositeContextConfiguration.Default);

            var transform = builder.AddTransform<DoubleTransform, int, int>("double-once-more");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, composite);
            builder.Connect(composite, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class DeepNestingPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<SmallIntSource, int>("source");

            var composite = builder.AddComposite<int, int, OuterDeepSubPipeline>(
                "outer-deep-composite",
                CompositeContextConfiguration.Default);

            var sink = builder.AddSink<CollectorSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    // Context inheritance tests

    private sealed class ContextCaptureTransform : TransformNode<int, int>
    {
        public static string? CapturedValue { get; private set; }

        public override Task<int> ExecuteAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            CapturedValue = context.Parameters.TryGetValue("NestedValue", out var value)
                ? value?.ToString()
                : null;

            return Task.FromResult(input);
        }
    }

    private sealed class InnerContextAwareSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<ContextCaptureTransform, int, int>("capture");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class MiddleContextAwareSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");

            var composite = builder.AddComposite<int, int, InnerContextAwareSubPipeline>(
                "inner-context-aware",
                CompositeContextConfiguration.InheritAll);

            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, composite);
            builder.Connect(composite, output);
        }
    }

    private sealed class NestedContextAwarePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<SmallIntSource, int>("source");

            var composite = builder.AddComposite<int, int, MiddleContextAwareSubPipeline>(
                "middle-context-aware",
                CompositeContextConfiguration.InheritAll);

            var sink = builder.AddSink<CollectorSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }
}
