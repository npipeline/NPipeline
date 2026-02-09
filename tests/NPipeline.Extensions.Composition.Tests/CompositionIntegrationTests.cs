using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit;

namespace NPipeline.Extensions.Composition.Tests;

public class CompositionIntegrationTests
{
    [Fact]
    public async Task CompositeNode_ShouldExecuteSubPipelineSuccessfully()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();

        // Act
        await runner.RunAsync<ParentPipeline>(context);

        // Assert
        TestSink.ReceivedItems.Should().HaveCount(3);
        TestSink.ReceivedItems.Should().Equal(2, 4, 6); // Each input multiplied by 2
    }

    [Fact]
    public async Task CompositeNode_WithContextInheritance_ShouldInheritParameters()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();
        context.Parameters["TestParam"] = "InheritedValue";

        // Act
        await runner.RunAsync<ContextAwareParentPipeline>(context);

        // Assert
        ContextAwareTransform.ReceivedParameter.Should().Be("InheritedValue");
    }

    private sealed class TestSource : ISourceNode<int>
    {
        public IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            return new InMemoryDataPipe<int>([1, 2, 3], "TestSource");
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class TestSink : ISinkNode<int>
    {
        public static readonly List<int> ReceivedItems = [];

        public async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            ReceivedItems.Clear();

            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                ReceivedItems.Add(item);
            }
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class SimpleTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(input * 2);
        }
    }

    private sealed class SimpleTransformPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<SimpleTransform, int, int>("transform");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class ParentPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<TestSource, int>("source");

            var composite = builder.AddComposite<int, int, SimpleTransformPipeline>(
                "composite",
                CompositeContextConfiguration.Default);

            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class ContextAwareTransform : TransformNode<int, int>
    {
        public static string? ReceivedParameter;

        public override Task<int> ExecuteAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            ReceivedParameter = context.Parameters.TryGetValue("TestParam", out var value)
                ? value?.ToString()
                : null;

            return Task.FromResult(input);
        }
    }

    private sealed class ContextAwarePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<ContextAwareTransform, int, int>("transform");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class ContextAwareParentPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<TestSource, int>("source");

            var composite = builder.AddComposite<int, int, ContextAwarePipeline>(
                "composite",
                CompositeContextConfiguration.InheritAll);

            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }
}
