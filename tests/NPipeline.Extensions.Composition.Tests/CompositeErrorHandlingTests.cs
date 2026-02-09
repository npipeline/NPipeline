using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Xunit;

namespace NPipeline.Extensions.Composition.Tests;

/// <summary>
///     Tests for error handling and cancellation in composite nodes.
/// </summary>
public class CompositeErrorHandlingTests
{
    [Fact]
    public async Task CompositeNode_WithSubPipelineError_ShouldPropagateException()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();

        // Act & Assert
        var exception = await Assert.ThrowsAsync<NodeExecutionException>(() =>
            runner.RunAsync<ErrorProducingParentPipeline>(context));

        exception.InnerException.Should().BeOfType<InvalidOperationException>();
        exception.InnerException!.Message.Should().Contain("Test error in sub-pipeline");
    }

    [Fact]
    public async Task CompositeNode_WithEmptySubPipeline_ShouldThrowException()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();

        // Act & Assert
        var exception = await Assert.ThrowsAsync<NodeExecutionException>(() =>
            runner.RunAsync<EmptyOutputParentPipeline>(context));

        exception.InnerException.Should().BeOfType<InvalidOperationException>();
        exception.InnerException!.Message.Should().Contain("did not receive any output item");
    }

    [Fact]
    public async Task CompositeNode_WithNullableOutput_ShouldHandleNull()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();

        // Act
        await runner.RunAsync<NullableOutputParentPipeline>(context);

        // Assert
        NullableTestSink.ReceivedValue.Should().BeNull();
    }

    [Fact]
    public async Task CompositeNode_WithTypeMismatch_ShouldThrowInvalidCastException()
    {
        // Arrange
        var runner = PipelineRunner.Create();
        var context = new PipelineContext();

        // Act & Assert
        var exception = await Assert.ThrowsAsync<NodeExecutionException>(() =>
            runner.RunAsync<TypeMismatchParentPipeline>(context));

        exception.InnerException.Should().BeOfType<InvalidCastException>();
        exception.InnerException!.Message.Should().Contain("type mismatch");
    }

    // Test helper nodes and pipelines

    private sealed class ErrorSource : ISourceNode<int>
    {
        public IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            return new InMemoryDataPipe<int>([1], "ErrorSource");
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ErrorTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            throw new InvalidOperationException("Test error in sub-pipeline");
        }
    }

    private sealed class ErrorSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<ErrorTransform, int, int>("error");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class ErrorProducingParentPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<ErrorSource, int>("source");

            var composite = builder.AddComposite<int, int, ErrorSubPipeline>(
                "composite",
                CompositeContextConfiguration.Default);

            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class TestSink : ISinkNode<int>
    {
        public async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // Consume items
            }
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class SlowTransform : TransformNode<int, int>
    {
        public override async Task<int> ExecuteAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);
            return input;
        }
    }

    private sealed class SlowSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<SlowTransform, int, int>("slow");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class SlowProcessingParentPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<ErrorSource, int>("source");

            var composite = builder.AddComposite<int, int, SlowSubPipeline>(
                "composite",
                CompositeContextConfiguration.Default);

            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    private sealed class EmptyOutputSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<EmptySource, int>("input");
            var output = builder.AddSink<PipelineOutputSink<int>, int>("output");

            builder.Connect(source, output);
        }
    }

    private sealed class EmptySource : ISourceNode<int>
    {
        public IDataPipe<int> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            return new InMemoryDataPipe<int>([], "EmptySource");
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class EmptyOutputParentPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<ErrorSource, int>("source");

            var composite = builder.AddComposite<int, int, EmptyOutputSubPipeline>(
                "composite",
                CompositeContextConfiguration.Default);

            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    // Nullable output tests

    private sealed class NullableSource : ISourceNode<string?>
    {
        public IDataPipe<string?> Initialize(PipelineContext context, CancellationToken cancellationToken)
        {
            return new InMemoryDataPipe<string?>(["test"], "NullableSource");
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class NullReturningTransform : TransformNode<string?, string?>
    {
        public override Task<string?> ExecuteAsync(string? input, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult<string?>(null);
        }
    }

    private sealed class NullableSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<string?>, string?>("input");
            var transform = builder.AddTransform<NullReturningTransform, string?, string?>("null-transform");
            var output = builder.AddSink<PipelineOutputSink<string?>, string?>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class NullableTestSink : ISinkNode<string?>
    {
        public static string? ReceivedValue { get; private set; }

        public async Task ExecuteAsync(IDataPipe<string?> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var item in input.WithCancellation(cancellationToken))
            {
                ReceivedValue = item;
            }
        }

        public ValueTask DisposeAsync()
        {
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class NullableOutputParentPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<NullableSource, string?>("source");

            var composite = builder.AddComposite<string?, string?, NullableSubPipeline>(
                "composite",
                CompositeContextConfiguration.Default);

            var sink = builder.AddSink<NullableTestSink, string?>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }

    // Type mismatch tests

    private sealed class StringOutputSubPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<PipelineInputSource<int>, int>("input");
            var transform = builder.AddTransform<ToStringTransform, int, string>("to-string");
            var output = builder.AddSink<PipelineOutputSink<string>, string>("output");

            builder.Connect(source, transform);
            builder.Connect(transform, output);
        }
    }

    private sealed class ToStringTransform : TransformNode<int, string>
    {
        public override Task<string> ExecuteAsync(int input, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(input.ToString());
        }
    }

    private sealed class TypeMismatchParentPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource<ErrorSource, int>("source");

            // This should cause a type mismatch - expecting int output, but sub-pipeline produces string
            var composite = builder.AddComposite<int, int, StringOutputSubPipeline>(
                "composite",
                CompositeContextConfiguration.Default);

            var sink = builder.AddSink<TestSink, int>("sink");

            builder.Connect(source, composite);
            builder.Connect(composite, sink);
        }
    }
}
