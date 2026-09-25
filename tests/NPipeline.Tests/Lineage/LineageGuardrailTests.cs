using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Lineage;

public sealed class LineageGuardrailTests
{
    [Fact]
    public void BuildPipeline_WithLineageEnabledWithoutExtension_ThrowsClearError()
    {
        // A builder constructed without a lineage module uses NullLineage, so enabling item-level lineage
        // should fail at Build() time with a message naming the package to install.
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<DummySource, int>("source");
        var transform = builder.AddTransform<DummyTransform, int, int>("transform");
        var sink = builder.AddSink<DummySink, int>("sink");
        builder.Connect(source, transform).Connect(transform, sink);
        builder.EnableItemLevelLineage();

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("NPipeline.Extensions.Lineage", ex.Message);
    }

    [Fact]
    public void BuildPipeline_WithoutLineageEnabled_DoesNotThrow()
    {
        // With lineage left off, NullLineage is fine and the guardrail should not fire.
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<DummySource, int>("source");
        var transform = builder.AddTransform<DummyTransform, int, int>("transform");
        var sink = builder.AddSink<DummySink, int>("sink");
        builder.Connect(source, transform).Connect(transform, sink);

        var pipeline = builder.Build();
        Assert.NotNull(pipeline);
    }

    private sealed class DummySource : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken ct)
            => new DataStream<int>(Array.Empty<int>().ToAsyncEnumerable(), "dummy");
    }

    private sealed class DummyTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken ct)
            => ValueTask.FromResult(item);
    }

    private sealed class DummySink : SinkNode<int>
    {
        public override Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken ct)
            => Task.CompletedTask;
    }
}
