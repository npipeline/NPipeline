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
        // Without calling AddNPipelineLineage(), PipelineBuilder.Lineage
        // is NullLineage. The guardrail should throw with
        // a clear message at Build() time when lineage is enabled.
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<DummySource, int>("source");
        var transform = builder.AddTransform<DummyTransform, int, int>("transform");
        builder.Connect(source, transform);
        builder.EnableItemLevelLineage();

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("NPipeline.Extensions.Lineage", ex.Message);
    }

    [Fact]
    public void BuildPipeline_WithoutLineageEnabled_DoesNotThrow()
    {
        // Without lineage enabled, the guardrail should not fire even though
        // PipelineBuilder.Lineage is NullLineage.
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<DummySource, int>("source");
        var transform = builder.AddTransform<DummyTransform, int, int>("transform");
        builder.Connect(source, transform);

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
        public override Task<int> TransformAsync(int item, PipelineContext context, CancellationToken ct)
            => Task.FromResult(item);
    }
}
