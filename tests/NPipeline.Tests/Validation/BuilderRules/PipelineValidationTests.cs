using AwesomeAssertions;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Validation.BuilderRules;

public sealed class PipelineValidationTests
{
    private Pipeline.Pipeline Build<TDef>() where TDef : IPipelineDefinition, new()
    {
        var builder = new PipelineBuilder();
        var def = new TDef();
        def.Define(builder, PipelineContext.Default);
        return builder.Build();
    }

    [Fact]
    public void ValidPipeline_Should_PassValidation()
    {
        var act = () => Build<ValidPipeline>();
        act.Should().NotThrow();
    }

    [Fact]
    public void PipelineWithoutSource_Should_FailValidation()
    {
        var act = () => Build<MissingSourcePipeline>();

        act.Should()
            .Throw<PipelineValidationException>()
            .WithMessage("*Non-source nodes with no inbound edges*");
    }

    [Fact]
    public void UnreachableNode_Should_FailValidation()
    {
        var act = () => Build<UnreachableNodePipeline>();
        act.Should().Throw<PipelineValidationException>().WithMessage("*Unreachable nodes*");
    }

    [Fact]
    public void Cycle_Should_FailValidation()
    {
        var act = () => Build<CyclePipeline>();

        act.Should()
            .Throw<PipelineValidationException>()
            .Where(e => e.Message.Contains("Cycle detected:") && (e.Message.Contains("t -> u -> t") || e.Message.Contains("u -> t -> u")),
                "cycle path should be included");
    }

    [Fact]
    public void IsolatedNode_Should_FailValidation()
    {
        var act = () => Build<IsolatedNodePipeline>();
        act.Should().Throw<PipelineValidationException>().WithMessage("*Isolated nodes*");
    }

    [Fact]
    public void TryBuild_Should_SurfaceCycleIssueWithCategory()
    {
        var builder = new PipelineBuilder();
        new CyclePipeline().Define(builder, PipelineContext.Default);
        var ok = builder.TryBuild(out var pipeline, out var result);
        ok.Should().BeFalse();
        result.Issues.Should().Contain(i => i.Category == "Cycles" && i.Message.Contains("Cycle detected"));
    }

    [Fact]
    public void WarnMode_Should_BuildPipelineDespiteErrors()
    {
        var builder = new PipelineBuilder().WithValidationMode(GraphValidationMode.Warn);
        new InvalidButWarnable().Define(builder, PipelineContext.Default);
        var ok = builder.TryBuild(out var pipeline, out var result);
        ok.Should().BeTrue();
        pipeline.Should().NotBeNull();
        result.Issues.Should().NotBeEmpty(); // issues captured
    }

    [Fact]
    public void OffMode_Should_SkipValidation()
    {
        var builder = new PipelineBuilder().WithValidationMode(GraphValidationMode.Off);
        new InvalidButWarnable().Define(builder, PipelineContext.Default);

        // Build should not throw even though the graph is invalid structurally.
        var act = () => builder.Build();
        act.Should().NotThrow();
    }

    private sealed class T : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
        }
    }

    private sealed class ValidPipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            // Provide a single item via context-backed source (reuse provided context 'c')
            var s = b.AddInMemorySourceWithDataFromContext(c, "s", [1]);
            var t = b.AddTransform<T, int, int>("t");
            var k = b.AddInMemorySink<int>("k");
            b.Connect(s, t).Connect(t, k);
        }
    }

    private sealed class MissingSourcePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var t = b.AddTransform<T, int, int>("t");
            var u = b.AddTransform<T, int, int>("u");
            b.Connect(t, u);
        }
    }

    private sealed class UnreachableNodePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var s = b.AddInMemorySourceWithDataFromContext(c, "s", [1]);
            var t = b.AddTransform<T, int, int>("t");
            var orphan = b.AddTransform<T, int, int>("orphan");
            var k = b.AddInMemorySink<int>("k");
            b.Connect(s, t).Connect(t, k); /* orphan disconnected */
        }
    }

    private sealed class CyclePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var s = b.AddInMemorySourceWithDataFromContext(c, "s", [1]);
            var t = b.AddTransform<T, int, int>("t");
            b.Connect(s, t); // introduce back edge cycle

            // Hack: builder doesn't expose raw edge add; simulate by adding another transform referencing existing ids is not possible
            // So we construct cycle via two transforms
            var u = b.AddTransform<T, int, int>("u");
            b.Connect(t, u).Connect(u, t);
        }
    }

    private sealed class IsolatedNodePipeline : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var s = b.AddInMemorySourceWithDataFromContext(c, "s", [1]);
            var t = b.AddTransform<T, int, int>("t");
            var k = b.AddInMemorySink<int>("k");
            var iso = b.AddTransform<T, int, int>("iso");
            b.Connect(s, t).Connect(t, k); /* iso isolated */
        }
    }

    private sealed class InvalidButWarnable : IPipelineDefinition
    {
        public void Define(PipelineBuilder b, PipelineContext c)
        {
            var t = b.AddTransform<T, int, int>("t");
            var u = b.AddTransform<T, int, int>("u");
            b.Connect(t, u);
        }
    }
}
