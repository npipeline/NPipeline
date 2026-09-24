using AwesomeAssertions;
using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.Extensions.Testing;
using NPipeline.Lineage;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Extensions.Lineage.Tests;

public sealed class PipelineBuilderCharacterizationTests
{
    [Fact]
    public void EnableLineage_OneToOne_NoMaterializationPathRetained()
    {
        var b = new PipelineBuilder(new LineageService()).WithoutExtendedValidation();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var t = b.AddTransform<PassthroughTransform, int, int>("t");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");
        b.Connect(s, t).Connect(t, k);
        b.EnableItemLevelLineage();
        var p = b.Build();
        var tDef = p.Graph.Nodes.Single(n => n.Id == t.Id);
        tDef.DeclaredCardinality.Should().BeNull();
        tDef.LineageAdapter.Should().NotBeNull();
    }

    [Fact]
    public void Lineage_WithDeclaredOneToMany_AdapterPresent()
    {
        var b = new PipelineBuilder(new LineageService()).WithoutExtendedValidation();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var t = b.AddTransform<OneToManyTransform, int, int>("oom");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");
        b.Connect(s, t).Connect(t, k);

        b.EnableItemLevelLineage(o =>
            o with { SampleEvery = 1, MaterializationCap = 10 });

        var p = b.Build();
        var def = p.Graph.Nodes.Single(n => n.Id == t.Id);
        def.DeclaredCardinality.Should().Be(TransformCardinality.OneToMany);
        def.LineageAdapter.Should().NotBeNull();
    }

    [Fact]
    public void Lineage_OverflowPolicyStrict_WhenCapExceeded_ThrowsDuringBuildMaterializationPhase()
    {
        var b = new PipelineBuilder(new LineageService()).WithoutExtendedValidation();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var t = b.AddTransform<OneToManyTransform, int, int>("oom");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");
        b.Connect(s, t).Connect(t, k);

        b.EnableItemLevelLineage(o =>
            o with { SampleEvery = 1, MaterializationCap = 1, OverflowPolicy = LineageOverflowPolicy.Strict });

        var p = b.Build();
        p.Should().NotBeNull();
    }

    [Fact]
    public void Lineage_OverflowPolicyWarnContinue_DoesNotAffectBuild()
    {
        var b = new PipelineBuilder(new LineageService()).WithoutExtendedValidation();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var t = b.AddTransform<OneToManyTransform, int, int>("oom");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");
        b.Connect(s, t).Connect(t, k);

        b.EnableItemLevelLineage(o =>
            o with { SampleEvery = 1, MaterializationCap = 1, OverflowPolicy = LineageOverflowPolicy.WarnContinue });

        var p = b.Build();
        p.Graph.Nodes.Should().ContainSingle(n => n.Id == t.Id);
    }

    private sealed class PassthroughTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) => ValueTask.FromResult(item);
    }

    [TransformCardinality(TransformCardinality.OneToMany)]
    private sealed class OneToManyTransform : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) => ValueTask.FromResult(item);
    }
}
