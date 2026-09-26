using System.Collections.Frozen;
using AwesomeAssertions;
using NPipeline.Attributes.Nodes;
using NPipeline.DataFlow.Branching;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Graph.Validation;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;
using NPipeline.Tests.Reliability.Behavior;

namespace NPipeline.Tests.Validation.BuilderRules;

public sealed class PipelineValidationTests
{
    private NPipeline.Pipeline.Pipeline Build<TDef>() where TDef : IPipelineDefinition, new()
    {
        var builder = new PipelineBuilder();
        var def = new TDef();
        def.Define(builder, PipelineContext.CreateDefault());
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
    public void UnconnectedSourceAndSink_Should_FailValidation()
    {
        var builder = new PipelineBuilder();
        _ = builder.AddSource<StreamingSource<int>, int>("s");
        _ = builder.AddSink<CollectingSink<int>, int>("k");

        var act = () => builder.Build();

        act.Should().Throw<PipelineValidationException>().WithMessage("*Isolated nodes*");
    }

    [Fact]
    public void UnconnectedSourceTransformAndSink_Should_FailValidation()
    {
        var builder = new PipelineBuilder();
        _ = builder.AddSource<StreamingSource<int>, int>("s");
        _ = builder.AddTransform<T, int, int>("t");
        _ = builder.AddSink<CollectingSink<int>, int>("k");

        var act = () => builder.Build();

        act.Should().Throw<PipelineValidationException>().WithMessage("*Isolated nodes*");
    }

    [Fact]
    public void LoneSource_Should_PassCoreRules()
    {
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        _ = builder.AddSource<StreamingSource<int>, int>("s");

        var validation = builder.Validate();

        validation.IsValid.Should().BeTrue();
    }

    [Fact]
    public void DanglingTransform_Should_FailValidation()
    {
        var builder = new PipelineBuilder();
        var source = builder.AddSource<StreamingSource<int>, int>("s");
        var transform = builder.AddTransform<FlakyTransform, int, int>("t");
        var sink = builder.AddSink<CollectingSink<int>, int>("k");
        _ = builder.Connect(source, transform).Connect(source, sink);

        var act = () => builder.Build();

        act.Should()
            .Throw<PipelineValidationException>()
            .WithMessage("*The output of these nodes is never consumed*");
    }

    [Fact]
    public void TerminalTap_Should_FailValidation()
    {
        var builder = new PipelineBuilder();
        var source = builder.AddSource<StreamingSource<int>, int>("s");
        var tap = builder.AddTap(new CollectingSink<int>(), "tap");
        var sink = builder.AddSink<CollectingSink<int>, int>("k");
        _ = builder.Connect(source, tap).Connect(source, sink);

        var act = () => builder.Build();

        act.Should()
            .Throw<PipelineValidationException>()
            .WithMessage("*The output of these nodes is never consumed*");
    }

    [Fact]
    public void ConnectedTransformChain_Should_PassValidation()
    {
        var builder = new PipelineBuilder();
        var source = builder.AddSource<StreamingSource<int>, int>("s");
        var transform = builder.AddTransform<T, int, int>("t");
        var sink = builder.AddSink<CollectingSink<int>, int>("k");
        _ = builder.Connect(source, transform).Connect(transform, sink);

        var act = () => builder.Build();

        act.Should().NotThrow();
    }

    [Fact]
    public async Task DiamondWithBoundedBranchCapacity_Should_Complete()
    {
        var run = BehaviorPipeline.RunAsync(b =>
        {
            var source = b.AddSource<StreamingSource<int>, int>("s");
            _ = b.AddPreconfiguredNodeInstance(source.Id, StreamingSource<int>.Of(Enumerable.Range(1, 200)));
            var a = b.AddTransform<T, int, int>("a");
            var d = b.AddTransform<T, int, int>("d");
            var sink = b.AddSink<CollectingSink<int>, int>("k");
            _ = b.Connect(source, a).Connect(source, d).Connect(a, sink).Connect(d, sink);
            _ = b.WithBranchOptions(source.Id, new BranchOptions(4));
        });

        var finished = await Task.WhenAny(run, Task.Delay(TimeSpan.FromSeconds(10)));
        finished.Should().BeSameAs(run, "every branch of the diamond reaches a sink, so the run must complete");
        await run;
    }

    [Fact]
    public void JoinWithoutRightInput_Should_FailValidation()
    {
        var builder = new PipelineBuilder();
        var left = builder.AddSource<StreamingSource<Left>, Left>("left");
        var join = builder.AddJoin<TestJoinNode, Left, Right, int>("join");
        var sink = builder.AddSink<CollectingSink<int>, int>("sink");
        _ = builder.Connect(left, join).Connect(join, sink);

        var act = () => builder.Build();

        act.Should()
            .Throw<PipelineValidationException>()
            .WithMessage("*missing its right input (Right); connected upstream types: Left*");
    }

    [Fact]
    public void JoinWithoutLeftInput_Should_FailValidation()
    {
        var builder = new PipelineBuilder();
        var right = builder.AddSource<StreamingSource<Right>, Right>("right");
        var join = builder.AddJoin<TestJoinNode, Left, Right, int>("join");
        var sink = builder.AddSink<CollectingSink<int>, int>("sink");
        _ = builder.Connect(right, join).Connect(join, sink);

        var act = () => builder.Build();

        act.Should()
            .Throw<PipelineValidationException>()
            .WithMessage("*missing its left input (Left); connected upstream types: Right*");
    }

    [Fact]
    public void JoinWithBothInputs_Should_PassValidation()
    {
        var builder = new PipelineBuilder();
        var left = builder.AddSource<StreamingSource<Left>, Left>("left");
        var right = builder.AddSource<StreamingSource<Right>, Right>("right");
        var join = builder.AddJoin<TestJoinNode, Left, Right, int>("join");
        var sink = builder.AddSink<CollectingSink<int>, int>("sink");
        _ = builder.Connect(left, join).Connect(right, join).Connect(join, sink);

        var act = () => builder.Build();

        act.Should().NotThrow();
    }

    [Fact]
    public void SelfJoin_WithBothInputs_Should_PassValidation()
    {
        var builder = new PipelineBuilder();
        var left = builder.AddSource<StreamingSource<int>, int>("left");
        var right = builder.AddSource<StreamingSource<int>, int>("right");
        var join = builder.AddSelfJoin(left, right, "selfJoin", (a, b) => a + b, i => i);
        var sink = builder.AddSink<CollectingSink<int>, int>("sink");
        _ = builder.Connect(join, sink);

        var act = () => builder.Build();

        act.Should().NotThrow();
    }

    [Fact]
    public void JoinWithBothInputs_OnHandBuiltGraphWithoutDefinitionMap_Should_PassValidation()
    {
        var leftSource = new NodeDefinition("left", "left", typeof(StreamingSource<Left>), NodeKind.Source, null, typeof(Left));
        var rightSource = new NodeDefinition("right", "right", typeof(StreamingSource<Right>), NodeKind.Source, null, typeof(Right));
        var join = new NodeDefinition("join", "join", typeof(TestJoinNode), NodeKind.Join, typeof(Left), typeof(int), IsJoin: true, SecondInputType: typeof(Right));
        var sink = new NodeDefinition("sink", "sink", typeof(CollectingSink<int>), NodeKind.Sink, typeof(int));

        // The validator reads the join's input types from Nodes, not from a separately supplied definition map.
        var graph = new PipelineGraph
        {
            Nodes = [leftSource, rightSource, join, sink],
            Edges = [new Edge("left", "join"), new Edge("right", "join"), new Edge("join", "sink")],
            PreconfiguredNodeInstances = FrozenDictionary<string, INode>.Empty,
        };

        var result = PipelineGraphValidator.Validate(graph);

        result.IsValid.Should().BeTrue();
    }

    [Fact]
    public void Validate_And_TryBuild_Agree_OnMissingSink()
    {
        var builder = new PipelineBuilder();
        var source = builder.AddSource<StreamingSource<int>, int>("s");
        var transform = builder.AddTransform<T, int, int>("t");
        _ = builder.Connect(source, transform);

        var validation = builder.Validate();
        validation.IsValid.Should().BeFalse();
        validation.Errors.Should().Contain(error => error.Contains("no sink"));

        var ok = builder.TryBuild(out _, out var result);
        ok.Should().BeFalse();
        result.Errors.Should().Contain(error => error.Contains("no sink"));
    }

    [Fact]
    public void Validate_And_TryBuild_Agree_OnFailingCustomRule()
    {
        var builder = new PipelineBuilder().WithValidationRule(new AlwaysFailsRule());
        var source = builder.AddSource<StreamingSource<int>, int>("s");
        var sink = builder.AddSink<CollectingSink<int>, int>("k");
        _ = builder.Connect(source, sink);

        var validation = builder.Validate();
        validation.IsValid.Should().BeFalse();
        validation.Errors.Should().Contain(error => error.Contains("custom rule failed"));

        var ok = builder.TryBuild(out _, out var result);
        ok.Should().BeFalse();
        result.Errors.Should().Contain(error => error.Contains("custom rule failed"));
    }

    [Fact]
    public void Validate_And_TryBuild_Agree_OnInvalidResilienceOptions()
    {
        var builder = new PipelineBuilder();
        var source = builder.AddSource<StreamingSource<int>, int>("s");
        var sink = builder.AddSink<CollectingSink<int>, int>("k");
        _ = builder.Connect(source, sink);
        _ = builder.WithResilience(options => options with { ItemRetry = new ItemRetryOptions { MaxRetries = -1 } });

        // Configuration that cannot become a graph is reported, not thrown, by both.
        var validation = builder.Validate();
        validation.IsValid.Should().BeFalse();
        validation.Errors.Should().ContainMatch("*resilience options for the pipeline are invalid*");

        builder.TryBuild(out var pipeline, out var tryBuildResult).Should().BeFalse();
        pipeline.Should().BeNull();
        tryBuildResult.Errors.Should().ContainMatch("*resilience options for the pipeline are invalid*");
    }

    [Fact]
    public void Validate_And_TryBuild_Agree_OnResilienceOptionsRule()
    {
        // Validate() used to build a graph without the error-handling configuration, so this rule could never fire.
        var builder = new PipelineBuilder();
        var source = builder.AddSource<StreamingSource<int>, int>("s");
        var sink = builder.AddSink<CollectingSink<int>, int>("k");
        _ = builder.Connect(source, sink);
        _ = builder.WithResilience(sink, o => o with { ItemRetry = o.ItemRetry with { MaxRetries = o.ItemRetry.MaxRetries + 4 } });

        var validation = builder.Validate();
        validation.IsValid.Should().BeFalse();
        validation.Errors.Should().ContainMatch("*only transform nodes use*");

        builder.TryBuild(out _, out var tryBuildResult).Should().BeFalse();
        tryBuildResult.Errors.Should().ContainMatch("*only transform nodes use*");
    }

    [Fact]
    public void Validate_And_TryBuild_Agree_OnAnEmptyBuilder()
    {
        var builder = new PipelineBuilder();

        builder.Validate().Errors.Should().Contain("A pipeline must have at least one node.");
        builder.TryBuild(out _, out var tryBuildResult).Should().BeFalse();
        tryBuildResult.Errors.Should().Contain("A pipeline must have at least one node.");
    }

    [Fact]
    public void ToMermaidDiagram_And_Describe_DoNotThrow_OnInvalidResilienceOptions()
    {
        var builder = new PipelineBuilder();
        var source = builder.AddSource<StreamingSource<int>, int>("s");
        var sink = builder.AddSink<CollectingSink<int>, int>("k");
        _ = builder.Connect(source, sink);
        _ = builder.WithResilience(options => options with { ItemRetry = new ItemRetryOptions { MaxRetries = -1 } });

        // Visualization renders structure only, so invalid configuration must not stop it.
        var mermaid = builder.ToMermaidDiagram();
        var description = builder.Describe();

        mermaid.Should().Contain("s");
        mermaid.Should().Contain("k");
        description.Should().Contain("Nodes:");
        description.Should().Contain("Edges:");
    }

    [Fact]
    public void GraphWithoutAnySourceNode_Should_ReportMissingSource()
    {
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var first = builder.AddTransform<T, int, int>("first");
        var second = builder.AddTransform<T, int, int>("second");
        _ = builder.Connect(first, second);

        var result = builder.Validate();

        result.IsValid.Should().BeFalse();
        result.Errors.Should().Contain(error => error.Contains("at least one ISourceNode<T> is required"));
    }

    [Fact]
    public void TryBuild_Should_SurfaceCycleIssueWithCategory()
    {
        var builder = new PipelineBuilder();
        new CyclePipeline().Define(builder, PipelineContext.CreateDefault());
        var ok = builder.TryBuild(out var pipeline, out var result);
        ok.Should().BeFalse();
        result.Issues.Should().Contain(i => i.Category == "Cycles" && i.Message.Contains("Cycle detected"));
    }

    [Fact]
    public void WarnMode_Should_BuildPipelineDespiteErrors()
    {
        var builder = new PipelineBuilder().WithValidationMode(GraphValidationMode.Warn);
        new InvalidButWarnable().Define(builder, PipelineContext.CreateDefault());
        var ok = builder.TryBuild(out var pipeline, out var result);
        ok.Should().BeTrue();
        pipeline.Should().NotBeNull();
        result.Issues.Should().NotBeEmpty(); // issues captured
    }

    [Fact]
    public void OffMode_Should_SkipValidation()
    {
        var builder = new PipelineBuilder().WithValidationMode(GraphValidationMode.Off);
        new InvalidButWarnable().Define(builder, PipelineContext.CreateDefault());

        // Build should not throw even though the graph is invalid structurally.
        var act = () => builder.Build();
        act.Should().NotThrow();
    }

    private sealed class T : TransformNode<int, int>
    {
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) => ValueTask.FromResult(item);
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
            b.Connect(s, t);

            // Create a cycle using two transforms connected in a loop.
            // This is the idiomatic way to construct cycles for testing since
            // the builder API intentionally doesn't expose raw edge manipulation.
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

    private sealed record Left(int Id);

    private sealed record Right(int Id);

    [KeySelector(typeof(Left), nameof(Left.Id))]
    [KeySelector(typeof(Right), nameof(Right.Id))]
    private sealed class TestJoinNode : KeyedJoinNode<int, Left, Right, int>
    {
        public override int CreateOutput(Left item1, Right item2) => item1.Id + item2.Id;

        public override int CreateOutputFromLeft(Left item1) => item1.Id;

        public override int CreateOutputFromRight(Right item2) => item2.Id;
    }

    private sealed class AlwaysFailsRule : IGraphRule
    {
        public string Name => "AlwaysFails";
        public bool StopOnError => false;

        public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
        {
            yield return new ValidationIssue(ValidationSeverity.Error, "custom rule failed", "Custom");
        }
    }
}
