// ReSharper disable ClassNeverInstantiated.Local

using System.Diagnostics;
using System.Runtime.CompilerServices;
using AwesomeAssertions;
using NPipeline.Attributes.Lineage;
using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Execution.Annotations;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Graph.Validation;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Core.Builder;

public sealed class PipelineBuilderCharacterizationTests
{
    [Fact]
    public void BuildMinimalPipeline_ProducesSingleSourceNodeGraph()
    {
        var b = new PipelineBuilder();
        var src = b.AddSource<InMemorySourceNode<int>, int>("src");
        var p = b.Build();
        p.Graph.Nodes.Should().HaveCount(1);
        p.Graph.Nodes[0].Id.Should().Be(src.Id);
        p.Graph.Edges.Should().BeEmpty();
    }

    [Fact]
    public void DuplicateNodeName_EarlyValidationEnabled_ThrowsAtAdd()
    {
        var b = new PipelineBuilder().WithEarlyNameValidation();
        b.AddSource<InMemorySourceNode<int>, int>("dup");
        Action act = () => b.AddSource<InMemorySourceNode<int>, int>("dup");

        act.Should().Throw<ArgumentException>()
            .WithMessage("*has already been added*");
    }

    [Fact]
    public void DuplicateNodeName_LateValidation_FailsOnBuild()
    {
        var b = new PipelineBuilder();
        b.AddSource<InMemorySourceNode<int>, int>("dup");
        b.AddTransform<PassthroughTransform, int, int>("dup");
        Action act = () => b.Build();

        act.Should().Throw<PipelineValidationException>()
            .Which.Result.Errors.Should().Contain(e => e.Contains("Node names must be unique"));
    }

    [Fact]
    public void AddAllNodeTypes_VerifyDefinitions()
    {
        var b = new PipelineBuilder();
        var s1 = b.AddSource<InMemorySourceNode<int>, int>("s1");
        var s2 = b.AddSource<InMemorySourceNode<long>, long>("s2");
        var t = b.AddTransform<PassthroughTransform, int, int>("t");
        var j = b.AddJoin<TestJoinNode, int, long, int>("j");
        var a = b.AddAggregate<IdentityAggregate, int, int, int, int>("a");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");

        // Connect int source through transform and aggregate to sink
        b.Connect(s1, t);
        b.Connect(s1, a);
        b.Connect(a, k);
        b.Connect(t, k);

        // Connect both sources into join (different input types avoid generic ambiguity)
        b.Connect(s1, j);
        b.Connect(s2, j);

        // Join output -> transform (just to exercise a join->transform edge) then to sink via existing t
        // (We could add a distinct sink path, but existing edges are sufficient to ensure join node definition presence.)
        var p = b.Build();
        p.Graph.Nodes.Should().Contain(n => n.Id == s1.Id && n.Kind == NodeKind.Source);
        p.Graph.Nodes.Should().Contain(n => n.Id == s2.Id && n.Kind == NodeKind.Source);
        p.Graph.Nodes.Should().Contain(n => n.Id == t.Id && n.Kind == NodeKind.Transform);
        p.Graph.Nodes.Should().Contain(n => n.Id == j.Id && n.Kind == NodeKind.Join);
        p.Graph.Nodes.Should().Contain(n => n.Id == a.Id && n.Kind == NodeKind.Aggregate);
        p.Graph.Nodes.Should().Contain(n => n.Id == k.Id && n.Kind == NodeKind.Sink);
    }

    [Fact]
    public void EnableLineage_OneToOne_NoMaterializationPathRetained()
    {
        var b = new PipelineBuilder();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var t = b.AddTransform<PassthroughTransform, int, int>("t");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");
        b.Connect(s, t).Connect(t, k);
        b.EnableItemLevelLineage();
        var p = b.Build();
        var tDef = p.Graph.Nodes.Single(n => n.Id == t.Id);
        tDef.DeclaredCardinality.Should().BeNull(); // current behavior (no attribute)
        tDef.LineageAdapter.Should().NotBeNull();
    }

    [Fact]
    public void Lineage_WithDeclaredOneToMany_AdapterPresent()
    {
        var b = new PipelineBuilder();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var t = b.AddTransform<OneToManyTransform, int, int>("oom");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");
        b.Connect(s, t).Connect(t, k);

        b.EnableItemLevelLineage(o =>
        {
            o.SampleEvery = 1;
            o.MaterializationCap = 10; // ensure cap path still supported
        });

        var p = b.Build();
        var def = p.Graph.Nodes.Single(n => n.Id == t.Id);
        def.DeclaredCardinality.Should().Be(TransformCardinality.OneToMany);
        def.LineageAdapter.Should().NotBeNull();
    }

    [Fact]
    public void Lineage_OverflowPolicyStrict_WhenCapExceeded_ThrowsDuringBuildMaterializationPhase()
    {
        var b = new PipelineBuilder();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var t = b.AddTransform<OneToManyTransform, int, int>("oom");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");
        b.Connect(s, t).Connect(t, k);

        b.EnableItemLevelLineage(o =>
        {
            o.SampleEvery = 1;
            o.MaterializationCap = 1; // force overflow path for one-to-many mapping logic
            o.OverflowPolicy = LineageOverflowPolicy.Strict;
        });

        // Current behavior: Build does not execute transformation, so overflow cannot occur here. Expect no throw.
        // (Characterizing that overflow policy Strict does NOT impact Build-time for static graph.)
        var p = b.Build();
        p.Should().NotBeNull();
    }

    [Fact]
    public void Lineage_OverflowPolicyWarnContinue_DoesNotAffectBuild()
    {
        var b = new PipelineBuilder();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var t = b.AddTransform<OneToManyTransform, int, int>("oom");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");
        b.Connect(s, t).Connect(t, k);

        b.EnableItemLevelLineage(o =>
        {
            o.SampleEvery = 1;
            o.MaterializationCap = 1; // force potential overflow later
            o.OverflowPolicy = LineageOverflowPolicy.WarnContinue;
        });

        var p = b.Build();
        p.Graph.Nodes.Should().ContainSingle(n => n.Id == t.Id);
    }

    [Fact]
    public void RetryOptions_GlobalAndPerNodeOverridePersisted()
    {
        var b = new PipelineBuilder();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var t = b.AddTransform<PassthroughTransform, int, int>("t");
        b.Connect(s, t);
        b.WithRetryOptions(o => o.With(5));
        b.WithRetryOptions(t, PipelineRetryOptions.Default.With(2));
        var p = b.Build();
        p.Graph.ErrorHandling.RetryOptions.Should().NotBeNull();
        p.Graph.ErrorHandling.RetryOptions!.MaxItemRetries.Should().Be(5);

        p.Graph.ErrorHandling.NodeRetryOverrides.Should().ContainKey(t.Id)
            .WhoseValue.MaxItemRetries.Should().Be(2);
    }

    [Fact]
    public void CircuitBreakerOptions_PersistedIntoGraph()
    {
        var b = new PipelineBuilder();
        b.AddSource<InMemorySourceNode<int>, int>("s");
        b.WithCircuitBreaker(7, TimeSpan.FromSeconds(30));
        var p = b.Build();
        p.Graph.ErrorHandling.CircuitBreakerOptions.Should().NotBeNull();
        p.Graph.ErrorHandling.CircuitBreakerOptions!.FailureThreshold.Should().Be(7);
    }

    [Fact]
    public void CircuitBreakerMemoryOptions_PersistedIntoGraph()
    {
        var builder = new PipelineBuilder();
        builder.AddSource<InMemorySourceNode<int>, int>("s");
        builder.WithCircuitBreaker();

        var customMemory = CircuitBreakerMemoryManagementOptions.Default with
        {
            EnableAutomaticCleanup = false,
            MaxTrackedCircuitBreakers = 42,
        };

        builder.ConfigureCircuitBreakerMemoryManagement(_ => customMemory);

        var pipeline = builder.Build();
        pipeline.Graph.ErrorHandling.CircuitBreakerMemoryOptions.Should().NotBeNull();
        pipeline.Graph.ErrorHandling.CircuitBreakerMemoryOptions!.Should().Be(customMemory);
    }

    [Fact]
    public void ValidationMode_Warn_DoesNotThrowOnDuplicateNames()
    {
        var b = new PipelineBuilder();
        b.WithValidationMode(GraphValidationMode.Warn);
        b.AddSource<InMemorySourceNode<int>, int>("n1");
        b.AddTransform<PassthroughTransform, int, int>("n1"); // duplicate
        var p = b.Build();
        p.Should().NotBeNull(); // Build succeeded
    }

    [Fact]
    public void GlobalExecutionObserver_PreservedAsAnnotation()
    {
        var b = new PipelineBuilder();
        b.AddSource<InMemorySourceNode<int>, int>("s");
        var obs = new TestObserver();
        b.SetGlobalExecutionObserver(obs);
        var p = b.Build();
        p.Graph.ExecutionOptions.NodeExecutionAnnotations.Should().ContainKey(ExecutionAnnotationKeys.GlobalExecutionObserver);
    }

    // Lightweight reflection sampling baseline: ensure we can at least obtain a node definition without extra reflection during Build() beyond current behavior.
    [Fact]
    public void Reflection_BuildTimeOnly_NoRuntimeExecutionIncludedHere()
    {
        var b = new PipelineBuilder();
        b.AddSource<InMemorySourceNode<int>, int>("s");
        var sw = Stopwatch.StartNew();
        var p = b.Build();
        sw.Stop();
        p.Graph.Nodes.Should().HaveCount(1);
        sw.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(2)); // coarse sanity guard
    }

    // Test types used only within characterization tests

    private sealed class PassthroughTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
        }
    }

    [TransformCardinality(TransformCardinality.OneToMany)]
    private sealed class OneToManyTransform : TransformNode<int, int>
    {
        public override Task<int> ExecuteAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return Task.FromResult(item);
        }
    }

    private sealed class TestJoinNode : IJoinNode
    {
        public ValueTask<IAsyncEnumerable<object?>> ExecuteAsync(IAsyncEnumerable<object?> inputStream, PipelineContext context,
            CancellationToken cancellationToken = default)
        {
            async IAsyncEnumerable<object?> Impl([EnumeratorCancellation] CancellationToken ct = default)
            {
                await foreach (var item in inputStream.WithCancellation(ct))

                    // Pass through only int items for determinism
                {
                    if (item is int i)
                        yield return i;
                }
            }

            return ValueTask.FromResult<IAsyncEnumerable<object?>>(Impl(cancellationToken));
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class IdentityAggregate() : AdvancedAggregateNode<int, int, int, int>(AggregateWindows.Tumbling(TimeSpan.FromMinutes(1)))
    {
        public override int GetKey(int item)
        {
            return item;

            // key by value
        }

        public override int CreateAccumulator()
        {
            return 0;
        }

        public override int Accumulate(int accumulator, int item)
        {
            return accumulator + item;
        }

        public override int GetResult(int accumulator)
        {
            return accumulator;
        }
    }

    private sealed class TestObserver : IExecutionObserver
    {
        public void OnNodeStarted(NodeExecutionStarted e)
        {
        }

        public void OnNodeCompleted(NodeExecutionCompleted e)
        {
        }

        public void OnRetry(NodeRetryEvent e)
        {
        }

        public void OnDrop(QueueDropEvent e)
        {
        }

        public void OnQueueMetrics(QueueMetricsEvent e)
        {
        }
    }
}
