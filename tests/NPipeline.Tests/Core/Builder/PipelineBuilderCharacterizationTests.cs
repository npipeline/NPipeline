// ReSharper disable ClassNeverInstantiated.Local

using System.Diagnostics;
using System.Runtime.CompilerServices;
using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.Execution;
using NPipeline.Execution.Annotations;
using NPipeline.Execution.Strategies;
using NPipeline.Extensions.Testing;
using NPipeline.Graph;
using NPipeline.Graph.Validation;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Reliability;

namespace NPipeline.Tests.Core.Builder;

public sealed class PipelineBuilderCharacterizationTests
{
    [Fact]
    public void BuildMinimalPipeline_ProducesSingleSourceNodeGraph()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        var src = b.AddSource<InMemorySourceNode<int>, int>("src");
        var p = b.Build();
        p.Graph.Nodes.Should().HaveCount(1);
        p.Graph.Nodes[0].Id.Should().Be(src.Id);
        p.Graph.Edges.Should().BeEmpty();
    }

    [Fact]
    public void DuplicateNodeName_EarlyValidationEnabled_ThrowsAtAdd()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation().WithEarlyNameValidation();
        b.AddSource<InMemorySourceNode<int>, int>("dup");
        Action act = () => b.AddSource<InMemorySourceNode<int>, int>("dup");

        act.Should().Throw<ArgumentException>()
            .WithMessage("*has already been added*");
    }

    [Fact]
    public void DuplicateNodeName_LateValidation_FailsOnBuild()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation().WithoutEarlyNameValidation();
        b.AddSource<InMemorySourceNode<int>, int>("dup");
        b.AddTransform<PassthroughTransform, int, int>("dup");
        Action act = () => b.Build();

        act.Should().Throw<PipelineValidationException>()
            .Which.Result.Errors.Should().Contain(e => e.Contains("Node names must be unique"));
    }

    [Fact]
    public void AddAllNodeTypes_VerifyDefinitions()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
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
    public void AddTap_RegistersNodeWithTapKind()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var tap = b.AddTap(new InMemorySinkNode<int>(), "myTap");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");
        b.Connect(s, tap).Connect(tap, k);

        var p = b.Build();
        p.Graph.Nodes.Should().Contain(n => n.Id == tap.Id && n.Kind == NodeKind.Tap);
    }

    [Fact]
    public void AddBranch_RegistersNodeWithBranchKind()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var branch = b.AddBranch<int>(_ => Task.CompletedTask, "myBranch");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");
        b.Connect(s, branch).Connect(branch, k);

        var p = b.Build();
        p.Graph.Nodes.Should().Contain(n => n.Id == branch.Id && n.Kind == NodeKind.Branch);
    }

    [Fact]
    public void AddRoute_RegistersNodeWithRouteKind()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var route = b.AddRoute<int>("myRoute");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");
        b.Connect(s, route).Connect(route, k);

        var p = b.Build();
        p.Graph.Nodes.Should().Contain(n => n.Id == route.Id && n.Kind == NodeKind.Route);
    }

    [Fact]
    public void AddStreamTransform_RegistersNodeWithStreamTransformKind()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var st = b.AddStreamTransform<PassthroughStreamTransform, int, int>("st");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");
        b.Connect(s, st).Connect(st, k);

        var p = b.Build();
        p.Graph.Nodes.Should().Contain(n => n.Id == st.Id && n.Kind == NodeKind.StreamTransform);
    }

    [Fact]
    public void AddBatcher_RegistersNodeWithBatchKind()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var batcher = b.AddBatcher<int>("myBatcher", 5, TimeSpan.FromSeconds(1));
        var k = b.AddSink<InMemorySinkNode<IReadOnlyCollection<int>>, IReadOnlyCollection<int>>("k");
        b.Connect(s, batcher).Connect(batcher, k);

        var p = b.Build();
        p.Graph.Nodes.Should().Contain(n => n.Id == batcher.Id && n.Kind == NodeKind.Batch);
    }

    [Fact]
    public void AddUnbatcher_RegistersNodeWithBatchKind()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        var s = b.AddSource<InMemorySourceNode<IEnumerable<int>>, IEnumerable<int>>("s");
        var unbatcher = b.AddUnbatcher<int>("myUnbatcher");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");
        b.Connect(s, unbatcher).Connect(unbatcher, k);

        var p = b.Build();
        p.Graph.Nodes.Should().Contain(n => n.Id == unbatcher.Id && n.Kind == NodeKind.Batch);
    }

    [Fact]
    public void AddUnbatcher_WithReadOnlyCollectionInput_ConnectsFromBatcher()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var batcher = b.AddBatcher<int>("myBatcher", 5, TimeSpan.FromSeconds(1));
        var unbatcher = b.AddReadOnlyCollectionUnbatcher<int>("myReadOnlyCollectionUnbatcher");
        var k = b.AddSink<InMemorySinkNode<int>, int>("k");
        b.Connect(s, batcher).Connect(batcher, unbatcher).Connect(unbatcher, k);

        var p = b.Build();
        p.Graph.Nodes.Should().Contain(n => n.Id == unbatcher.Id && n.Kind == NodeKind.Batch);
        p.Graph.Nodes.Should().Contain(n => n.Id == unbatcher.Id && n.NodeType == typeof(ReadOnlyCollectionUnbatchingNode<int>));
    }

    [Fact]
    public void AddInMemoryLookup_RegistersNodeWithLookupKind()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var lookup = b.AddInMemoryLookup<int, int, string, string>(
            "myLookup",
            new Dictionary<int, string> { { 1, "one" } },
            i => i,
            (_, v) => v ?? "unknown");
        var k = b.AddSink<InMemorySinkNode<string>, string>("k");
        b.Connect(s, lookup).Connect(lookup, k);

        var p = b.Build();
        p.Graph.Nodes.Should().Contain(n => n.Id == lookup.Id && n.Kind == NodeKind.Lookup);
    }

    [Fact]
    public void ResilienceOptions_GlobalAndPerNodeOverridePersisted()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        var s = b.AddSource<InMemorySourceNode<int>, int>("s");
        var t = b.AddTransform<PassthroughTransform, int, int>("t");
        b.Connect(s, t);
        b.WithResilience(o => o with { ItemRetry = o.ItemRetry with { MaxRetries = 5 } });
        b.WithResilience(t, o => o with { ItemRetry = o.ItemRetry with { MaxRetries = 2 } });
        var p = b.Build();
        p.Graph.ErrorHandling.Resilience.Should().NotBeNull();
        p.Graph.ErrorHandling.Resilience!.ItemRetry.MaxRetries.Should().Be(5);

        // The node's options derive from the pipeline's, so what it did not set is inherited.
        var nodeOptions = p.Graph.ErrorHandling.NodeResilience.Should().ContainKey(t.Id).WhoseValue;
        nodeOptions.ItemRetry.MaxRetries.Should().Be(2);
        nodeOptions.ItemRetry.Backoff.Should().Be(p.Graph.ErrorHandling.Resilience.ItemRetry.Backoff);
    }

    [Fact]
    public void ResilienceOptions_ConfiguredBeforeTheProfile_StartFromTheProfile()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        b.AddSource<InMemorySourceNode<int>, int>("s");
        b.WithResilience(o => o with { OnItemFailure = ItemFailureAction.Skip });
        b.WithOptimizationProfile(PipelineOptimizationProfile.HighThroughput);
        var p = b.Build();

        p.Graph.ErrorHandling.Resilience!.ItemRetry.Should().BeSameAs(ItemRetryOptions.None);
        p.Graph.ErrorHandling.Resilience.OnItemFailure.Should().Be(ItemFailureAction.Skip);
    }

    [Fact]
    public void ResilienceOptions_Invalid_FailTheBuild()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        b.AddSource<InMemorySourceNode<int>, int>("s");
        b.WithResilience(o => o with { ItemRetry = new ItemRetryOptions { MaxRetries = -1 } });

        var act = () => b.Build();

        act.Should().Throw<InvalidOperationException>().WithMessage("*resilience options for the pipeline are invalid*");
    }

    [Fact]
    public void CircuitBreakerOptions_PersistedIntoGraph()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        b.AddSource<InMemorySourceNode<int>, int>("s");
        b.WithResilience(o => o with { CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = 7 } });
        var p = b.Build();
        p.Graph.ErrorHandling.Resilience!.CircuitBreaker.Should().NotBeNull();
        p.Graph.ErrorHandling.Resilience.CircuitBreaker!.ConsecutiveFailures.Should().Be(7);
    }

    [Fact]
    public void CircuitBreakerOptions_WithNoTripCondition_FailTheBuild()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
        b.AddSource<InMemorySourceNode<int>, int>("s");
        b.WithResilience(o => o with { CircuitBreaker = new CircuitBreakerOptions { ConsecutiveFailures = null } });

        var act = () => b.Build();

        act.Should().Throw<InvalidOperationException>().WithMessage("*resilience options for the pipeline are invalid*");
    }

    [Fact]
    public void ValidationMode_Warn_DoesNotThrowOnDuplicateNames()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation().WithoutEarlyNameValidation();
        b.WithValidationMode(GraphValidationMode.Warn);
        b.AddSource<InMemorySourceNode<int>, int>("n1");
        b.AddTransform<PassthroughTransform, int, int>("n1"); // duplicate
        var p = b.Build();
        p.Should().NotBeNull(); // Build succeeded
    }

    [Fact]
    public void GlobalExecutionObserver_PreservedAsAnnotation()
    {
        var b = new PipelineBuilder().WithoutExtendedValidation();
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
        var b = new PipelineBuilder().WithoutExtendedValidation();
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
        public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
        {
            return ValueTask.FromResult<int>(item);
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

    private sealed class IdentityAggregate() : AdvancedAggregateNode<int, int, int, int>(new AggregateNodeConfiguration<int>(
        AggregateWindows.Tumbling(TimeSpan.FromMinutes(1))))
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

    private sealed class PassthroughStreamTransform : IStreamTransformNode<int, int>
    {
        public async IAsyncEnumerable<int> TransformAsync(IAsyncEnumerable<int> items, PipelineContext context,
            [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await foreach (var item in items.WithCancellation(cancellationToken))
                yield return item;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
