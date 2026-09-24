using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.Execution;
using NPipeline.Execution.Caching;
using NPipeline.Execution.Plans;
using NPipeline.Execution.Strategies;
using NPipeline.Graph;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace NPipeline.Tests.Execution.Caching;

/// <summary>
///     Covers which graphs share compiled execution plans and which must not.
///     <para>
///         Plans used to be keyed on a SHA-256 digest of the graph that was computed once at build time. It left out
///         node kind, and any <c>with</c> expression that rewrote the graph - which the runtime binder does whenever a
///         run overrides lineage settings - copied the old digest forward, so the key stopped describing the graph it
///         was keying. The key is now derived from exactly the node properties a plan is compiled from.
///     </para>
/// </summary>
public sealed class ExecutionPlanCacheKeyTests
{
    private const int CacheCapacity = 100;

    [Fact]
    public void AStructurallyIdenticalGraph_ReusesCachedPlans()
    {
        var cache = new InMemoryPipelineExecutionPlanCache();
        var plans = PlansFor(BuildGraph());

        cache.CachePlans(typeof(DefinitionA), BuildGraph(), plans);

        _ = cache.TryGetCachedPlans(typeof(DefinitionA), BuildGraph(), out var cached).Should().BeTrue();
        _ = cached.Should().NotBeNull();
    }

    [Fact]
    public void ADifferentPipelineDefinition_DoesNotReuseCachedPlans()
    {
        var cache = new InMemoryPipelineExecutionPlanCache();
        cache.CachePlans(typeof(DefinitionA), BuildGraph(), PlansFor(BuildGraph()));

        _ = cache.TryGetCachedPlans(typeof(DefinitionB), BuildGraph(), out _).Should().BeFalse();
    }

    [Fact]
    public void AGraphDifferingOnlyByNodeKind_DoesNotReuseCachedPlans()
    {
        var cache = new InMemoryPipelineExecutionPlanCache();
        cache.CachePlans(typeof(DefinitionA), BuildGraph(), PlansFor(BuildGraph()));

        // Node kind selects both the delegate shape and the executor's dispatch arm, so it must be part of the key.
        // The old digest omitted it entirely.
        var reKinded = WithSinkKind(BuildGraph(), NodeKind.CompositeOutput);

        _ = cache.TryGetCachedPlans(typeof(DefinitionA), reKinded, out _).Should().BeFalse();
    }

    [Fact]
    public void AGraphDifferingOnlyByExecutionStrategyType_DoesNotReuseCachedPlans()
    {
        var cache = new InMemoryPipelineExecutionPlanCache();
        cache.CachePlans(typeof(DefinitionA), BuildGraph(), PlansFor(BuildGraph()));

        var reStrategied = WithSourceStrategy(BuildGraph(), new BatchingExecutionStrategy(10, TimeSpan.FromSeconds(1)));

        _ = cache.TryGetCachedPlans(typeof(DefinitionA), reStrategied, out _).Should().BeFalse();
    }

    [Fact]
    public void RewritingLineageSettings_StillReusesCachedPlans()
    {
        var cache = new InMemoryPipelineExecutionPlanCache();
        var graph = BuildGraph();
        cache.CachePlans(typeof(DefinitionA), graph, PlansFor(graph));

        // This mirrors what RuntimePipelineBinder does on a run that overrides lineage. Compiled plans hold no lineage
        // state, so the rewritten graph legitimately shares them - but only because the key is recomputed from the
        // graph rather than carried along inside it.
        var rewritten = graph with { Lineage = graph.Lineage with { ItemLevelLineageEnabled = true } };

        _ = rewritten.Lineage.ItemLevelLineageEnabled.Should().BeTrue();
        _ = cache.TryGetCachedPlans(typeof(DefinitionA), rewritten, out _).Should().BeTrue();
    }

    [Fact]
    public void ReplacingAnEntryAtCapacity_DoesNotEvictAnotherEntry()
    {
        var cache = new InMemoryPipelineExecutionPlanCache();
        var graphs = Enumerable.Range(0, CacheCapacity).Select(BuildGraph).ToArray();

        foreach (var graph in graphs)
        {
            cache.CachePlans(typeof(DefinitionA), graph, PlansFor(graph));
        }

        cache.CachePlans(typeof(DefinitionA), graphs[^1], PlansFor(graphs[^1]));

        cache.Count.Should().Be(CacheCapacity);

        foreach (var graph in graphs)
        {
            _ = cache.TryGetCachedPlans(typeof(DefinitionA), graph, out _).Should().BeTrue();
        }
    }

    [Fact]
    public void AddingAnEntryBeyondCapacity_EvictsOneEntryAndRemainsBounded()
    {
        var cache = new InMemoryPipelineExecutionPlanCache();
        var graphs = Enumerable.Range(0, CacheCapacity + 1).Select(BuildGraph).ToArray();

        foreach (var graph in graphs)
        {
            cache.CachePlans(typeof(DefinitionA), graph, PlansFor(graph));
        }

        cache.Count.Should().Be(CacheCapacity);
        _ = cache.TryGetCachedPlans(typeof(DefinitionA), graphs[^1], out _).Should().BeTrue();
    }

    [Fact]
    public void ConcurrentReadsAndInsertions_NeverExceedCapacity()
    {
        var cache = new InMemoryPipelineExecutionPlanCache();
        var graphs = Enumerable.Range(0, CacheCapacity * 3).Select(BuildGraph).ToArray();

        foreach (var graph in graphs.Take(CacheCapacity))
        {
            cache.CachePlans(typeof(DefinitionA), graph, PlansFor(graph));
        }

        Parallel.For(0, 10_000, i =>
        {
            var graph = graphs[i % graphs.Length];

            if ((i & 1) == 0)
                _ = cache.TryGetCachedPlans(typeof(DefinitionA), graph, out _);
            else
                cache.CachePlans(typeof(DefinitionA), graph, PlansFor(graph));
        });

        cache.Count.Should().BeLessThanOrEqualTo(CacheCapacity);
    }

    private static Dictionary<string, NodeExecutionPlan> PlansFor(PipelineGraph graph)
    {
        var source = graph.Nodes[0];

        return new Dictionary<string, NodeExecutionPlan>(StringComparer.Ordinal)
        {
            [source.Id] = new(source.Id, NodeKind.Source, null, typeof(int)),
        };
    }

    private static PipelineGraph BuildGraph()
    {
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<Source, int>("src");
        var sink = builder.AddSink<Sink, int>("snk");
        _ = builder.Connect(source, sink);

        return builder.Build().Graph;
    }

    private static PipelineGraph BuildGraph(int suffix)
    {
        var builder = new PipelineBuilder().WithoutExtendedValidation();
        var source = builder.AddSource<Source, int>($"src-{suffix}");
        var sink = builder.AddSink<Sink, int>($"snk-{suffix}");
        _ = builder.Connect(source, sink);

        return builder.Build().Graph;
    }

    private static PipelineGraph WithSinkKind(PipelineGraph graph, NodeKind kind)
    {
        return graph with
        {
            Nodes =
            [
                .. graph.Nodes.Select(n => n.Id == "snk"
                    ? n with { Kind = kind }
                    : n),
            ],
        };
    }

    private static PipelineGraph WithSourceStrategy(PipelineGraph graph, IExecutionStrategy strategy)
    {
        return graph with
        {
            Nodes =
            [
                .. graph.Nodes.Select(n => n.Id == "src"
                    ? n.WithExecutionStrategy(strategy)
                    : n),
            ],
        };
    }

    private sealed class Source : SourceNode<int>
    {
        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new NPipeline.DataFlow.DataStreams.InMemoryDataStream<int>([1, 2, 3], "src");
    }

    private sealed class Sink : SinkNode<int>
    {
        public override async Task ConsumeAsync(IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
            {
                // Discard.
            }
        }
    }

    private sealed class DefinitionA : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
        }
    }

    private sealed class DefinitionB : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
        }
    }
}
