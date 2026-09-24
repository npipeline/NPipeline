using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using NPipeline.Attributes;
using NPipeline.Execution.CircuitBreaking;
using NPipeline.Graph;
using NPipeline.Lineage;

namespace NPipeline.Pipeline;

/// <summary>
///     Creates pipelines from definitions.
/// </summary>
/// <remarks>
///     A definition marked <see cref="CacheableGraphAttribute" /> is built once and its graph reused, so the per-run
///     cost of <c>Define</c>, the immutable and frozen collections, the child graphs and the full validation rule set
///     is paid on the first run only. Everything else is rebuilt per run exactly as before.
///     <para>
///         The factory also owns the circuit breakers of every definition it builds, one registry per definition type,
///         so a node's breaker state survives from one run to the next for as long as the factory lives. Register the
///         factory as a singleton, as <c>AddNPipeline</c> does, to get that behavior.
///     </para>
/// </remarks>
public sealed class PipelineFactory : IPipelineFactory
{
    /// <summary>
    ///     Upper bound on cached graphs. A definition type count is finite and a lineage module is normally a
    ///     singleton, so this is a backstop against a caller that churns modules, not an eviction policy: once the
    ///     cache is full it simply stops growing and later definitions rebuild per run.
    /// </summary>
    private const int MaxCachedGraphs = 64;

    private static readonly ConcurrentDictionary<Type, bool> CacheableDefinitions = new();
    private readonly ConcurrentDictionary<Type, CircuitBreakerRegistry> _circuitBreakers = new();
    private readonly ConcurrentDictionary<GraphCacheKey, PipelineGraph> _graphCache = new();
    private readonly ConcurrentDictionary<Type, bool> _uncacheableReported = new();

    /// <inheritdoc />
    public Pipeline Create<TDefinition>(PipelineContext context) where TDefinition : IPipelineDefinition, new()
    {
        ArgumentNullException.ThrowIfNull(context);
        return BuildPipeline(new TDefinition(), context);
    }

    /// <inheritdoc />
    public Pipeline Create(IPipelineDefinition definition, PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(context);

        return BuildPipeline(definition, context);
    }

    private Pipeline BuildPipeline(IPipelineDefinition definition, PipelineContext context)
    {
        var pipeline = BuildPipelineCore(definition, context);
        pipeline.CircuitBreakers = _circuitBreakers.GetOrAdd(definition.GetType(), static _ => new CircuitBreakerRegistry());
        return pipeline;
    }

    private Pipeline BuildPipelineCore(IPipelineDefinition definition, PipelineContext context)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(context);

        var definitionType = definition.GetType();

        if (!CanCacheGraph(definitionType, context))
            return BuildFresh(definition, context);

        // The graph carries lineage adapters built from the run's lineage module, so a graph built for one module
        // must never be handed to a runner using another. Keying on the module keeps findings 3's bug class closed.
        var key = new GraphCacheKey(definitionType, context.Lineage.Module);

        if (_graphCache.TryGetValue(key, out var cachedGraph))
            return new Pipeline(cachedGraph);

        var pipeline = BuildFresh(definition, context);

        if (IsReusable(definitionType, pipeline) && _graphCache.Count < MaxCachedGraphs)
            _ = _graphCache.TryAdd(key, pipeline.Graph);

        return pipeline;
    }

    private static Pipeline BuildFresh(IPipelineDefinition definition, PipelineContext context)
    {
        // Build with the lineage module the run will actually use, so build-time adapters and runtime lineage
        // handling cannot come from different instances.
        var builder = new PipelineBuilder(context.Lineage.Module);

        definition.Define(builder, context);

        // Allow tests / advanced users to supply preconfigured node instances via context.
        if (context.NodeEnvironment.PreconfiguredNodeInstances.Count > 0)
        {
            foreach (var kvp in context.NodeEnvironment.PreconfiguredNodeInstances)
            {
                // Best-effort: ignore duplicates (will throw) so wrap in try/catch.
                try
                {
                    builder.AddPreconfiguredNodeInstance(kvp.Key, kvp.Value);
                }
                catch
                {
                    /* ignore */
                }
            }
        }

        return builder.Build();
    }

    private static bool CanCacheGraph(Type definitionType, PipelineContext context)
    {
        // A context that supplies its own node instances produces a graph specific to that run.
        if (context.NodeEnvironment.PreconfiguredNodeInstances.Count > 0)
            return false;

        return CacheableDefinitions.GetOrAdd(
            definitionType,
            static t => Attribute.IsDefined(t, typeof(CacheableGraphAttribute), false));
    }

    /// <summary>
    ///     Decides whether a freshly built pipeline may be reused by later runs.
    /// </summary>
    /// <remarks>
    ///     Preconfigured node instances and builder disposables are both owned by the run that built them and are
    ///     disposed when it ends, so a graph carrying either would hand the next run disposed objects. This is the
    ///     defect finding 1 fixed for the plan cache, and the same guard belongs here.
    /// </remarks>
    private bool IsReusable(Type definitionType, Pipeline pipeline)
    {
        if (pipeline.Graph.PreconfiguredNodeInstances.Count == 0 && pipeline.BuilderDisposables.Count == 0)
            return true;

        if (_uncacheableReported.TryAdd(definitionType, true))
        {
            Trace.TraceWarning(
                $"[NPipeline] Pipeline definition '{definitionType.FullName}' is marked [CacheableGraph] but registers node instances at " +
                "definition time, so its graph is rebuilt on every run. Those instances are disposed when their run ends and cannot be " +
                "shared with a later one. Register the node types instead of instances, or remove [CacheableGraph].");
        }

        return false;
    }

    /// <summary>
    ///     Identifies a cached graph by the definition that produced it and the lineage module it was built against.
    /// </summary>
    /// <remarks>
    ///     The module is compared by reference. What matters is that the graph's lineage adapters came from that exact
    ///     instance, which is not something a module's own <c>Equals</c> can speak for.
    /// </remarks>
    private readonly struct GraphCacheKey(Type definitionType, ILineage lineageModule) : IEquatable<GraphCacheKey>
    {
        private readonly Type _definitionType = definitionType;
        private readonly ILineage _lineageModule = lineageModule;

        public bool Equals(GraphCacheKey other) => _definitionType == other._definitionType && ReferenceEquals(_lineageModule, other._lineageModule);

        public override bool Equals(object? obj) => obj is GraphCacheKey other && Equals(other);

        public override int GetHashCode() => HashCode.Combine(_definitionType, RuntimeHelpers.GetHashCode(_lineageModule));
    }
}
