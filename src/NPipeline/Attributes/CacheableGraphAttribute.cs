namespace NPipeline.Attributes;

/// <summary>
///     Declares that a pipeline definition builds the same graph on every run, so the graph can be built once and
///     reused instead of being rebuilt, revalidated and re-frozen on every <c>RunAsync</c>.
/// </summary>
/// <remarks>
///     <para>
///         Building a graph is not cheap: <c>Define</c> runs, the nodes and edges are copied into immutable arrays, two
///         frozen dictionaries are constructed, child graphs are built for composite nodes, and the full validation
///         rule set runs. For a service executing a short pipeline per request that fixed cost dominates. Marking the
///         definition with this attribute moves all of it to the first run.
///     </para>
///     <para>
///         The contract you are signing up to is that <c>Define</c> is a pure function of nothing — it must ignore the
///         <see cref="Pipeline.PipelineContext" /> it is handed, because that context belongs to one run and the graph
///         it produces will serve every later run. A definition that reads <c>context.Parameters</c> to decide which
///         nodes to add, or which sink to configure, must not be marked.
///     </para>
///     <para>
///         Reading the context from inside a <em>node</em> is entirely fine and is the normal way to parameterise a
///         run. Nodes are instantiated per run and receive the run's own context; only <c>Define</c> is constrained.
///     </para>
///     <para>
///         Two shapes are refused at build time rather than cached, because reusing them would hand a later run
///         objects the first run has already disposed:
///         <list type="bullet">
///             <item>
///                 a definition that registers a preconfigured node instance — every builder overload that takes a
///                 node instance rather than a node type does this, and the instance is disposed when its run ends;
///             </item>
///             <item>a run whose context supplies preconfigured node instances of its own.</item>
///         </list>
///         Either one falls back to a normal rebuild, and the first is reported through
///         <see cref="System.Diagnostics.Trace" /> once per definition type, since it means the attribute is not
///         buying what it claims to.
///     </para>
///     <para>
///         Anything else <c>Define</c> configures — a resilience policy, a dead-letter sink, a lineage sink, an
///         execution strategy supplied as an instance — is shared by every run that uses the cached graph. Supply those
///         as types rather than instances if they hold per-run state.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// [CacheableGraph]
/// public sealed class OrderProcessingPipeline : IPipelineDefinition
/// {
///     public void Define(PipelineBuilder builder, PipelineContext context)
///     {
///         var source = builder.AddSource&lt;OrderSource, Order&gt;("orders");
///         var sink = builder.AddSink&lt;OrderSink, Order&gt;("write");
///         builder.Connect(source, sink);
///     }
/// }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class CacheableGraphAttribute : Attribute;
