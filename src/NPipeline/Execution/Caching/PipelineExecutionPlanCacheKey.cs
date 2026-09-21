using System.Diagnostics.CodeAnalysis;
using NPipeline.Graph;

namespace NPipeline.Execution.Caching;

/// <summary>
///     Identifies a set of compiled execution plans by the graph properties those plans are actually built from.
/// </summary>
/// <remarks>
///     <para>
///         The key is derived from exactly the inputs
///         <see cref="Services.NodeInstantiationService.BuildPlans" /> reads: for each node, its id, kind, node type,
///         input and output types, and execution strategy type. Nothing else about the graph can change a plan, so
///         nothing else belongs in the key.
///     </para>
///     <para>
///         Deriving the key this way, rather than hashing the graph as a whole, keeps it correct by construction. A
///         graph-wide hash has to be kept in step by hand with every field that plans happen to consume, and it silently
///         goes stale whenever a <c>with</c> expression rewrites the graph and copies the old hash forward - which the
///         runtime binder does on every run that overrides lineage settings.
///     </para>
///     <para>
///         Comparison is exact rather than by digest, so two different graphs cannot collide onto one another's plans.
///     </para>
/// </remarks>
internal sealed class PipelineExecutionPlanCacheKey : IEquatable<PipelineExecutionPlanCacheKey>
{
    private readonly Type _definitionType;
    private readonly int _hashCode;
    private readonly NodePlanSignature[] _nodes;

    private PipelineExecutionPlanCacheKey(Type definitionType, NodePlanSignature[] nodes)
    {
        _definitionType = definitionType;
        _nodes = nodes;

        var hash = new HashCode();
        hash.Add(definitionType);
        hash.Add(nodes.Length);

        foreach (var node in nodes)
        {
            hash.Add(node);
        }

        _hashCode = hash.ToHashCode();
    }

    /// <summary>
    ///     Builds the key for a pipeline definition and the graph it produced.
    /// </summary>
    /// <remarks>
    ///     Nodes are compared in graph order. A definition that emits its nodes in a different order than a previous run
    ///     produces a different key and simply misses the cache, which costs a rebuild but never returns another graph's
    ///     plans.
    /// </remarks>
    public static PipelineExecutionPlanCacheKey Create(Type definitionType, PipelineGraph graph)
    {
        ArgumentNullException.ThrowIfNull(definitionType);
        ArgumentNullException.ThrowIfNull(graph);

        var nodes = new NodePlanSignature[graph.Nodes.Length];

        for (var i = 0; i < graph.Nodes.Length; i++)
        {
            var node = graph.Nodes[i];

            nodes[i] = new NodePlanSignature(
                node.Id,
                node.Kind,
                node.NodeType,
                node.InputType,
                node.OutputType,
                node.ExecutionStrategy?.GetType());
        }

        return new PipelineExecutionPlanCacheKey(definitionType, nodes);
    }

    /// <inheritdoc />
    public bool Equals(PipelineExecutionPlanCacheKey? other)
    {
        if (ReferenceEquals(this, other))
            return true;

        if (other is null || _hashCode != other._hashCode || _definitionType != other._definitionType)
            return false;

        return _nodes.AsSpan().SequenceEqual(other._nodes.AsSpan());
    }

    /// <inheritdoc />
    public override bool Equals(object? obj)
    {
        return Equals(obj as PipelineExecutionPlanCacheKey);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return _hashCode;
    }

    /// <summary>
    ///     The per-node properties a compiled execution plan is built from.
    /// </summary>
    /// <param name="Id">Node identifier, which the plan carries.</param>
    /// <param name="Kind">Node kind, which selects the delegate shape and the executor's dispatch arm.</param>
    /// <param name="NodeType">Concrete node type, which decides the node interface the delegate is compiled against.</param>
    /// <param name="InputType">Declared input item type, baked into the compiled cast.</param>
    /// <param name="OutputType">Declared output item type, baked into the compiled cast and the output adapter.</param>
    /// <param name="ExecutionStrategyType">
    ///     Execution strategy type declared on the node definition. The strategy instance is resolved per run from the
    ///     node itself, so only the type can affect the compiled delegate.
    /// </param>
    [SuppressMessage("Performance", "CA1815:Override equals and operator equals on value types",
        Justification = "Record struct already provides value equality; operators are not used.")]
    private readonly record struct NodePlanSignature(
        string Id,
        NodeKind Kind,
        Type NodeType,
        Type? InputType,
        Type? OutputType,
        Type? ExecutionStrategyType);
}
