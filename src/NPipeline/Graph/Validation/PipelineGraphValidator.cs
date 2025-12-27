using System.Collections.Immutable;
using NPipeline.Graph.Validation.Rules;
using NPipeline.Nodes;

namespace NPipeline.Graph.Validation;

/// <summary>
///     Validates structural correctness and sanity constraints of a <see cref="PipelineGraph" /> using modular rules.
/// </summary>
public static class PipelineGraphValidator
{
    // Core validation rules for pipeline graphs
    private static readonly IReadOnlyList<IGraphRule> CoreRules =
    [
        new UniqueNodeNameRule(),
        new DuplicateNodeIdRule(),
        new EdgeReferenceRule(),
        new SourceAndReachabilityRule(),
        new CycleDetectionRule(),
    ];

    // Extended rules (enabled by default, opt-out via builder.WithoutExtendedValidation())
    internal static readonly IReadOnlyList<IGraphRule> ExtendedRules =
    [
        new MissingSinkRule(),
        new SelfLoopRule(),
        new DuplicateEdgeRule(),
        new TypeCompatibilityRule(),
        new ResilienceConfigurationRule(),
        new ParallelConfigurationRule(),
    ];

    /// <summary>
    ///     Validates a pipeline graph using core validation rules and any additional rules provided.
    /// </summary>
    /// <param name="graph">The pipeline graph to validate.</param>
    /// <param name="extraRules">Optional additional validation rules to apply.</param>
    /// <returns>A <see cref="PipelineValidationResult" /> containing all validation issues found.</returns>
    public static PipelineValidationResult Validate(PipelineGraph graph, IEnumerable<IGraphRule>? extraRules = null)
    {
        var issues = ImmutableList.CreateBuilder<ValidationIssue>();
        var ctx = new GraphValidationContext(graph);
        var rules = CoreRules.Concat(extraRules ?? []);

        foreach (var r in rules)
        {
            foreach (var issue in r.Evaluate(ctx))
            {
                issues.Add(issue);
            }

            if (r.StopOnError && issues.Any(i => i.Severity == ValidationSeverity.Error))
                break;
        }

        return new PipelineValidationResult(issues.ToImmutable());
    }

    private static bool IsSourceNode(Type nodeType)
    {
        // Check if the type implements any ISourceNode<T>
        return nodeType.GetInterfaces().Any(i =>
            i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ISourceNode<>));
    }

    private static bool IsSinkNode(Type nodeType)
    {
        // Check if the type implements any ISinkNode<T>
        return nodeType.GetInterfaces().Any(i =>
            i.IsGenericType && i.GetGenericTypeDefinition() == typeof(ISinkNode<>));
    }

    private sealed class DuplicateNodeIdRule : IGraphRule
    {
        public string Name => "DuplicateIds";
        public bool StopOnError => true;

        public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
        {
            var dupGroups = context.Graph.Nodes
                .GroupBy(n => n.Id)
                .Where(g => g.Count() > 1)
                .ToList();

            if (dupGroups.Count > 0)
            {
                var details = dupGroups
                    .Select(g => $"{g.Key}: [{string.Join(", ", g.Select(n => $"{n.Name} ({n.NodeType.Name})"))}]");

                yield return new ValidationIssue(
                    ValidationSeverity.Error,
                    $"Duplicate node IDs detected: {string.Join("; ", details)}",
                    "Structure");
            }
        }
    }

    private sealed class EdgeReferenceRule : IGraphRule
    {
        public string Name => "EdgeReferences";
        public bool StopOnError => true;

        public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
        {
            var nodeMap = context.Graph.Nodes.ToDictionary(n => n.Id);

            foreach (var e in context.Graph.Edges)
            {
                nodeMap.TryGetValue(e.SourceNodeId, out var src);
                nodeMap.TryGetValue(e.TargetNodeId, out var dst);

                if (src is null)
                {
                    var dstInfo = dst is null
                        ? "(unknown)"
                        : $"'{dst.Name}', {dst.NodeType.Name}";

                    yield return new ValidationIssue(
                        ValidationSeverity.Error,
                        $"Edge references unknown source node '{e.SourceNodeId}' → '{e.TargetNodeId}' ({dstInfo})",
                        "References");
                }

                if (dst is null)
                {
                    var srcInfo = src is null
                        ? "(unknown)"
                        : $"'{src.Name}', {src.NodeType.Name}";

                    yield return new ValidationIssue(
                        ValidationSeverity.Error,
                        $"Edge references unknown target node '{e.TargetNodeId}' ← '{e.SourceNodeId}' ({srcInfo})",
                        "References");
                }
            }
        }
    }

    private sealed class SourceAndReachabilityRule : IGraphRule
    {
        public string Name => "SourcesReachability";
        public bool StopOnError => false;

        public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
        {
            var g = context.Graph;
            var outgoing = context.Outgoing;
            var incoming = context.Incoming;
            var nodeMap = g.Nodes.ToDictionary(n => n.Id);

            if (g.Edges.Count == 0 && g.Nodes.Count > 0)
            {
                var hasSource = g.Nodes.Any(n => IsSourceNode(n.NodeType));

                if (!hasSource)
                {
                    yield return new ValidationIssue(ValidationSeverity.Error, "Pipeline has no source nodes (at least one ISourceNode<T> is required).",
                        "Sources");
                }

                yield break;
            }

            if (g.Edges.Count > 0)
            {
                var sourceCandidates = g.Nodes.Where(n => !incoming.ContainsKey(n.Id)).ToList();

                if (sourceCandidates.Count == 0)
                    yield return new ValidationIssue(ValidationSeverity.Error, "Pipeline has no source nodes (nodes with zero inbound edges).", "Sources");

                var nonSourceZeroInbound = sourceCandidates.Where(n => !IsSourceNode(n.NodeType)).ToList();

                if (nonSourceZeroInbound.Count > 0)
                {
                    var details = string.Join(", ", nonSourceZeroInbound.Select(n => $"{n.Id} ('{n.Name}', {n.NodeType.Name})"));

                    yield return new ValidationIssue(ValidationSeverity.Error,
                        $"Non-source nodes with no inbound edges: {details}", "Sources");
                }

                var actualSourceIds = sourceCandidates.Where(n => IsSourceNode(n.NodeType)).Select(n => n.Id).ToList();
                var reachable = new HashSet<string>();
                var queue = new Queue<string>(actualSourceIds);

                while (queue.Count > 0)
                {
                    var current = queue.Dequeue();

                    if (!reachable.Add(current))
                        continue;

                    if (outgoing.TryGetValue(current, out var targets))
                    {
                        foreach (var t in targets)
                        {
                            queue.Enqueue(t);
                        }
                    }
                }

                var unreachable = g.Nodes.Select(n => n.Id).Where(id => !reachable.Contains(id)).ToList();

                if (unreachable.Count > 0)
                {
                    var details = string.Join(", ",
                        unreachable.Select(id => nodeMap.TryGetValue(id, out var n)
                            ? $"{id} ('{n.Name}', {n.NodeType.Name})"
                            : id));

                    yield return new ValidationIssue(ValidationSeverity.Error,
                        $"Unreachable nodes (not connected to any source): {details}", "Reachability");
                }

                var isolated = g.Nodes.Where(n => !incoming.ContainsKey(n.Id) && !outgoing.ContainsKey(n.Id)).ToList();

                if (isolated.Count > 0 && g.Nodes.Count > 1)
                {
                    var details = string.Join(", ", isolated.Select(n => $"{n.Id} ('{n.Name}', {n.NodeType.Name})"));
                    yield return new ValidationIssue(ValidationSeverity.Error, $"Isolated nodes (no edges): {details}", "Reachability");
                }
            }
        }
    }

    private sealed class CycleDetectionRule : IGraphRule
    {
        public string Name => "Cycles";
        public bool StopOnError => false;

        public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
        {
            var outgoing = context.Outgoing;
            var temp = new HashSet<string>();
            var perm = new HashSet<string>();
            var stack = new Stack<string>();
            List<string>? foundCycle = null;

            foreach (var n in context.Graph.Nodes)
            {
                if (HasCycle(n.Id))
                {
                    if (foundCycle is not null)
                        yield return new ValidationIssue(ValidationSeverity.Error, $"Cycle detected: {string.Join(" -> ", foundCycle)}", "Cycles");
                    else
                        yield return new ValidationIssue(ValidationSeverity.Error, "Cycle detected in graph (execution requires a DAG).", "Cycles");

                    yield break;
                }
            }

            yield break;

            bool HasCycle(string id)
            {
                if (perm.Contains(id))
                    return false;

                if (!temp.Add(id))
                {
                    stack.Push(id);
                    var arr = stack.Reverse().ToList();
                    var idx = arr.IndexOf(id);
                    foundCycle = arr.Skip(idx).Concat([id]).ToList();
                    return true;
                }

                stack.Push(id);

                if (outgoing.TryGetValue(id, out var targets))
                {
                    foreach (var t in targets)
                    {
                        if (HasCycle(t))
                            return true;
                    }
                }

                temp.Remove(id);
                perm.Add(id);
                stack.Pop();
                return false;
            }
        }
    }

    private sealed class MissingSinkRule : IGraphRule
    {
        public string Name => "MissingSink";
        public bool StopOnError => false;

        public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
        {
            if (context.Graph.Nodes.Count == 0)
                yield break;

            // A sink is a node with no outgoing edges and kind Sink.
            var hasSink = context.Graph.Nodes.Any(n => n.Kind == NodeKind.Sink);

            if (!hasSink)
            {
                yield return new ValidationIssue(ValidationSeverity.Error,
                    "Pipeline has no sink nodes (at least one ISinkNode<T> required to materialize results).", "Structure");
            }
        }
    }

    private sealed class SelfLoopRule : IGraphRule
    {
        public string Name => "SelfLoop";
        public bool StopOnError => true;

        public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
        {
            var nodeMap = context.Graph.Nodes.ToDictionary(n => n.Id);

            foreach (var e in context.Graph.Edges)
            {
                if (e.SourceNodeId == e.TargetNodeId)
                {
                    var n = nodeMap.GetValueOrDefault(e.SourceNodeId);

                    var detail = n is null
                        ? e.SourceNodeId
                        : $"{n.Id} ('{n.Name}', {n.NodeType.Name})";

                    yield return new ValidationIssue(ValidationSeverity.Error, $"Self-loop detected on node {detail}", "Structure");

                    yield break;
                }
            }
        }
    }

    private sealed class DuplicateEdgeRule : IGraphRule
    {
        public string Name => "DuplicateEdge";
        public bool StopOnError => false;

        public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
        {
            var nodeMap = context.Graph.Nodes.ToDictionary(n => n.Id);

            var dupGroups = context.Graph.Edges
                .GroupBy(e => (e.SourceNodeId, e.TargetNodeId, e.SourceOutputName, e.TargetInputName))
                .Where(g => g.Count() > 1);

            foreach (var g in dupGroups)
            {
                var srcInfo = nodeMap.TryGetValue(g.Key.SourceNodeId, out var src)
                    ? $"'{src.Name}', {src.NodeType.Name}"
                    : "unknown";

                var dstInfo = nodeMap.TryGetValue(g.Key.TargetNodeId, out var dst)
                    ? $"'{dst.Name}', {dst.NodeType.Name}"
                    : "unknown";

                yield return new ValidationIssue(
                    ValidationSeverity.Error,
                    $"Duplicate edge {g.Key.SourceNodeId} -> {g.Key.TargetNodeId} (count={g.Count()}) [{srcInfo} -> {dstInfo}]",
                    "Structure");
            }
        }
    }

    private sealed class TypeCompatibilityRule : IGraphRule
    {
        public string Name => "TypeCompatibility";
        public bool StopOnError => false;

        public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
        {
            var nodeMap = context.Graph.Nodes.ToDictionary(n => n.Id);

            foreach (var e in context.Graph.Edges)
            {
                if (!nodeMap.TryGetValue(e.SourceNodeId, out var src) || !nodeMap.TryGetValue(e.TargetNodeId, out var dst))
                    continue; // already handled by earlier rule

                if (src.OutputType is null || dst.InputType is null)
                    continue; // source may be raw or sink w/out typing

                // Skip type checking for join nodes - they have two input types (TLeft, TRight) and InputType only represents TLeft
                if (dst.IsJoin)
                    continue;

                if (!dst.InputType.IsAssignableFrom(src.OutputType))
                {
                    yield return new ValidationIssue(
                        ValidationSeverity.Error,
                        $"Type mismatch: {src.Id} ('{src.Name}', {src.NodeType.Name}) outputs {src.OutputType.Name} not assignable to {dst.Id} ('{dst.Name}', {dst.NodeType.Name}) input {dst.InputType.Name}",
                        "Types");
                }
            }
        }
    }

    private sealed class UniqueNodeNameRule : IGraphRule
    {
        public string Name => "UniqueNodeNames";
        public bool StopOnError => true;

        public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
        {
            var duplicateNames = context.Graph.Nodes
                .GroupBy(n => n.Name)
                .Where(g => g.Count() > 1)
                .Select(g => g.Key)
                .ToList();

            if (duplicateNames.Count > 0)
            {
                yield return new ValidationIssue(
                    ValidationSeverity.Error,
                    $"Duplicate node names detected: {string.Join(", ", duplicateNames)}. Node names must be unique.",
                    "Structure"
                );
            }
        }
    }
}
