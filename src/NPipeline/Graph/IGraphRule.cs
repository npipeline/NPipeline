namespace NPipeline.Graph;

public interface IGraphRule
{
    string Name { get; }
    bool StopOnError { get; }
    IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context);
}

public sealed class GraphValidationContext(PipelineGraph graph)
{
    private Dictionary<string, List<string>>? _in;
    private Dictionary<string, List<string>>? _out;

    public PipelineGraph Graph { get; } = graph;

    public Dictionary<string, List<string>> Outgoing =>
        _out ??= Graph.Edges.GroupBy(e => e.SourceNodeId).ToDictionary(g => g.Key, g => g.Select(e => e.TargetNodeId).ToList());

    public Dictionary<string, List<string>> Incoming =>
        _in ??= Graph.Edges.GroupBy(e => e.TargetNodeId).ToDictionary(g => g.Key, g => g.Select(e => e.SourceNodeId).ToList());
}
