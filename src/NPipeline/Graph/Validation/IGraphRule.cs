namespace NPipeline.Graph.Validation;

/// <summary>
///     Defines a validation rule for pipeline graphs.
/// </summary>
public interface IGraphRule
{
    /// <summary>
    ///     Gets the name of the validation rule.
    /// </summary>
    string Name { get; }

    /// <summary>
    ///     Gets a value indicating whether validation should stop when this rule encounters an error.
    /// </summary>
    bool StopOnError { get; }

    /// <summary>
    ///     Evaluates the validation rule against the provided graph context.
    /// </summary>
    /// <param name="context">The validation context containing the graph to validate.</param>
    /// <returns>A collection of validation issues found during evaluation.</returns>
    IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context);
}

/// <summary>
///     Provides context for graph validation operations, including the graph and precomputed edge relationships.
/// </summary>
/// <param name="graph">The pipeline graph to validate.</param>
public sealed class GraphValidationContext(PipelineGraph graph)
{
    private Dictionary<string, List<string>>? _in;
    private Dictionary<string, List<string>>? _out;

    /// <summary>
    ///     Gets the pipeline graph being validated.
    /// </summary>
    public PipelineGraph Graph { get; } = graph;

    /// <summary>
    ///     Gets a dictionary mapping node IDs to their outgoing edge target node IDs.
    /// </summary>
    public Dictionary<string, List<string>> Outgoing =>
        _out ??= Graph.Edges.GroupBy(e => e.SourceNodeId).ToDictionary(g => g.Key, g => g.Select(e => e.TargetNodeId).ToList());

    /// <summary>
    ///     Gets a dictionary mapping node IDs to their incoming edge source node IDs.
    /// </summary>
    public Dictionary<string, List<string>> Incoming =>
        _in ??= Graph.Edges.GroupBy(e => e.TargetNodeId).ToDictionary(g => g.Key, g => g.Select(e => e.SourceNodeId).ToList());
}
