using System.Collections.Immutable;
using NPipeline.Nodes;

namespace NPipeline.Graph.Validation.Rules;

/// <summary>
///     Rejects resilience options that would silently do nothing: item retry or node restart configured for a node
///     that is not a transform. Only transform nodes retry items or restart their stream.
/// </summary>
/// <remarks>
///     A node's options are derived from the pipeline's, so this rule flags only the settings the node changed. A
///     source inheriting the pipeline's item retry is not an error.
/// </remarks>
internal sealed class ResilienceOptionsRule : IGraphRule
{
    /// <inheritdoc />
    public string Name => "ResilienceOptions";

    /// <inheritdoc />
    public bool StopOnError => false;

    /// <inheritdoc />
    public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
    {
        var graph = context.Graph;
        var nodeResilience = graph.ErrorHandling.NodeResilience;

        if (nodeResilience is not { Count: > 0 } || graph.ErrorHandling.Resilience is not { } pipelineOptions)
            return [];

        var issues = ImmutableList.CreateBuilder<ValidationIssue>();

        foreach (var node in graph.Nodes)
        {
            if (!nodeResilience.TryGetValue(node.Id, out var options) || typeof(ITransformNode).IsAssignableFrom(node.NodeType))
                continue;

            if (options.ItemRetry != pipelineOptions.ItemRetry)
                issues.Add(Unsupported(node, "ItemRetry"));

            if (options.NodeRestart != pipelineOptions.NodeRestart)
                issues.Add(Unsupported(node, "NodeRestart"));
        }

        return issues.ToImmutable();
    }

    private static ValidationIssue Unsupported(NodeDefinition node, string setting)
    {
        return new ValidationIssue(
            ValidationSeverity.Error,
            $"Node '{node.Name}' is a {node.Kind} node, but its resilience options set {setting}, which only transform nodes use. " +
            "Remove the setting, or use NodeRetry to execute the whole node again.",
            "Resilience");
    }
}
