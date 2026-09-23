using System.Collections.Immutable;
using NPipeline.Execution;
using NPipeline.Execution.Strategies;
using NPipeline.Nodes;
using NPipeline.Reliability;

namespace NPipeline.Graph.Validation.Rules;

/// <summary>
///     Rejects resilience options that would silently do nothing: item retry, node restart, or a circuit breaker
///     configured for a node that is not a transform. Only transform nodes retry items, restart their stream, or have
///     their attempts guarded by a breaker. Also rejects node restart on a transform whose execution strategy cannot
///     resume.
/// </summary>
/// <remarks>
///     A node's options are derived from the pipeline's, so the node-kind check flags only the settings the node
///     changed. A source inheriting the pipeline's item retry is not an error. Restarting from the beginning instead
///     of from the checkpoint would deliver items twice, so a strategy that cannot resume is an error, not a fallback.
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
        var pipelineOptions = graph.ErrorHandling.Resilience ?? PipelineResilienceOptions.None;
        var issues = ImmutableList.CreateBuilder<ValidationIssue>();

        foreach (var node in graph.Nodes)
        {
            PipelineResilienceOptions? nodeOptions = null;
            _ = nodeResilience?.TryGetValue(node.Id, out nodeOptions);

            if (typeof(ITransformNode).IsAssignableFrom(node.NodeType))
            {
                if ((nodeOptions ?? pipelineOptions).NodeRestart.MaxRestarts > 0
                    && node.ExecutionStrategy is { } strategy and not ResilientExecutionStrategy and not IResumableExecutionStrategy)
                {
                    issues.Add(new ValidationIssue(
                        ValidationSeverity.Error,
                        ErrorMessages.NodeRestartRequiresResumableStrategy(node.Name, strategy.GetType().Name),
                        "Resilience"));
                }

                continue;
            }

            if (nodeOptions is not { } options)
                continue;

            if (options.ItemRetry != pipelineOptions.ItemRetry)
                issues.Add(Unsupported(node, "ItemRetry"));

            if (options.NodeRestart != pipelineOptions.NodeRestart)
                issues.Add(Unsupported(node, "NodeRestart"));

            if (options.CircuitBreaker != pipelineOptions.CircuitBreaker)
                issues.Add(Unsupported(node, "CircuitBreaker"));
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
