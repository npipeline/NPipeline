using System.Collections.Immutable;
using NPipeline.Execution.Strategies;
using NPipeline.Reliability;

namespace NPipeline.Graph.Validation.Rules;

/// <summary>
///     Warns about resilience configuration that is valid but probably not what was intended.
/// </summary>
/// <remarks>
///     <list type="bullet">
///         <item>
///             <description>
///                 A node wrapped for restart whose options allow no restarts, with no custom policy that could
///                 restart it anyway.
///             </description>
///         </item>
///         <item>
///             <description>A circuit breaker configured where nothing is ever retried or restarted.</description>
///         </item>
///     </list>
/// </remarks>
internal sealed class ResilienceConfigurationRule : IGraphRule
{
    /// <inheritdoc />
    public string Name => "ResilienceConfiguration";

    /// <inheritdoc />
    public bool StopOnError => false;

    /// <inheritdoc />
    public IEnumerable<ValidationIssue> Evaluate(GraphValidationContext context)
    {
        var graph = context.Graph;
        var issues = ImmutableList.CreateBuilder<ValidationIssue>();
        var hasCustomPolicy = graph.ErrorHandling.ResiliencePolicy is not null and not DefaultResiliencePolicy
                              || graph.ErrorHandling.ResiliencePolicyType is not null;

        foreach (var node in graph.Nodes)
        {
            var options = OptionsFor(graph, node.Id);

            if (node.ExecutionStrategy is ResilientExecutionStrategy && options.NodeRestart.MaxRestarts == 0 && !hasCustomPolicy)
            {
                issues.Add(new ValidationIssue(
                    ValidationSeverity.Warning,
                    $"Node '{node.Name}' is wrapped for restart, but its NodeRestart.MaxRestarts is 0, so it will never restart. " +
                    "Configure: builder.WithResilience(handle, o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 3 } })",
                    "Resilience"));
            }

            if (options.CircuitBreaker is { Enabled: true } &&
                options.ItemRetry.MaxRetries == 0 && options.NodeRestart.MaxRestarts == 0 && options.NodeRetry.MaxRetries == 0 &&
                !hasCustomPolicy)
            {
                issues.Add(new ValidationIssue(
                    ValidationSeverity.Warning,
                    $"Node '{node.Name}' has a circuit breaker, but nothing on it is ever retried or restarted, so the breaker has nothing to stop.",
                    "Resilience"));
            }
        }

        return issues.ToImmutable();
    }

    private static PipelineResilienceOptions OptionsFor(PipelineGraph graph, string nodeId)
    {
        if (graph.ErrorHandling.NodeResilience?.TryGetValue(nodeId, out var nodeOptions) == true)
            return nodeOptions;

        return graph.ErrorHandling.Resilience ?? PipelineResilienceOptions.None;
    }
}
