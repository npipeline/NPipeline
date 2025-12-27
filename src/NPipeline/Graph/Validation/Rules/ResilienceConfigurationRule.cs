using System.Collections.Immutable;
using NPipeline.Execution.Strategies;

namespace NPipeline.Graph.Validation.Rules;

/// <summary>
///     Validates that resilience configuration is complete when ResilientExecutionStrategy is used.
///     Detects common missing configurations that prevent node restarts from working correctly.
/// </summary>
/// <remarks>
///     <para>
///         For node restarts to function properly, the following conditions must ALL be met:
///         <list type="number">
///             <item>The node is wrapped with ResilientExecutionStrategy</item>
///             <item>MaxNodeRestartAttempts > 0 is configured</item>
///             <item>MaxMaterializedItems is set to a positive number (not null)</item>
///             <item>A PipelineErrorHandler is registered (pipeline-level or node-level)</item>
///         </list>
///     </para>
///     <para>
///         This rule generates warnings when any of these prerequisites are missing, helping users
///         avoid silent failures where node restarts are configured but do not execute.
///     </para>
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

        // Find all nodes using ResilientExecutionStrategy
        var nodesWithResilience = graph.Nodes
            .Where(n => n.ExecutionConfig.ExecutionStrategy is ResilientExecutionStrategy)
            .ToList();

        if (nodesWithResilience.Count == 0)
            return issues.ToImmutable();

        // Check if any error handler is configured
        var hasPipelineErrorHandler = graph.ErrorHandling.PipelineErrorHandler is not null
                                      || graph.ErrorHandling.PipelineErrorHandlerType is not null;

        foreach (var node in nodesWithResilience)
        {
            // Check for error handler
            if (!hasPipelineErrorHandler)
            {
                issues.Add(new ValidationIssue(
                    ValidationSeverity.Warning,
                    $"Node '{node.Name}' uses ResilientExecutionStrategy but no IPipelineErrorHandler is configured. " +
                    $"Node restarts will not work without an error handler to make restart decisions. " +
                    $"Configure: builder.AddPipelineErrorHandler<YourHandler>() or use " +
                    $"builder.WithStandardResilience() for automatic configuration.",
                    "Resilience"));
            }

            // Get effective retry options (prefer node-specific, then graph-level)
            var retryOptions = graph.ErrorHandling.NodeRetryOverrides?.GetValueOrDefault(node.Id)
                               ?? graph.ErrorHandling.RetryOptions;

            if (retryOptions is null)
            {
                issues.Add(new ValidationIssue(
                    ValidationSeverity.Warning,
                    $"Node '{node.Name}' uses ResilientExecutionStrategy but retry options are not configured. " +
                    $"Set MaxNodeRestartAttempts > 0 and MaxMaterializedItems to enable restarts. " +
                    $"Configure: builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 3, maxMaterializedItems: 1000))",
                    "Resilience"));

                continue;
            }

            // Check MaxNodeRestartAttempts
            if (retryOptions.MaxNodeRestartAttempts <= 0)
            {
                issues.Add(new ValidationIssue(
                    ValidationSeverity.Warning,
                    $"Node '{node.Name}' uses ResilientExecutionStrategy but MaxNodeRestartAttempts is {retryOptions.MaxNodeRestartAttempts} (not > 0). " +
                    $"The node will not restart on failures. " +
                    $"Configure: builder.WithRetryOptions(o => o.With(maxNodeRestartAttempts: 3))",
                    "Resilience"));
            }

            // Check MaxMaterializedItems
            if (retryOptions.MaxMaterializedItems is null)
            {
                issues.Add(new ValidationIssue(
                    ValidationSeverity.Warning,
                    $"Node '{node.Name}' uses ResilientExecutionStrategy but MaxMaterializedItems is null (unbounded). " +
                    $"This disables materialization, preventing node restarts and allowing unlimited memory growth. " +
                    $"Configure: builder.WithRetryOptions(o => o.With(maxMaterializedItems: 1000))",
                    "Resilience"));
            }
            else if (retryOptions.MaxMaterializedItems <= 0)
            {
                issues.Add(new ValidationIssue(
                    ValidationSeverity.Warning,
                    $"Node '{node.Name}' uses ResilientExecutionStrategy but MaxMaterializedItems is {retryOptions.MaxMaterializedItems} (not > 0). " +
                    $"Materialization is disabled, preventing restarts. " +
                    $"Configure: builder.WithRetryOptions(o => o.With(maxMaterializedItems: 1000))",
                    "Resilience"));
            }
        }

        return issues.ToImmutable();
    }
}
