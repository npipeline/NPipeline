using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Detects a resilience policy whose <c>DecideItemFailureAsync</c> or <c>DecideNodeFailureAsync</c> returns
///     <c>ResilienceDecision.Retry</c> without consulting the failure's retry budget (<c>CanRetry</c>,
///     <c>IsTransient</c>, <c>Attempt</c>, <c>MaxRetries</c>, <c>IsBreakerOpen</c>, or <c>InputConsumed</c>).
/// </summary>
/// <remarks>
///     The policy owns the decision and the runtime never overrides it, so such a policy retries permanent failures,
///     ignores the node's <c>MaxRetries</c>, and keeps calling a dependency whose breaker is open, until the runtime's
///     safety ceiling of 100 repeats fails the node.
/// </remarks>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class UnconditionalRetryDecisionAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for a Retry decision that ignores the retry budget.
    /// </summary>
    public const string UnconditionalRetryDecisionId = "NP9205";

    private const string ReliabilityNamespace = "NPipeline.Reliability";

    private static readonly ImmutableHashSet<string> BudgetMembers =
        ImmutableHashSet.Create("CanRetry", "IsTransient", "Attempt", "MaxRetries", "IsBreakerOpen", "InputConsumed");

    private static readonly DiagnosticDescriptor Rule = new(
        UnconditionalRetryDecisionId,
        "Resilience policy returns Retry without consulting the retry budget",
        "{0} returns ResilienceDecision.Retry without checking '{1}.CanRetry' (or IsTransient, Attempt, MaxRetries), "
        + "so it retries permanent failures and ignores the node's retry limit",
        "Reliability & Error Handling",
        DiagnosticSeverity.Warning,
        true,
        "The runtime carries out a policy's Retry without second-guessing it. Guard it with failure.CanRetry, which "
        + "combines the node's classifier, retry limit, and circuit breaker, or defer to "
        + "base.DecideItemFailureAsync / base.DecideNodeFailureAsync. "
        + "https://docs.npipeline.net/analyzers/reliability#np9205-unconditional-retry.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterCompilationStartAction(start =>
        {
            var decisionType = start.Compilation.GetTypeByMetadataName($"{ReliabilityNamespace}.ResilienceDecision");
            var policyInterface = start.Compilation.GetTypeByMetadataName($"{ReliabilityNamespace}.IResiliencePolicy");

            if (decisionType is null || policyInterface is null)
                return;

            start.RegisterSyntaxNodeAction(
                nodeContext => AnalyzeMethod(nodeContext, decisionType, policyInterface),
                SyntaxKind.MethodDeclaration);
        });
    }

    private static void AnalyzeMethod(SyntaxNodeAnalysisContext context, INamedTypeSymbol decisionType, INamedTypeSymbol policyInterface)
    {
        var method = (MethodDeclarationSyntax)context.Node;

        if (method.Identifier.Text is not ("DecideItemFailureAsync" or "DecideNodeFailureAsync"))
            return;

        if (context.SemanticModel.GetDeclaredSymbol(method, context.CancellationToken) is not { } symbol
            || !ResiliencePolicySyntax.IsPolicyDecision(symbol, policyInterface))
            return;

        if (!ResiliencePolicySyntax.ReferencesDecision(method, context.SemanticModel, decisionType, "Retry", context.CancellationToken))
            return;

        var failureParameter = symbol.Parameters[0];

        if (ConsultsBudget(method, context.SemanticModel, failureParameter, context.CancellationToken))
            return;

        context.ReportDiagnostic(Diagnostic.Create(Rule, method.Identifier.GetLocation(), symbol.Name, failureParameter.Name));
    }

    /// <summary>
    ///     Whether the method reads a budget member of the failure parameter, or passes the failure on (to
    ///     <c>base</c> or a helper), which is assumed to consult it.
    /// </summary>
    private static bool ConsultsBudget(
        MethodDeclarationSyntax method,
        SemanticModel semanticModel,
        IParameterSymbol failureParameter,
        CancellationToken cancellationToken)
    {
        foreach (var identifier in method.DescendantNodes().OfType<IdentifierNameSyntax>())
        {
            if (identifier.Identifier.Text != failureParameter.Name)
                continue;

            if (!SymbolEqualityComparer.Default.Equals(semanticModel.GetSymbolInfo(identifier, cancellationToken).Symbol, failureParameter))
                continue;

            switch (identifier.Parent)
            {
                case MemberAccessExpressionSyntax member when member.Expression == identifier:
                    if (BudgetMembers.Contains(member.Name.Identifier.Text))
                        return true;

                    break;

                // failure passed as an argument, e.g. base.DecideItemFailureAsync(failure, ct) or ShouldRetry(failure).
                case ArgumentSyntax:
                    return true;

                // A property pattern such as `failure is { CanRetry: true }` or a switch on the failure.
                case IsPatternExpressionSyntax or SwitchExpressionSyntax or SwitchStatementSyntax:
                    return true;
            }
        }

        return false;
    }
}
