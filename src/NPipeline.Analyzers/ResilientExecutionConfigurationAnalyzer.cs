using System.Collections.Concurrent;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Detects <c>ResilienceDecision.RestartNode</c> used where it cannot restart anything:
///     <list type="bullet">
///         <item>
///             returned from <c>DecideItemFailureAsync</c> (the runtime throws) or <c>DecideNodeFailureAsync</c> (the
///             runtime treats it as <c>Fail</c>);
///         </item>
///         <item>
///             returned from <c>DecideRestartAsync</c> in a compilation that never sets
///             <c>NodeRestartOptions.MaxRestarts</c>. The restart decision is only consulted for nodes whose
///             <c>NodeRestart.MaxRestarts</c> is greater than zero, so without it the override never runs.
///         </item>
///     </list>
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class ResilientExecutionConfigurationAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for a RestartNode decision that cannot restart a node.
    /// </summary>
    public const string IncompleteResilientConfigurationId = "NP9001";

    private const string ReliabilityNamespace = "NPipeline.Reliability";

    private static readonly DiagnosticDescriptor Rule = new(
        IncompleteResilientConfigurationId,
        "RestartNode decision requires NodeRestart",
        "{0}",
        "Configuration & Setup",
        DiagnosticSeverity.Warning,
        true,
        "Only DecideRestartAsync can restart a node, and the runtime consults it only for transform nodes whose "
        + "NodeRestart.MaxRestarts is greater than zero. Enable restart with "
        + "builder.WithResilience(handle, o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 3 } }). "
        + "https://docs.npipeline.net/analyzers/configuration#np9001-restartnode-requires-noderestart.");

    // Reported at compilation end, once every MaxRestarts assignment has been seen.
    private static readonly DiagnosticDescriptor RestartNeverConsultedRule = new(
        IncompleteResilientConfigurationId,
        Rule.Title,
        Rule.MessageFormat,
        Rule.Category,
        Rule.DefaultSeverity,
        true,
        Rule.Description,
        customTags: WellKnownDiagnosticTags.CompilationEnd);

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule, RestartNeverConsultedRule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterCompilationStartAction(start =>
        {
            var decisionType = start.Compilation.GetTypeByMetadataName($"{ReliabilityNamespace}.ResilienceDecision");
            var policyInterface = start.Compilation.GetTypeByMetadataName($"{ReliabilityNamespace}.IResiliencePolicy");
            var restartOptionsType = start.Compilation.GetTypeByMetadataName($"{ReliabilityNamespace}.NodeRestartOptions");

            if (decisionType is null || policyInterface is null)
                return;

            var restartOverrides = new ConcurrentBag<Location>();
            var setsMaxRestarts = 0;

            start.RegisterSyntaxNodeAction(nodeContext =>
            {
                var method = (MethodDeclarationSyntax)nodeContext.Node;

                if (nodeContext.SemanticModel.GetDeclaredSymbol(method, nodeContext.CancellationToken) is not { } symbol
                    || !ResiliencePolicySyntax.IsPolicyDecision(symbol, policyInterface))
                    return;

                if (!ResiliencePolicySyntax.ReferencesDecision(method, nodeContext.SemanticModel, decisionType, "RestartNode",
                        nodeContext.CancellationToken))
                    return;

                switch (symbol.Name)
                {
                    case "DecideItemFailureAsync":
                        nodeContext.ReportDiagnostic(Diagnostic.Create(Rule, method.Identifier.GetLocation(),
                            "DecideItemFailureAsync returns ResilienceDecision.RestartNode, which the item layer does not support "
                            + "and fails the node. Return Retry, Skip, DeadLetter, or Fail; restarts are decided in DecideRestartAsync."));

                        break;
                    case "DecideNodeFailureAsync":
                        nodeContext.ReportDiagnostic(Diagnostic.Create(Rule, method.Identifier.GetLocation(),
                            "DecideNodeFailureAsync returns ResilienceDecision.RestartNode, which the node-retry layer treats as Fail. "
                            + "Return Retry or Fail; restarts are decided in DecideRestartAsync."));

                        break;
                    case "DecideRestartAsync":
                        restartOverrides.Add(method.Identifier.GetLocation());
                        break;
                }
            }, SyntaxKind.MethodDeclaration);

            if (restartOptionsType is not null)
            {
                start.RegisterSyntaxNodeAction(nodeContext =>
                {
                    if (Volatile.Read(ref setsMaxRestarts) != 0)
                        return;

                    var name = nodeContext.Node switch
                    {
                        AssignmentExpressionSyntax { Left: IdentifierNameSyntax identifier } => identifier,
                        AssignmentExpressionSyntax { Left: MemberAccessExpressionSyntax member } => member.Name,
                        _ => null,
                    };

                    if (name?.Identifier.Text != "MaxRestarts")
                        return;

                    var property = nodeContext.SemanticModel.GetSymbolInfo(name, nodeContext.CancellationToken).Symbol;

                    if (SymbolEqualityComparer.Default.Equals(property?.ContainingType, restartOptionsType))
                        Interlocked.Exchange(ref setsMaxRestarts, 1);
                }, SyntaxKind.SimpleAssignmentExpression);
            }

            start.RegisterCompilationEndAction(end =>
            {
                if (Volatile.Read(ref setsMaxRestarts) != 0)
                    return;

                foreach (var location in restartOverrides)
                {
                    end.ReportDiagnostic(Diagnostic.Create(RestartNeverConsultedRule, location,
                        "DecideRestartAsync can return ResilienceDecision.RestartNode, but nothing in this project sets "
                        + "NodeRestartOptions.MaxRestarts, so the decision is never consulted and a failed stream fails the node. "
                        + "Set NodeRestart.MaxRestarts > 0 for the transform nodes that should restart."));
                }
            });
        });
    }
}
