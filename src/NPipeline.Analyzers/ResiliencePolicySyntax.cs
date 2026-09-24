using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace NPipeline.Analyzers;

/// <summary>
///     Helpers shared by the analyzers that inspect <c>IResiliencePolicy</c> implementations.
/// </summary>
internal static class ResiliencePolicySyntax
{
    private static readonly string[] DecisionMethodNames = ["DecideItemFailureAsync", "DecideRestartAsync", "DecideNodeFailureAsync"];

    /// <summary>
    ///     Whether <paramref name="method" /> is one of the three decisions of a type that implements
    ///     <c>IResiliencePolicy</c>, directly or through <c>ResiliencePolicyBase</c>.
    /// </summary>
    public static bool IsPolicyDecision(IMethodSymbol method, INamedTypeSymbol policyInterface) =>
        Array.IndexOf(DecisionMethodNames, method.Name) >= 0
        && method.Parameters.Length == 2
        && method.ContainingType is { } type
        && type.AllInterfaces.Contains(policyInterface, SymbolEqualityComparer.Default);

    /// <summary>
    ///     Whether the body of <paramref name="method" /> mentions <c>ResilienceDecision.{member}</c>, qualified or
    ///     through <c>using static</c>.
    /// </summary>
    public static bool ReferencesDecision(
        MethodDeclarationSyntax method,
        SemanticModel semanticModel,
        INamedTypeSymbol decisionType,
        string member,
        CancellationToken cancellationToken)
    {
        var body = (SyntaxNode?)method.Body ?? method.ExpressionBody;

        if (body is null)
            return false;

        foreach (var identifier in body.DescendantNodes().OfType<IdentifierNameSyntax>())
        {
            if (identifier.Identifier.Text != member)
                continue;

            if (semanticModel.GetSymbolInfo(identifier, cancellationToken).Symbol is IFieldSymbol field
                && SymbolEqualityComparer.Default.Equals(field.ContainingType, decisionType))
                return true;
        }

        return false;
    }
}
