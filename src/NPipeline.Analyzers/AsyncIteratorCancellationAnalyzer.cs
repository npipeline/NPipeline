using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Detects an async iterator in a node that has no <c>CancellationToken</c> parameter, so it cannot observe the
///     token its consumer passes to <c>GetAsyncEnumerator</c>.
/// </summary>
/// <remarks>
///     The runtime stops a stream by cancelling its enumerator: when one input of a merge fails, when a consumer leaves
///     early, when a fan-out branch is released, or when the run is cancelled. Disposal then waits for the iterator to
///     notice. An iterator without a token only stops at its next item, which for a quiet source may be never. An
///     iterator that has a token but lacks <c>[EnumeratorCancellation]</c> is reported by NP9203.
/// </remarks>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class AsyncIteratorCancellationAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for an async iterator in a node that cannot observe cancellation.
    /// </summary>
    public const string AsyncIteratorWithoutCancellationId = "NP9206";

    private static readonly DiagnosticDescriptor Rule = new(
        AsyncIteratorWithoutCancellationId,
        "Async iterator in a node cannot observe cancellation",
        "Async iterator '{0}' has no CancellationToken parameter, so stopping its stream waits for its next item; add "
        + "'[EnumeratorCancellation] CancellationToken cancellationToken = default' and observe it",
        "Reliability & Error Handling",
        DiagnosticSeverity.Warning,
        true,
        "The pipeline stops a stream by cancelling its enumerator, then waits for the iterator to stop. Give every async "
        + "iterator in a node a CancellationToken parameter marked [EnumeratorCancellation], and pass it to the awaits "
        + "inside. https://docs.npipeline.net/analyzers/reliability#np9206-async-iterator-without-cancellation.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterCompilationStartAction(start =>
        {
            var nodeInterface = start.Compilation.GetTypeByMetadataName("NPipeline.Nodes.INode");
            var asyncEnumerable = start.Compilation.GetTypeByMetadataName("System.Collections.Generic.IAsyncEnumerable`1");
            var cancellationToken = start.Compilation.GetTypeByMetadataName("System.Threading.CancellationToken");

            if (nodeInterface is null || asyncEnumerable is null || cancellationToken is null)
                return;

            start.RegisterSyntaxNodeAction(
                ctx => Analyze(ctx, nodeInterface, asyncEnumerable, cancellationToken),
                SyntaxKind.MethodDeclaration,
                SyntaxKind.LocalFunctionStatement);
        });
    }

    private static void Analyze(
        SyntaxNodeAnalysisContext context,
        INamedTypeSymbol nodeInterface,
        INamedTypeSymbol asyncEnumerable,
        INamedTypeSymbol cancellationToken)
    {
        var (modifiers, identifier, body) = context.Node switch
        {
            MethodDeclarationSyntax method => (method.Modifiers, method.Identifier, (SyntaxNode?)method.Body),
            LocalFunctionStatementSyntax local => (local.Modifiers, local.Identifier, local.Body),
            _ => (default, default, null),
        };

        if (body is null || !modifiers.Any(SyntaxKind.AsyncKeyword))
            return;

        if (context.SemanticModel.GetDeclaredSymbol(context.Node, context.CancellationToken) is not IMethodSymbol symbol)
            return;

        if (symbol.ReturnType is not INamedTypeSymbol returnType ||
            !SymbolEqualityComparer.Default.Equals(returnType.OriginalDefinition, asyncEnumerable))
            return;

        if (symbol.Parameters.Any(p => SymbolEqualityComparer.Default.Equals(p.Type, cancellationToken)))
            return;

        if (!ContainsOwnYield(body, context.Node))
            return;

        var containingType = symbol.ContainingType;

        if (containingType is null || !containingType.AllInterfaces.Contains(nodeInterface, SymbolEqualityComparer.Default))
            return;

        context.ReportDiagnostic(Diagnostic.Create(Rule, identifier.GetLocation(), symbol.Name));
    }

    /// <summary>
    ///     Whether the body yields for this function itself, rather than only inside a nested local function or lambda.
    /// </summary>
    private static bool ContainsOwnYield(SyntaxNode body, SyntaxNode function)
    {
        foreach (var yield in body.DescendantNodes().OfType<YieldStatementSyntax>())
        {
            var owner = yield.Ancestors().FirstOrDefault(a => a is LocalFunctionStatementSyntax or MethodDeclarationSyntax or AnonymousFunctionExpressionSyntax);

            if (owner == function)
                return true;
        }

        return false;
    }
}
