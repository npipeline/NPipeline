using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects TransformNode implementations that could benefit from ValueTask optimization.
///     This analyzer identifies classes that:
///     1. Inherit from TransformNode&lt;TIn, TOut&gt;
///     2. Only override ExecuteAsync method
///     3. Use Task.FromResult() to wrap synchronous operations
///     4. Don't override ExecuteValueTaskAsync method
///     Such implementations can be optimized by overriding ExecuteValueTaskAsync to avoid unnecessary Task allocations.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class ValueTaskOptimizationAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for missing ValueTask optimization.
    /// </summary>
    public const string MissingValueTaskOptimizationId = "NP9106";

    private static readonly DiagnosticDescriptor MissingValueTaskOptimizationRule = new(
        MissingValueTaskOptimizationId,
        "Consider overriding ExecuteValueTaskAsync for synchronous operations",
        "TransformNode '{0}' uses Task.FromResult in ExecuteAsync but doesn't override ExecuteValueTaskAsync. "
        + "Overriding ExecuteValueTaskAsync can improve performance by avoiding Task allocations for synchronous operations.",
        "Performance & Optimization",
        DiagnosticSeverity.Info,
        true,
        "TransformNode implementations that use Task.FromResult for synchronous operations can benefit from "
        + "overriding ExecuteValueTaskAsync to return ValueTask.FromResult directly. This avoids unnecessary Task allocations "
        + "and improves performance, especially in high-throughput scenarios. "
        + "https://npipeline.dev/docs/core-concepts/nodes/valuetask-transforms/.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [MissingValueTaskOptimizationRule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze class declarations that inherit from TransformNode
        context.RegisterSymbolAction(AnalyzeTransformNode, SymbolKind.NamedType);
    }

    private static void AnalyzeTransformNode(SymbolAnalysisContext context)
    {
        if (context.Symbol is not INamedTypeSymbol typeSymbol)
            return;

        // Check if this type inherits from TransformNode<TIn, TOut>
        if (!InheritsFromTransformNode(typeSymbol))
            return;

        // Check if ExecuteAsync is overridden
        var executeAsyncMethod = FindExecuteAsyncOverride(typeSymbol);

        if (executeAsyncMethod == null)
            return;

        // Check if ExecuteValueTaskAsync is already overridden
        if (FindExecuteValueTaskAsyncOverride(typeSymbol) != null)
            return;

        // Get syntax reference to analyze method body
        var syntaxRef = executeAsyncMethod.DeclaringSyntaxReferences.FirstOrDefault();

        if (syntaxRef == null)
            return;

        if (syntaxRef.GetSyntax(context.CancellationToken) is not MethodDeclarationSyntax methodNode)
            return;

        // Check if method uses Task.FromResult
        var walker = new TaskFromResultWalker();
        walker.Visit(methodNode);

        if (walker.UsesTaskFromResult)
        {
            // Report diagnostic for missing ValueTask optimization
            var diagnostic = Diagnostic.Create(
                MissingValueTaskOptimizationRule,
                typeSymbol.Locations[0],
                typeSymbol.Name);

            context.ReportDiagnostic(diagnostic);
        }
    }

    /// <summary>
    ///     Checks if a type inherits from TransformNode&lt;TIn, TOut&gt;.
    /// </summary>
    private static bool InheritsFromTransformNode(INamedTypeSymbol typeSymbol)
    {
        var currentType = typeSymbol.BaseType;

        while (currentType != null)
        {
            // Check for both "TransformNode" and generic TransformNode
            if (currentType.Name == "TransformNode" &&
                currentType.ContainingNamespace?.Name == "Nodes" &&
                currentType.ContainingNamespace?.ContainingNamespace?.Name == "NPipeline")
                return true;

            currentType = currentType.BaseType;
        }

        return false;
    }

    /// <summary>
    ///     Finds the ExecuteAsync method override in the type.
    /// </summary>
    private static IMethodSymbol? FindExecuteAsyncOverride(INamedTypeSymbol typeSymbol)
    {
        return typeSymbol.GetMembers("ExecuteAsync")
            .OfType<IMethodSymbol>()
            .FirstOrDefault(m => m.IsOverride && m.Parameters.Length == 3);
    }

    /// <summary>
    ///     Finds the ExecuteValueTaskAsync method override in the type.
    /// </summary>
    private static IMethodSymbol? FindExecuteValueTaskAsyncOverride(INamedTypeSymbol typeSymbol)
    {
        return typeSymbol.GetMembers("ExecuteValueTaskAsync")
            .OfType<IMethodSymbol>()
            .FirstOrDefault(m => m.IsOverride && m.Parameters.Length == 3);
    }

    /// <summary>
    ///     AST walker that detects if a method uses Task.FromResult.
    /// </summary>
    private sealed class TaskFromResultWalker : CSharpSyntaxWalker
    {
        public bool UsesTaskFromResult { get; private set; }

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (UsesTaskFromResult)
            {
                base.VisitInvocationExpression(node);
                return;
            }

            if (node.Expression is MemberAccessExpressionSyntax memberAccess)
            {
                var methodName = memberAccess.Name.Identifier.Text;

                // Check for Task.FromResult or Task<T>.FromResult
                if (methodName == "FromResult" &&
                    (memberAccess.Expression is IdentifierNameSyntax { Identifier.Text: "Task" } ||
                     memberAccess.Expression is GenericNameSyntax { Identifier.Text: "Task" }))
                    UsesTaskFromResult = true;
            }

            base.VisitInvocationExpression(node);
        }
    }
}
