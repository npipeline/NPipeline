using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects when PipelineErrorDecision.RestartNode is returned but the
///     three mandatory prerequisites are missing:
///     1. ResilientExecutionStrategy must be applied to the node
///     2. MaxNodeRestartAttempts must be set to a value > 0
///     3. MaxMaterializedItems must be set to a non-null value
///     Missing any of these prerequisites will silently disable node restart functionality,
///     causing the pipeline to fail entirely instead of recovering gracefully.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class ResilientExecutionConfigurationAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for when RestartNode can be returned without proper configuration.
    /// </summary>
    public const string IncompleteResilientConfigurationId = "NP9001";

    private static readonly DiagnosticDescriptor IncompleteResilientConfigurationRule = new(
        IncompleteResilientConfigurationId,
        "RestartNode decision requires complete resilience configuration",
        "Error handler can return PipelineErrorDecision.RestartNode but may not have all three mandatory prerequisites configured. "
        + "Missing prerequisites will silently disable restart, causing the entire pipeline to fail instead of recovering the failed node. "
        + "Checklist: (1) Node wrapped with ResilientExecutionStrategy, (2) MaxNodeRestartAttempts > 0, (3) MaxMaterializedItems is not null.",
        "Resilience",
        DiagnosticSeverity.Warning,
        true,
        "See node restart quick start guide for complete configuration requirements. "
        + "Each prerequisite is mandatory - skipping even one will silently disable restart: "
        + "https://npipeline.dev/docs/core-concepts/resilience/getting-started.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        ImmutableArray.Create(IncompleteResilientConfigurationRule);

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze method declarations that implement IPipelineErrorHandler.HandleNodeFailureAsync
        context.RegisterSymbolAction(AnalyzeErrorHandler, SymbolKind.Method);
    }

    private static void AnalyzeErrorHandler(SymbolAnalysisContext context)
    {
        if (context.Symbol is not IMethodSymbol methodSymbol)
            return;

        // Check if this is the HandleNodeFailureAsync method
        if (methodSymbol.Name != "HandleNodeFailureAsync" || methodSymbol.Parameters.Length < 4)
            return;

        // Verify it's implementing IPipelineErrorHandler.HandleNodeFailureAsync
        var containingType = methodSymbol.ContainingType;

        if (containingType == null)
            return;

        var implementsErrorHandler = containingType.AllInterfaces
            .Any(i => i.Name == "IPipelineErrorHandler" && i.ContainingNamespace?.Name == "ErrorHandling");

        if (!implementsErrorHandler)
            return;

        // Get the syntax reference to analyze the method body
        var syntaxRef = methodSymbol.DeclaringSyntaxReferences.FirstOrDefault();

        if (syntaxRef == null)
            return;

        var methodNode = syntaxRef.GetSyntax(context.CancellationToken) as MethodDeclarationSyntax;

        if (methodNode == null)
            return;

        // Check if the method can return PipelineErrorDecision.RestartNode
        var canReturnRestartNode = CanReturnRestartNode(methodNode);

        if (!canReturnRestartNode)
            return;

        // Emit a warning about incomplete resilience configuration
        var diagnostic = Diagnostic.Create(
            IncompleteResilientConfigurationRule,
            methodNode.Identifier.GetLocation());

        context.ReportDiagnostic(diagnostic);
    }

    /// <summary>
    ///     Checks if a method can return PipelineErrorDecision.RestartNode.
    /// </summary>
    private static bool CanReturnRestartNode(MethodDeclarationSyntax methodNode)
    {
        // Look for direct return statements, implicit returns, and switch expressions
        var walker = new RestartNodeReturnWalker();
        walker.Visit(methodNode);
        return walker.CanReturnRestartNode;
    }

    /// <summary>
    ///     AST walker that detects if a method can return PipelineErrorDecision.RestartNode.
    /// </summary>
    private sealed class RestartNodeReturnWalker : CSharpSyntaxWalker
    {
        public bool CanReturnRestartNode { get; private set; }

        public override void VisitReturnStatement(ReturnStatementSyntax node)
        {
            if (CanReturnRestartNode)
            {
                base.VisitReturnStatement(node);
                return;
            }

            if (node.Expression != null && IsRestartNodeReference(node.Expression))
                CanReturnRestartNode = true;

            base.VisitReturnStatement(node);
        }

        public override void VisitSwitchExpression(SwitchExpressionSyntax node)
        {
            // Check switch arm expressions
            if (!CanReturnRestartNode)
            {
                foreach (var arm in node.Arms)
                {
                    if (IsRestartNodeReference(arm.Expression))
                    {
                        CanReturnRestartNode = true;
                        break;
                    }
                }
            }

            base.VisitSwitchExpression(node);
        }

        public override void VisitConditionalExpression(ConditionalExpressionSyntax node)
        {
            // Check both branches of ternary operator
            if (!CanReturnRestartNode &&
                (IsRestartNodeReference(node.WhenTrue) || IsRestartNodeReference(node.WhenFalse)))
                CanReturnRestartNode = true;

            base.VisitConditionalExpression(node);
        }

        /// <summary>
        ///     Checks if an expression evaluates to or returns PipelineErrorDecision.RestartNode.
        /// </summary>
        private static bool IsRestartNodeReference(ExpressionSyntax? expression)
        {
            if (expression == null)
                return false;

            // Direct member access: PipelineErrorDecision.RestartNode
            if (expression is MemberAccessExpressionSyntax memberAccess)
            {
                return memberAccess.Name.Identifier.Text == "RestartNode" &&
                       memberAccess.Expression is IdentifierNameSyntax { Identifier.Text: "PipelineErrorDecision" };
            }

            // Simple identifier (when using static import): RestartNode
            if (expression is IdentifierNameSyntax identifier &&
                identifier.Identifier.Text == "RestartNode")
                return true;

            // Unwrap await expressions
            if (expression is AwaitExpressionSyntax awaitExpr)
                return IsRestartNodeReference(awaitExpr.Expression);

            // Unwrap cast expressions
            if (expression is CastExpressionSyntax castExpr)
                return IsRestartNodeReference(castExpr.Expression);

            // Unwrap parenthesized expressions
            if (expression is ParenthesizedExpressionSyntax parenExpr)
                return IsRestartNodeReference(parenExpr.Expression);

            return false;
        }
    }
}
