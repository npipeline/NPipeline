using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects problematic catch patterns that can swallow OperationCanceledException,
///     preventing proper cancellation propagation in pipelines.
///     Detects:
///     1. Broad catch of Exception that doesn't re-throw OperationCanceledException
///     2. Direct catch of OperationCanceledException without re-throw
///     3. Catch of AggregateException without checking inner exceptions for OperationCanceledException
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class OperationCanceledExceptionAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for swallowing OperationCanceledException.
    /// </summary>
    public const string SwallowingOperationCanceledExceptionId = "NP9102";

    private static readonly DiagnosticDescriptor SwallowingOperationCanceledExceptionRule = new(
        SwallowingOperationCanceledExceptionId,
        "Do not swallow OperationCanceledException",
        "Catch block '{0}' may swallow OperationCanceledException without re-throwing it. "
        + "This prevents proper cancellation propagation and can leave pipelines hanging. "
        + "Re-throw OperationCanceledException to maintain cancellation semantics.",
        "Reliability",
        DiagnosticSeverity.Warning,
        true,
        "OperationCanceledException should never be swallowed as it breaks cancellation chains. "
        + "Always re-throw OperationCanceledException to ensure graceful shutdown. "
        + "For broad catches, check exception type and re-throw if it's OperationCanceledException. "
        + "https://npipeline.dev/docs/core-concepts/cancellation/handling-cancellation.");

    /// <inheritdoc/>
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [SwallowingOperationCanceledExceptionRule];

    /// <inheritdoc/>
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze catch clauses
        context.RegisterSyntaxNodeAction(AnalyzeCatchClause, SyntaxKind.CatchClause);
    }

    private static void AnalyzeCatchClause(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not CatchClauseSyntax catchClause)
            return;

        // Get the exception type being caught
        var exceptionType = catchClause.Declaration?.Type;

        if (exceptionType == null)
            return;

        // Get the semantic model to resolve type information
        var semanticModel = context.SemanticModel;
        var exceptionTypeInfo = semanticModel.GetTypeInfo(exceptionType);
        var exceptionTypeSymbol = exceptionTypeInfo.Type;

        if (exceptionTypeSymbol == null)
            return;

        // Check if this catch block might swallow OperationCanceledException
        var walker = new CatchClauseWalker(exceptionTypeSymbol);
        walker.Visit(catchClause.Block);

        if (walker.MightSwallowOperationCanceledException)
        {
            var diagnostic = Diagnostic.Create(
                SwallowingOperationCanceledExceptionRule,
                catchClause.CatchKeyword.GetLocation(),
                GetCatchClauseDescription(exceptionTypeSymbol));

            context.ReportDiagnostic(diagnostic);
        }
    }

    /// <summary>
    ///     Gets a description of what the catch clause is catching.
    /// </summary>
    private static string GetCatchClauseDescription(ITypeSymbol exceptionType)
    {
        if (exceptionType.Name == "Exception" && exceptionType.ContainingNamespace?.Name == "System")
            return "catch (Exception)";

        if (exceptionType.Name == "AggregateException" && exceptionType.ContainingNamespace?.Name == "System")
            return "catch (AggregateException)";

        if (exceptionType.Name == "OperationCanceledException" && exceptionType.ContainingNamespace?.Name == "System")
            return "catch (OperationCanceledException)";

        return $"catch ({exceptionType.Name})";
    }

    /// <summary>
    ///     AST walker that analyzes catch blocks to determine if they might swallow OperationCanceledException.
    /// </summary>
    private sealed class CatchClauseWalker(ITypeSymbol caughtExceptionType) : CSharpSyntaxWalker
    {
        public bool MightSwallowOperationCanceledException { get; private set; } = true;

        public override void VisitThrowStatement(ThrowStatementSyntax node)
        {
            // If we find a throw statement, check if it might re-throw OperationCanceledException
            if (IsReThrowingOperationCanceledException(node))
                MightSwallowOperationCanceledException = false;

            base.VisitThrowStatement(node);
        }

        public override void VisitIfStatement(IfStatementSyntax node)
        {
            // Check if this if statement checks for OperationCanceledException and re-throws it
            if (IsConditionalReThrowOfOperationCanceledException(node))
                MightSwallowOperationCanceledException = false;

            base.VisitIfStatement(node);
        }

        /// <summary>
        ///     Determines if a throw statement re-throws OperationCanceledException.
        /// </summary>
        private bool IsReThrowingOperationCanceledException(ThrowStatementSyntax throwStatement)
        {
            // Simple re-throw: throw;
            if (throwStatement.Expression == null)
                return true;

            // Explicit re-throw of the caught exception: throw ex;
            if (throwStatement.Expression is IdentifierNameSyntax identifier)
            {
                var exceptionVariable = caughtExceptionType.Name == "Exception"
                    ? "ex"
                    : caughtExceptionType.Name == "AggregateException"
                        ? "aex"
                        : caughtExceptionType.Name == "OperationCanceledException"
                            ? "oex"
                            : null;

                return identifier.Identifier.Text == exceptionVariable;
            }

            return false;
        }

        /// <summary>
        ///     Determines if an if statement conditionally re-throws OperationCanceledException.
        /// </summary>
        private bool IsConditionalReThrowOfOperationCanceledException(IfStatementSyntax ifStatement)
        {
            // Check for pattern: if (ex is OperationCanceledException) throw;
            var condition = ifStatement.Condition;

            if (condition is IsPatternExpressionSyntax isPattern)
            {
                // Check if the pattern is checking for OperationCanceledException
                if (isPattern.Pattern is ConstantPatternSyntax constantPattern &&
                    constantPattern.Expression is TypeOfExpressionSyntax typeOfExpression &&
                    typeOfExpression.Type.ToString().Contains("OperationCanceledException"))
                {
                    // Check if the then block contains a throw
                    if (ifStatement.Statement is BlockSyntax block && block.Statements.Any(s => s is ThrowStatementSyntax))
                        return true;
                }
            }

            // Check for pattern: if (ex is OperationCanceledException oce) throw oce;
            if (condition is IsPatternExpressionSyntax isPatternWithDeclaration &&
                isPatternWithDeclaration.Pattern is DeclarationPatternSyntax declarationPattern &&
                declarationPattern.Type.ToString().Contains("OperationCanceledException"))
            {
                // Check if the then block contains a throw
                if (ifStatement.Statement is BlockSyntax block && block.Statements.Any(s => s is ThrowStatementSyntax))
                    return true;
            }

            // Check for pattern: if (ex.GetType() == typeof(OperationCanceledException)) throw;
            if (condition is BinaryExpressionSyntax binaryExpression &&
                binaryExpression.Kind() == SyntaxKind.EqualsExpression &&
                IsOperationCanceledExceptionTypeCheck(binaryExpression.Left) &&
                IsOperationCanceledExceptionTypeCheck(binaryExpression.Right))
            {
                // Check if the then block contains a throw
                if (ifStatement.Statement is BlockSyntax block && block.Statements.Any(s => s is ThrowStatementSyntax))
                    return true;
            }

            return false;
        }

        /// <summary>
        ///     Checks if an expression is a type check for OperationCanceledException.
        /// </summary>
        private bool IsOperationCanceledExceptionTypeCheck(ExpressionSyntax expression)
        {
            // Check for typeof(OperationCanceledException)
            if (expression is TypeOfExpressionSyntax typeOfExpression)
                return typeOfExpression.Type.ToString().Contains("OperationCanceledException");

            // Check for GetType() calls
            if (expression is InvocationExpressionSyntax invocation)
            {
                if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
                    return memberAccess.Name.Identifier.Text == "GetType";
            }

            return false;
        }
    }
}
