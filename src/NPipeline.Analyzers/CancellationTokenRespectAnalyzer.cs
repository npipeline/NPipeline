using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects when methods don't properly respect cancellation tokens.
///     This analyzer checks for:
///     1. Methods with CancellationToken parameter that don't pass it to async calls
///     2. Long loops without ThrowIfCancellationRequested() checks
///     3. Async iterators without [EnumeratorCancellation] attribute
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class CancellationTokenRespectAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for when cancellation tokens are not properly respected.
    /// </summary>
    public const string CancellationTokenNotRespectedId = "NP9104";

    private static readonly DiagnosticDescriptor CancellationTokenNotRespectedRule = new(
        CancellationTokenNotRespectedId,
        "Method should respect cancellation token",
        "Method '{0}' has a CancellationToken parameter but doesn't properly respect it. Consider passing the token to async operations and checking it periodically in long-running operations.",
        "Resilience",
        DiagnosticSeverity.Warning,
        true,
        "Cancellation tokens should be passed to async operations and checked periodically to ensure responsive cancellation. This helps prevent hanging operations and improves application responsiveness. https://npipeline.dev/docs/core-concepts/cancellation/best-practices.");

    /// <inheritdoc/>
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [CancellationTokenNotRespectedRule];

    /// <inheritdoc/>
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze method bodies for cancellation token usage
        context.RegisterSyntaxNodeAction(AnalyzeMethod, SyntaxKind.MethodDeclaration);
    }

    private static void AnalyzeMethod(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not MethodDeclarationSyntax methodDeclaration)
            return;

        // Check if method has a CancellationToken parameter
        var cancellationTokenParam = FindCancellationTokenParameter(methodDeclaration, context.SemanticModel);

        if (cancellationTokenParam == null)
            return;

        // Check if this is an async iterator method with [EnumeratorCancellation] attribute
        var returnType = context.SemanticModel.GetTypeInfo(methodDeclaration.ReturnType!).Type;

        if (returnType?.Name == "IAsyncEnumerable")
        {
            // Check if CancellationToken parameter has [EnumeratorCancellation] attribute
            var hasEnumeratorCancellation = false;

            foreach (var attributeList in cancellationTokenParam.AttributeLists)
            {
                foreach (var attribute in attributeList.Attributes)
                {
                    var attributeName = attribute.Name?.ToString();

                    if (attributeName == "EnumeratorCancellation" || attributeName == "EnumeratorCancellationAttribute")
                    {
                        hasEnumeratorCancellation = true;
                        break;
                    }
                }

                if (hasEnumeratorCancellation)
                    break;
            }

            // If it has [EnumeratorCancellation], skip analysis
            if (hasEnumeratorCancellation)
                return;
        }

        // Analyze method body for cancellation token usage
        var walker = new CancellationTokenUsageWalker(cancellationTokenParam, context.SemanticModel);
        walker.Visit(methodDeclaration.Body);

        // Only report a diagnostic if there are actual issues found
        if (walker.Issues.Count > 0)
        {
            var diagnostic = Diagnostic.Create(
                CancellationTokenNotRespectedRule,
                methodDeclaration.Identifier.GetLocation(),
                methodDeclaration.Identifier.Text,
                string.Join(" ", walker.Issues.Select(i => i.Message)));

            context.ReportDiagnostic(diagnostic);
        }
    }

    /// <summary>
    ///     Finds the CancellationToken parameter in a method declaration.
    /// </summary>
    private static ParameterSyntax? FindCancellationTokenParameter(MethodDeclarationSyntax method, SemanticModel semanticModel)
    {
        foreach (var param in method.ParameterList.Parameters)
        {
            var paramType = semanticModel.GetTypeInfo(param.Type!).Type;

            if (paramType?.Name == "CancellationToken" &&
                (paramType.ContainingNamespace?.Name == "Threading" ||
                 paramType.ToDisplayString().Contains("CancellationToken")))
                return param;
        }

        return null;
    }

    /// <summary>
    ///     AST walker that detects improper cancellation token usage.
    /// </summary>
    private sealed class CancellationTokenUsageWalker(ParameterSyntax cancellationTokenParam, SemanticModel semanticModel) : CSharpSyntaxWalker
    {
        private readonly string _cancellationTokenName = cancellationTokenParam.Identifier.Text;
        private readonly List<(Location Location, string Message)> _issues = [];

        public IReadOnlyList<(Location Location, string Message)> Issues => _issues;

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            CheckForAsyncCallWithoutCancellationToken(node);
            base.VisitInvocationExpression(node);
        }

        public override void VisitForStatement(ForStatementSyntax node)
        {
            CheckLoopForCancellationCheck(node);
            base.VisitForStatement(node);
        }

        public override void VisitWhileStatement(WhileStatementSyntax node)
        {
            CheckLoopForCancellationCheck(node);
            base.VisitWhileStatement(node);
        }

        public override void VisitDoStatement(DoStatementSyntax node)
        {
            CheckLoopForCancellationCheck(node);
            base.VisitDoStatement(node);
        }

        public override void VisitForEachStatement(ForEachStatementSyntax node)
        {
            CheckLoopForCancellationCheck(node);
            base.VisitForEachStatement(node);
        }

        public override void VisitMethodDeclaration(MethodDeclarationSyntax node)
        {
            // Check for async iterator methods without [EnumeratorCancellation] attribute
            CheckAsyncIteratorMethod(node);
            base.VisitMethodDeclaration(node);
        }

        /// <summary>
        ///     Checks if async calls are properly passing the cancellation token.
        /// </summary>
        private void CheckForAsyncCallWithoutCancellationToken(InvocationExpressionSyntax invocation)
        {
            if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                return;

            var methodName = memberAccess.Name.Identifier.Text;

            // Check for common async methods that should accept cancellation token
            if (IsAsyncMethodRequiringCancellationToken(methodName))
            {
                // Check if cancellation token is passed
                var hasCancellationToken = HasCancellationTokenArgument(invocation);

                if (!hasCancellationToken)
                {
                    _issues.Add((invocation.GetLocation(),
                        $"Async call '{methodName}' should pass the cancellation token for responsive cancellation."));
                }
            }
        }

        /// <summary>
        ///     Checks if loops have proper cancellation checks.
        /// </summary>
        private void CheckLoopForCancellationCheck(StatementSyntax loop)
        {
            // Get the loop body based on loop type
            var loopBody = loop switch
            {
                ForStatementSyntax forStatement => forStatement.Statement,
                WhileStatementSyntax whileStatement => whileStatement.Statement,
                DoStatementSyntax doStatement => doStatement.Statement,
                ForEachStatementSyntax forEachStatement => forEachStatement.Statement,
                _ => null,
            };

            if (loopBody == null)
                return;

            // Look for ThrowIfCancellationRequested() in loop body
            var loopWalker = new ThrowIfCancellationRequestedWalker(_cancellationTokenName);
            loopWalker.Visit(loopBody);

            if (!loopWalker.HasThrowIfCancellationRequested)
            {
                _issues.Add((loop.GetLocation(),
                    "Loop should check for cancellation using cancellationToken.ThrowIfCancellationRequested() to avoid hanging."));
            }
        }

        /// <summary>
        ///     Checks if async iterator methods have [EnumeratorCancellation] attribute.
        /// </summary>
        private void CheckAsyncIteratorMethod(MethodDeclarationSyntax method)
        {
            // Check if this is an async iterator method (returns IAsyncEnumerable<T>)
            var returnType = semanticModel.GetTypeInfo(method.ReturnType!).Type;

            if (returnType?.Name != "IAsyncEnumerable")
                return;

            // Check if method has a CancellationToken parameter
            var hasCancellationTokenParam = method.ParameterList.Parameters
                .Any(p => semanticModel.GetTypeInfo(p.Type!).Type?.Name == "CancellationToken");

            if (!hasCancellationTokenParam)
            {
                _issues.Add((method.Identifier.GetLocation(),
                    "Async iterator methods should accept a CancellationToken parameter with [EnumeratorCancellation] attribute."));

                return;
            }

            // Check if CancellationToken parameter has [EnumeratorCancellation] attribute
            var cancellationTokenParam = method.ParameterList.Parameters
                .FirstOrDefault(p => semanticModel.GetTypeInfo(p.Type!).Type?.Name == "CancellationToken");

            if (cancellationTokenParam != null)
            {
                // Check for [EnumeratorCancellation] attribute in parameter's attribute lists
                var hasEnumeratorCancellation = false;

                foreach (var attributeList in cancellationTokenParam.AttributeLists)
                {
                    foreach (var attribute in attributeList.Attributes)
                    {
                        var attributeName = attribute.Name?.ToString();

                        if (attributeName == "EnumeratorCancellation" || attributeName == "EnumeratorCancellationAttribute")
                        {
                            hasEnumeratorCancellation = true;
                            break;
                        }
                    }

                    if (hasEnumeratorCancellation)
                        break;
                }

                if (!hasEnumeratorCancellation)
                {
                    _issues.Add((cancellationTokenParam.GetLocation(),
                        "CancellationToken parameter in async iterator should have [EnumeratorCancellation] attribute."));
                }
            }
        }

        /// <summary>
        ///     Determines if an async method typically requires a cancellation token.
        /// </summary>
        private static bool IsAsyncMethodRequiringCancellationToken(string methodName)
        {
            return methodName switch
            {
                "Delay" or "ReadAsync" or "WriteAsync" or "FlushAsync" or
                    "GetAsync" or "PostAsync" or "PutAsync" or "DeleteAsync" or "SendAsync" or
                    "ReadAsStringAsync" or "ReadAsStreamAsync" or "ReadAsByteArrayAsync" or
                    "GetStringAsync" or "GetByteArrayAsync" or "GetStreamAsync" => true,
                _ => false,
            };
        }

        /// <summary>
        ///     Checks if an invocation passes the cancellation token as an argument.
        /// </summary>
        private bool HasCancellationTokenArgument(InvocationExpressionSyntax invocation)
        {
            if (invocation.ArgumentList == null)
                return false;

            foreach (var arg in invocation.ArgumentList.Arguments)
            {
                if (arg.Expression is IdentifierNameSyntax identifier &&
                    identifier.Identifier.Text == _cancellationTokenName)
                    return true;
            }

            return false;
        }
    }

    /// <summary>
    ///     AST walker that detects ThrowIfCancellationRequested() calls.
    /// </summary>
    private sealed class ThrowIfCancellationRequestedWalker(string cancellationTokenName) : CSharpSyntaxWalker
    {
        public bool HasThrowIfCancellationRequested { get; private set; }

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (node.Expression is MemberAccessExpressionSyntax memberAccess &&
                memberAccess.Name.Identifier.Text == "ThrowIfCancellationRequested")
            {
                // Check if this is called on specific CancellationToken parameter
                if (memberAccess.Expression is IdentifierNameSyntax identifier &&
                    identifier.Identifier.Text == cancellationTokenName)
                    HasThrowIfCancellationRequested = true;
            }

            base.VisitInvocationExpression(node);
        }
    }
}
