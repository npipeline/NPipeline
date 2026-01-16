using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects synchronous over async anti-patterns that can lead to deadlocks
///     and performance issues. These patterns should be avoided:
///     1. Synchronous methods calling async methods without await
///     2. Async methods that don't await async calls
///     3. Task-returning methods that block on async operations
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SynchronousOverAsyncAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for synchronous over async anti-patterns.
    /// </summary>
    public const string SynchronousOverAsyncId = "NP9102";

    private static readonly DiagnosticDescriptor SynchronousOverAsyncRule = new(
        SynchronousOverAsyncId,
        "Avoid synchronous over async anti-patterns",
        "Method '{0}' contains synchronous over async anti-pattern '{1}' that can cause deadlocks and reduce performance. "
        + "Use proper async patterns instead to maintain the async chain and prevent thread pool starvation.",
        "Performance & Optimization",
        DiagnosticSeverity.Warning,
        true,
        "Synchronous over async patterns can cause deadlocks and performance issues. "
        + "Use proper async patterns: await async methods instead of using .Result/.Wait(), "
        + "avoid Task.Run() to wrap synchronous operations, and don't block in Task-returning methods. "
        + "https://npipeline.dev/docs/async-programming/avoiding-sync-over-async-patterns.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [SynchronousOverAsyncRule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze method bodies for synchronous over async patterns
        context.RegisterSyntaxNodeAction(AnalyzeMethod, SyntaxKind.MethodDeclaration);
    }

    private static void AnalyzeMethod(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not MethodDeclarationSyntax methodDeclaration)
            return;

        // Determine if this method is async or synchronous
        var isAsyncMethod = IsAsyncMethod(methodDeclaration, context.SemanticModel);

        // Walk through the method body to find synchronous over async patterns
        var walker = new SynchronousOverAsyncWalker(context.SemanticModel, isAsyncMethod);
        walker.Visit(methodDeclaration.Body);

        // Report any synchronous over async patterns found
        foreach (var (location, pattern) in walker.SynchronousOverAsyncPatterns)
        {
            var diagnostic = Diagnostic.Create(
                SynchronousOverAsyncRule,
                location,
                methodDeclaration.Identifier.Text,
                pattern);

            context.ReportDiagnostic(diagnostic);
        }
    }

    /// <summary>
    ///     Determines if a method is async by checking for async keyword or Task/ValueTask return type.
    /// </summary>
    private static bool IsAsyncMethod(MethodDeclarationSyntax method, SemanticModel semanticModel)
    {
        // Check for async keyword
        if (method.Modifiers.Any(m => m.IsKind(SyntaxKind.AsyncKeyword)))
            return true;

        // Get the return type symbol
        var returnTypeSymbol = semanticModel.GetTypeInfo(method.ReturnType).Type;

        if (returnTypeSymbol == null)
            return false;

        // Check if return type is Task, ValueTask, or Task<T>
        return returnTypeSymbol.Name == "Task" || returnTypeSymbol.Name == "ValueTask";
    }

    /// <summary>
    ///     AST walker that detects synchronous over async patterns.
    /// </summary>
    private sealed class SynchronousOverAsyncWalker(SemanticModel semanticModel, bool isAsyncMethod) : CSharpSyntaxWalker
    {
        private readonly List<(Location Location, string Pattern)> _synchronousOverAsyncPatterns = [];

        public IReadOnlyList<(Location Location, string Pattern)> SynchronousOverAsyncPatterns => _synchronousOverAsyncPatterns;

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            CheckForSynchronousOverAsyncInvocation(node);
            base.VisitInvocationExpression(node);
        }

        public override void VisitMemberAccessExpression(MemberAccessExpressionSyntax node)
        {
            CheckForSynchronousOverAsyncMemberAccess(node);
            base.VisitMemberAccessExpression(node);
        }

        /// <summary>
        ///     Checks for synchronous over async method invocations.
        /// </summary>
        private void CheckForSynchronousOverAsyncInvocation(InvocationExpressionSyntax invocation)
        {
            // Check for GetAwaiter().GetResult() pattern
            if (invocation.Expression is MemberAccessExpressionSyntax { Name.Identifier.Text: "GetResult" } getresultAccess)
            {
                // Check if this is called on a GetAwaiter() result
                if (getresultAccess.Expression is MemberAccessExpressionSyntax { Name.Identifier.Text: "GetAwaiter" } ||
                    (getresultAccess.Expression is InvocationExpressionSyntax innerInvocation &&
                     innerInvocation.Expression is MemberAccessExpressionSyntax { Name.Identifier.Text: "GetAwaiter" }))
                {
                    _synchronousOverAsyncPatterns.Add((invocation.GetLocation(), "GetAwaiter().GetResult()"));
                    return;
                }
            }

            if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
            {
                var methodName = memberAccess.Name.Identifier.Text;

                // Check for Task.Wait()
                if (methodName == "Wait" && IsTaskType(memberAccess.Expression))
                {
                    _synchronousOverAsyncPatterns.Add((invocation.GetLocation(), "Task.Wait()"));
                    return;
                }

                // Check for Task.Run() in async methods
                if (methodName == "Run" && IsTaskRunType(memberAccess.Expression) && isAsyncMethod)
                {
                    _synchronousOverAsyncPatterns.Add((invocation.GetLocation(), "Task.Run() in async method"));
                    return;
                }

                // Check for async methods that aren't awaited
                var isAsyncCall = IsAsyncMethodCall(memberAccess);

                if (isAsyncCall)
                {
                    var isAwaited = IsAwaited(invocation);

                    if (isAsyncMethod)
                        _synchronousOverAsyncPatterns.Add((invocation.GetLocation(), "Async method call without await"));
                    else
                        _synchronousOverAsyncPatterns.Add((invocation.GetLocation(), "Async method called from sync method without await"));
                }
            }
        }

        /// <summary>
        ///     Checks for blocking property access (like .Result).
        /// </summary>
        private void CheckForSynchronousOverAsyncMemberAccess(MemberAccessExpressionSyntax memberAccess)
        {
            if (memberAccess.Name.Identifier.Text == "Result" && IsTaskType(memberAccess.Expression))
                _synchronousOverAsyncPatterns.Add((memberAccess.GetLocation(), "Task.Result"));
        }

        /// <summary>
        ///     Determines if the expression represents a Task type.
        /// </summary>
        private bool IsTaskType(ExpressionSyntax expression)
        {
            var symbolInfo = semanticModel.GetSymbolInfo(expression);
            var symbol = symbolInfo.Symbol;

            if (symbol == null)
                return false;

            var typeSymbol = symbol switch
            {
                IPropertySymbol propertySymbol => propertySymbol.Type,
                IMethodSymbol methodSymbol => methodSymbol.ReturnType,
                ILocalSymbol localSymbol => localSymbol.Type,
                IParameterSymbol parameterSymbol => parameterSymbol.Type,
                _ => null,
            };

            return typeSymbol != null &&
                   (typeSymbol.Name == "Task" || typeSymbol.Name == "ValueTask" ||
                    (typeSymbol is INamedTypeSymbol namedType && (namedType.Name == "Task" || namedType.Name == "ValueTask")));
        }

        /// <summary>
        ///     Determines if the expression represents Task.Run.
        /// </summary>
        private bool IsTaskRunType(ExpressionSyntax expression)
        {
            return expression is IdentifierNameSyntax { Identifier.Text: "Task" };
        }

        /// <summary>
        ///     Determines if a member access is an async method call.
        /// </summary>
        private bool IsAsyncMethodCall(MemberAccessExpressionSyntax memberAccess)
        {
            // Get the symbol for the member access
            var symbolInfo = semanticModel.GetSymbolInfo(memberAccess);
            var symbol = symbolInfo.Symbol ?? symbolInfo.CandidateSymbols.FirstOrDefault();

            if (symbol is not IMethodSymbol methodSymbol)
                return false;

            // Check if method returns Task, ValueTask, or is marked as async
            var returnType = methodSymbol.ReturnType;

            if (returnType == null)
                return false;

            // Check for Task, ValueTask, Task<T>, or ValueTask<T>
            if (returnType.Name is "Task" or "ValueTask")
                return true;

            // Check for generic Task or ValueTask types
            if (returnType is INamedTypeSymbol namedType &&
                (namedType.Name == "Task" || namedType.Name == "ValueTask"))
                return true;

            // Check if method is marked as async
            return methodSymbol.IsAsync;
        }

        /// <summary>
        ///     Determines if an invocation is properly awaited.
        /// </summary>
        private bool IsAwaited(InvocationExpressionSyntax invocation)
        {
            // Check if invocation is directly awaited
            if (invocation.Parent is AwaitExpressionSyntax)
                return true;

            // Check if invocation is part of an expression that is awaited
            // but only if it's a direct child of awaited expression
            var current = invocation.Parent;

            while (current != null)
            {
                // If we find an await, check if this invocation is part of awaited expression
                if (current is AwaitExpressionSyntax awaitExpression)
                {
                    // The invocation is awaited if it's the direct expression being awaited
                    // or if it's part of a simple member access chain leading to awaited expression
                    var result = IsPartOfAwaitedExpression(invocation, awaitExpression);
                    return result;
                }

                // Stop at statement boundaries - if we cross a statement boundary,
                // invocation is not awaited
                if (current is StatementSyntax)
                    return false;

                current = current.Parent;
            }

            return false;
        }

        /// <summary>
        ///     Determines if invocation is part of expression being awaited.
        /// </summary>
        private static bool IsPartOfAwaitedExpression(InvocationExpressionSyntax invocation, AwaitExpressionSyntax awaitExpression)
        {
            // Check if invocation is direct expression being awaited
            if (awaitExpression.Expression == invocation)
                return true;

            // Check if invocation is part of a member access chain that leads to awaited expression
            var current = (SyntaxNode)invocation;

            while (current != null && current != awaitExpression.Expression)
            {
                if (current is MemberAccessExpressionSyntax memberAccess)
                    current = memberAccess;
                else
                    break;
            }

            return current == awaitExpression.Expression;
        }
    }
}
