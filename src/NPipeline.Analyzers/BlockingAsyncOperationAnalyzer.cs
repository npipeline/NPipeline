using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects blocking patterns in async methods that can lead to deadlocks
///     and performance issues. These patterns should be avoided in async code:
///     1. .Result and .Wait() calls on Tasks
///     2. GetAwaiter().GetResult() patterns
///     3. Thread.Sleep() in async methods (should use Task.DelayAsync instead)
///     4. Synchronous file I/O operations (File.ReadAllText, etc.)
///     5. Synchronous network I/O operations
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class BlockingAsyncOperationAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for blocking operations in async methods.
    /// </summary>
    public const string BlockingAsyncOperationId = "NP9101";

    private static readonly DiagnosticDescriptor BlockingAsyncOperationRule = new(
        BlockingAsyncOperationId,
        "Avoid blocking calls in async methods",
        "Async method '{0}' contains a blocking operation '{1}' that can cause deadlocks and reduce performance. "
        + "Use async alternatives instead to maintain the async chain and prevent thread pool starvation.",
        "Performance",
        DiagnosticSeverity.Warning,
        true,
        "Blocking operations in async methods can cause deadlocks and performance issues. "
        + "Use async alternatives: await instead of .Result/.Wait(), Task.Delay instead of Thread.Sleep, "
        + "File.ReadAllTextAsync instead of File.ReadAllText, etc. "
        + "https://npipeline.dev/docs/async-programming/avoiding-blocking-patterns.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [BlockingAsyncOperationRule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze method bodies for blocking operations
        context.RegisterSyntaxNodeAction(AnalyzeMethod, SyntaxKind.MethodDeclaration);
    }

    private static void AnalyzeMethod(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not MethodDeclarationSyntax methodDeclaration)
            return;

        // Check if this method is async
        if (!IsAsyncMethod(methodDeclaration, context.SemanticModel))
            return;

        // Walk through the method body to find blocking operations
        var walker = new BlockingOperationWalker(context.SemanticModel);
        walker.Visit(methodDeclaration.Body);

        // Report any blocking operations found
        foreach (var (location, operation) in walker.BlockingOperations)
        {
            var diagnostic = Diagnostic.Create(
                BlockingAsyncOperationRule,
                location,
                methodDeclaration.Identifier.Text,
                operation);

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
    ///     AST walker that detects blocking operations in async methods.
    /// </summary>
    private sealed class BlockingOperationWalker(SemanticModel semanticModel) : CSharpSyntaxWalker
    {
        private readonly List<(Location Location, string Operation)> _blockingOperations = [];

        public IReadOnlyList<(Location Location, string Operation)> BlockingOperations => _blockingOperations;

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            CheckForBlockingInvocation(node);
            base.VisitInvocationExpression(node);
        }

        public override void VisitMemberAccessExpression(MemberAccessExpressionSyntax node)
        {
            CheckForBlockingMemberAccess(node);
            base.VisitMemberAccessExpression(node);
        }

        /// <summary>
        ///     Checks for blocking method invocations.
        /// </summary>
        private void CheckForBlockingInvocation(InvocationExpressionSyntax invocation)
        {
            // Check for GetAwaiter().GetResult() pattern
            if (invocation.Expression is MemberAccessExpressionSyntax { Name.Identifier.Text: "GetResult" } getresultAccess)
            {
                // Check if this is called on a GetAwaiter() result
                if (getresultAccess.Expression is MemberAccessExpressionSyntax { Name.Identifier.Text: "GetAwaiter" } ||
                    (getresultAccess.Expression is InvocationExpressionSyntax innerInvocation &&
                     innerInvocation.Expression is MemberAccessExpressionSyntax { Name.Identifier.Text: "GetAwaiter" }))
                {
                    _blockingOperations.Add((invocation.GetLocation(), "GetAwaiter().GetResult()"));
                    return;
                }
            }

            if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
            {
                var methodName = memberAccess.Name.Identifier.Text;

                // Check for Task.Wait()
                if (methodName == "Wait" && IsTaskType(memberAccess.Expression))
                {
                    _blockingOperations.Add((invocation.GetLocation(), "Task.Wait()"));
                    return;
                }

                // Check for Thread.Sleep()
                if (methodName == "Sleep" && IsThreadType(memberAccess.Expression))
                {
                    _blockingOperations.Add((invocation.GetLocation(), "Thread.Sleep()"));
                    return;
                }

                // Check for synchronous file I/O operations
                if (IsFileIOBlockingCall(memberAccess))
                {
                    _blockingOperations.Add((invocation.GetLocation(), $"File.{methodName}()"));
                    return;
                }

                // Check for WebClient blocking methods
                if (IsWebClientBlockingCall(memberAccess))
                {
                    _blockingOperations.Add((invocation.GetLocation(), $"WebClient.{methodName}()"));
                    return;
                }

                // Check for HttpClient blocking methods (when used with .Result or .Wait())
                if (IsHttpClientBlockingCall(invocation, memberAccess))
                {
                    _blockingOperations.Add((invocation.GetLocation(), $"HttpClient.{methodName}()"));
                    return;
                }

                // Check for StreamReader/Writer blocking methods
                if (IsStreamReaderWriterBlockingCall(invocation, memberAccess))
                    _blockingOperations.Add((invocation.GetLocation(), $"{GetStreamReaderWriterTypeName(memberAccess.Expression)}.{methodName}()"));
            }
        }

        /// <summary>
        ///     Checks for blocking property access (like .Result).
        /// </summary>
        private void CheckForBlockingMemberAccess(MemberAccessExpressionSyntax memberAccess)
        {
            if (memberAccess.Name.Identifier.Text == "Result" && IsTaskType(memberAccess.Expression))
                _blockingOperations.Add((memberAccess.GetLocation(), "Task.Result"));
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
        ///     Determines if the expression represents Thread type.
        /// </summary>
        private bool IsThreadType(ExpressionSyntax expression)
        {
            return expression is IdentifierNameSyntax { Identifier.Text: "Thread" };
        }

        /// <summary>
        ///     Checks if the member access is a synchronous file I/O operation.
        /// </summary>
        private bool IsFileIOBlockingCall(MemberAccessExpressionSyntax memberAccess)
        {
            if (memberAccess.Expression is not IdentifierNameSyntax { Identifier.Text: "File" })
                return false;

            var methodName = memberAccess.Name.Identifier.Text;

            return methodName switch
            {
                "ReadAllText" or "ReadAllLines" or "ReadAllBytes" or "WriteAllText" or
                    "WriteAllLines" or "WriteAllBytes" or "AppendAllText" or "AppendAllLines" or
                    "OpenRead" or "OpenWrite" or "Create" or "Delete" or "Exists" or "Copy" or "Move" => true,
                _ => false,
            };
        }

        /// <summary>
        ///     Checks if the invocation is an HttpClient blocking operation.
        /// </summary>
        private bool IsHttpClientBlockingCall(InvocationExpressionSyntax invocation, MemberAccessExpressionSyntax memberAccess)
        {
            // Check if the expression is on an HttpClient instance
            if (!IsHttpClientType(memberAccess.Expression))
                return false;

            var methodName = memberAccess.Name.Identifier.Text;

            // Check for async methods that should be awaited
            if (methodName is "GetStringAsync" or "GetByteArrayAsync" or "GetStreamAsync" or
                "GetAsync" or "PostAsync" or "PutAsync" or "DeleteAsync" or "SendAsync")
            {
                // Check if it's being awaited (not blocking) or not awaited (blocking)
                return !IsAwaited(invocation);
            }

            return false;
        }

        /// <summary>
        ///     Determines if the expression represents an HttpClient type.
        /// </summary>
        private bool IsHttpClientType(ExpressionSyntax expression)
        {
            // Check for direct HttpClient type
            if (expression is IdentifierNameSyntax { Identifier.Text: "HttpClient" })
                return true;

            // Check for variable of HttpClient type
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

            return typeSymbol != null && typeSymbol.Name == "HttpClient";
        }

        /// <summary>
        ///     Checks if the member access is a WebClient blocking operation.
        /// </summary>
        private bool IsWebClientBlockingCall(MemberAccessExpressionSyntax memberAccess)
        {
            // Check if the expression is on a WebClient instance
            if (!IsWebClientType(memberAccess.Expression))
                return false;

            var methodName = memberAccess.Name.Identifier.Text;

            return methodName is "DownloadString" or "DownloadData" or "DownloadStringTaskAsync" or "DownloadDataTaskAsync" or
                "UploadString" or "UploadData" or "UploadStringTaskAsync" or "UploadDataTaskAsync";
        }

        /// <summary>
        ///     Determines if the expression represents a WebClient type.
        /// </summary>
        private bool IsWebClientType(ExpressionSyntax expression)
        {
            // Check for direct WebClient type
            if (expression is IdentifierNameSyntax { Identifier.Text: "WebClient" })
                return true;

            // Check for variable of WebClient type
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

            return typeSymbol != null && typeSymbol.Name == "WebClient";
        }

        /// <summary>
        ///     Checks if the invocation is a StreamReader/Writer blocking operation.
        /// </summary>
        private bool IsStreamReaderWriterBlockingCall(InvocationExpressionSyntax invocation, MemberAccessExpressionSyntax memberAccess)
        {
            var methodName = memberAccess.Name.Identifier.Text;

            // Check for StreamReader blocking methods
            if (methodName is "ReadToEnd" or "ReadLine" or "Read" or "ReadBlock" or "ReadToEndAsync" or "ReadLineAsync" or "ReadAsync" or "ReadBlockAsync")
            {
                // Check if it's being awaited (not blocking) or not awaited (blocking)
                if (IsStreamReaderOrWriterType(memberAccess.Expression))
                    return !IsAwaited(invocation);
            }

            // Check for StreamWriter blocking methods
            if (methodName is "Write" or "WriteLine" or "WriteAsync" or "WriteLineAsync" or "Flush" or "FlushAsync")
            {
                // Check if it's being awaited (not blocking) or not awaited (blocking)
                if (IsStreamReaderOrWriterType(memberAccess.Expression))
                    return !IsAwaited(invocation);
            }

            return false;
        }

        /// <summary>
        ///     Determines if the expression represents a StreamReader or StreamWriter type.
        /// </summary>
        private bool IsStreamReaderOrWriterType(ExpressionSyntax expression)
        {
            var typeInfo = semanticModel.GetTypeInfo(expression);
            var typeSymbol = typeInfo.Type;

            return typeSymbol != null && (typeSymbol.Name == "StreamReader" || typeSymbol.Name == "StreamWriter");
        }

        /// <summary>
        ///     Gets the type name for StreamReader/Writer for reporting purposes.
        /// </summary>
        private string GetStreamReaderWriterTypeName(ExpressionSyntax expression)
        {
            var typeInfo = semanticModel.GetTypeInfo(expression);
            var typeSymbol = typeInfo.Type;

            return typeSymbol?.Name ?? "StreamReader/Writer";
        }

        /// <summary>
        ///     Determines if an invocation is properly awaited.
        /// </summary>
        private bool IsAwaited(InvocationExpressionSyntax invocation)
        {
            // Check if the invocation is directly awaited
            if (invocation.Parent is AwaitExpressionSyntax)
                return true;

            // Check if the invocation is part of an awaited expression
            var current = invocation.Parent;

            while (current != null)
            {
                if (current is AwaitExpressionSyntax)
                    return true;

                current = current.Parent;
            }

            return false;
        }
    }
}
