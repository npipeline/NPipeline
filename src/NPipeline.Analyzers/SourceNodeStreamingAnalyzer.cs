using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects non-streaming patterns in SourceNode implementations that can lead to
///     memory issues and poor performance. These patterns should be avoided:
///     1. Allocating and populating List&lt;T&gt; or Array in ExecuteAsync
///     2. Using .ToAsyncEnumerable() on materialized collections
///     3. Synchronous I/O operations
///     4. Not using async IAsyncEnumerable with yield return patterns
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SourceNodeStreamingAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for the analyzer.
    /// </summary>
    public const string SourceNodeStreamingId = "NP9211";

    private static readonly DiagnosticDescriptor SourceNodeStreamingRule = new(
        SourceNodeStreamingId,
        "Use streaming patterns in SourceNode implementations",
        "SourceNode '{0}' contains non-streaming pattern '{1}' that can cause memory issues and poor performance. "
        + "Use async IAsyncEnumerable with yield return instead to stream data efficiently.",
        "Performance",
        DiagnosticSeverity.Warning,
        true,
        "Non-streaming patterns in SourceNode implementations can cause memory issues and performance problems. "
        + "Use streaming patterns: IAsyncEnumerable with yield return, StreamingDataPipe, async I/O operations, "
        + "and avoid materializing collections in memory. "
        + "https://npipeline.dev/docs/performance/source-node-streaming-patterns.");

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [SourceNodeStreamingRule];

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze method bodies for non-streaming patterns
        context.RegisterSyntaxNodeAction(AnalyzeMethod, SyntaxKind.MethodDeclaration);
    }

    private static void AnalyzeMethod(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not MethodDeclarationSyntax methodDeclaration)
            return;

        // Check if this method is ExecuteAsync in a SourceNode implementation
        var isSourceNodeExecuteAsync = IsSourceNodeExecuteAsyncMethod(methodDeclaration, context.SemanticModel);

        if (!isSourceNodeExecuteAsync)
            return;

        // Walk through method body to find non-streaming patterns
        var walker = new NonStreamingPatternWalker(context.SemanticModel);
        walker.Visit(methodDeclaration.Body);

        // Report any non-streaming patterns found
        foreach (var (location, pattern) in walker.NonStreamingPatterns)
        {
            var diagnostic = Diagnostic.Create(
                SourceNodeStreamingRule,
                location,
                methodDeclaration.Identifier.Text,
                pattern);

            context.ReportDiagnostic(diagnostic);
        }
    }

    /// <summary>
    ///     Determines if a method is ExecuteAsync in a SourceNode implementation.
    /// </summary>
    private static bool IsSourceNodeExecuteAsyncMethod(MethodDeclarationSyntax method, SemanticModel semanticModel)
    {
        // Check if method name is ExecuteAsync
        if (method.Identifier.Text != "ExecuteAsync")
            return false;

        // Get containing class
        if (method.Parent is not ClassDeclarationSyntax classDeclaration)
            return false;

        // Get the class symbol
        var classSymbol = semanticModel.GetDeclaredSymbol(classDeclaration);

        if (classSymbol == null)
            return false;

        // Check if the class inherits from SourceNode<T>
        var baseType = classSymbol.BaseType;

        while (baseType != null)
        {
            // Check for both non-generic and generic SourceNode
            if (baseType.Name == "SourceNode" && baseType.ContainingNamespace?.Name == "Nodes")
                return true;

            baseType = baseType.BaseType;
        }

        return false;
    }

    /// <summary>
    ///     AST walker that detects non-streaming patterns in SourceNode ExecuteAsync methods.
    /// </summary>
    private sealed class NonStreamingPatternWalker(SemanticModel semanticModel) : CSharpSyntaxWalker
    {
        private readonly List<(Location Location, string Pattern)> _nonStreamingPatterns = [];

        public IReadOnlyList<(Location Location, string Pattern)> NonStreamingPatterns => _nonStreamingPatterns;

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            CheckForNonStreamingInvocation(node);
            base.VisitInvocationExpression(node);
        }

        public override void VisitLocalDeclarationStatement(LocalDeclarationStatementSyntax node)
        {
            base.VisitLocalDeclarationStatement(node);
        }

        public override void VisitVariableDeclaration(VariableDeclarationSyntax node)
        {
            base.VisitVariableDeclaration(node);
        }

        public override void VisitEqualsValueClause(EqualsValueClauseSyntax node)
        {
            base.VisitEqualsValueClause(node);
        }

        public override void VisitArrayCreationExpression(ArrayCreationExpressionSyntax node)
        {
            var typeInfo = semanticModel.GetTypeInfo(node);
            var typeSymbol = typeInfo.Type;

            if (typeSymbol?.Kind == SymbolKind.ArrayType)
                _nonStreamingPatterns.Add((node.GetLocation(), "Array allocation - consider streaming instead"));

            base.VisitArrayCreationExpression(node);
        }

        public override void VisitObjectCreationExpression(ObjectCreationExpressionSyntax node)
        {
            CheckForNonStreamingObjectCreation(node);
            base.VisitObjectCreationExpression(node);
        }

        public override void VisitMemberAccessExpression(MemberAccessExpressionSyntax node)
        {
            CheckForNonStreamingMemberAccess(node);
            base.VisitMemberAccessExpression(node);
        }

        /// <summary>
        ///     Checks for non-streaming method invocations.
        /// </summary>
        private void CheckForNonStreamingInvocation(InvocationExpressionSyntax invocation)
        {
            if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
            {
                var methodName = memberAccess.Name.Identifier.Text;

                // Check for .ToAsyncEnumerable() on collections
                if (methodName == "ToAsyncEnumerable" && IsCollectionType(memberAccess.Expression))
                {
                    _nonStreamingPatterns.Add((invocation.GetLocation(), ".ToAsyncEnumerable() on materialized collection"));
                    return;
                }

                // Check for synchronous file I/O operations
                if (IsFileIOSynchronousCall(memberAccess))
                {
                    _nonStreamingPatterns.Add((invocation.GetLocation(), $"Synchronous file I/O: File.{methodName}()"));
                    return;
                }

                // Check for .ToList() or .ToArray() on collections
                if ((methodName == "ToList" || methodName == "ToArray") && IsEnumerableType(memberAccess.Expression))
                    _nonStreamingPatterns.Add((invocation.GetLocation(), $".{methodName}() materializes collection in memory"));
            }
        }

        /// <summary>
        ///     Checks for non-streaming object creation patterns.
        /// </summary>
        private void CheckForNonStreamingObjectCreation(ObjectCreationExpressionSyntax objectCreation)
        {
            var typeInfo = semanticModel.GetTypeInfo(objectCreation);
            var typeSymbol = typeInfo.Type;

            // Check for List<T> or array creation
            if (typeSymbol?.Name == "List" &&
                (typeSymbol.ContainingNamespace?.Name == "System.Collections.Generic" ||
                 typeSymbol.ContainingNamespace?.Name == "Generic"))
            {
                _nonStreamingPatterns.Add((objectCreation.GetLocation(), "List<T> allocation - consider streaming instead"));
                return;
            }

            // Check for array creation
            if (typeSymbol?.Kind == SymbolKind.ArrayType)
                _nonStreamingPatterns.Add((objectCreation.GetLocation(), "Array allocation - consider streaming instead"));
        }

        /// <summary>
        ///     Checks for non-streaming member access patterns.
        /// </summary>
        private void CheckForNonStreamingMemberAccess(MemberAccessExpressionSyntax memberAccess)
        {
            var methodName = memberAccess.Name.Identifier.Text;

            // Check for synchronous file I/O property access
            if (IsFileIOSynchronousProperty(memberAccess))
                _nonStreamingPatterns.Add((memberAccess.GetLocation(), $"Synchronous file I/O property: {methodName}"));
        }

        /// <summary>
        ///     Determines if expression represents a collection type.
        /// </summary>
        private bool IsCollectionType(ExpressionSyntax expression)
        {
            var typeInfo = semanticModel.GetTypeInfo(expression);
            var typeSymbol = typeInfo.Type;

            if (typeSymbol == null)
                return false;

            return typeSymbol.Name switch
            {
                "List" or "Array" or "IEnumerable" or "ICollection" or "IList" => true,
                _ => typeSymbol.AllInterfaces.Any(i => i.Name == "IEnumerable" && i.ContainingNamespace?.Name == "System.Collections.Generic"),
            };
        }

        /// <summary>
        ///     Determines if expression represents an enumerable type.
        /// </summary>
        private bool IsEnumerableType(ExpressionSyntax expression)
        {
            var typeInfo = semanticModel.GetTypeInfo(expression);
            var typeSymbol = typeInfo.Type;

            if (typeSymbol == null)
                return false;

            return typeSymbol.Name switch
            {
                "IEnumerable" or "IQueryable" => true,
                _ => typeSymbol.AllInterfaces.Any(i => i.Name == "IEnumerable" && i.ContainingNamespace?.Name == "System.Collections.Generic"),
            };
        }

        /// <summary>
        ///     Checks if member access is a synchronous file I/O operation.
        /// </summary>
        private bool IsFileIOSynchronousCall(MemberAccessExpressionSyntax memberAccess)
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
        ///     Checks if member access is a synchronous file I/O property.
        /// </summary>
        private bool IsFileIOSynchronousProperty(MemberAccessExpressionSyntax memberAccess)
        {
            if (memberAccess.Expression is not IdentifierNameSyntax { Identifier.Text: "File" })
                return false;

            var propertyName = memberAccess.Name.Identifier.Text;

            return propertyName switch
            {
                // Add any synchronous file properties if needed
                _ => false,
            };
        }
    }
}
