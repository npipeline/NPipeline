using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects non-streaming patterns in SourceNode implementations that can lead to
///     memory issues and poor performance. These patterns should be avoided:
///     1. Allocating and populating List&lt;T&gt; or Array in Initialize
///     2. Using .ToAsyncEnumerable() on materialized collections
///     3. Synchronous I/O operations
///     4. Not using async IAsyncEnumerable with yield return patterns
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SourceNodeStreamingAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for analyzer.
    /// </summary>
    public const string SourceNodeStreamingId = "NP9107";

    private static readonly DiagnosticDescriptor SourceNodeStreamingRule = new(
        SourceNodeStreamingId,
        "Use streaming patterns in SourceNode implementations",
        "SourceNode '{0}' contains non-streaming pattern '{1}' that can cause memory issues and poor performance. "
        + "Use async IAsyncEnumerable with yield return instead to stream data efficiently.",
        "Performance & Optimization",
        DiagnosticSeverity.Warning,
        true,
        "Non-streaming patterns in SourceNode implementations can cause memory issues and performance problems. "
        + "Use streaming patterns: IAsyncEnumerable with yield return, StreamingDataPipe, async I/O operations, "
        + "and avoid materializing collections in memory. "
        + "https://npipeline.dev/docs/performance/source-node-streaming-patterns.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [SourceNodeStreamingRule];

    /// <inheritdoc />
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

        // Check if this method is Initialize in a SourceNode implementation
        var isSourceNodeInitializeMethod = IsSourceNodeInitializeMethod(methodDeclaration, context.SemanticModel);

        if (!isSourceNodeInitializeMethod)
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
    ///     Determines if a method is Initialize in a SourceNode implementation.
    /// </summary>
    private static bool IsSourceNodeInitializeMethod(MethodDeclarationSyntax method, SemanticModel semanticModel)
    {
        // Check if method name is Initialize
        if (method.Identifier.Text != "Initialize")
            return false;

        // Get the method symbol to check signature
        var methodSymbol = semanticModel.GetDeclaredSymbol(method);

        if (methodSymbol == null)
            return false;

        // Check if method has exactly 2 parameters
        if (methodSymbol.Parameters.Length != 2)
            return false;

        // Check parameter types: PipelineContext and CancellationToken (be more lenient with namespace checking)
        var firstParam = methodSymbol.Parameters[0];
        var secondParam = methodSymbol.Parameters[1];

        var hasPipelineContext = firstParam.Type.Name == "PipelineContext";
        var hasCancellationToken = secondParam.Type.Name == "CancellationToken";

        if (!hasPipelineContext || !hasCancellationToken)
            return false;

        // Check return type is IDataPipe<T> (be more lenient)
        var returnType = methodSymbol.ReturnType;
        var hasCorrectReturnType = returnType.Name == "IDataPipe" || returnType.OriginalDefinition?.Name == "IDataPipe";

        if (!hasCorrectReturnType)
            return false;

        // Get containing class
        if (method.Parent is not ClassDeclarationSyntax classDeclaration)
            return false;

        // Get the class symbol
        var classSymbol = semanticModel.GetDeclaredSymbol(classDeclaration);

        if (classSymbol == null)
            return false;

        // Check if the class inherits from SourceNode<T> (be more lenient)
        var baseType = classSymbol.BaseType;

        while (baseType != null)
        {
            // Check for both non-generic and generic SourceNode
            if (baseType.Name == "SourceNode")
                return true;

            baseType = baseType.BaseType;
        }

        return false;
    }

    /// <summary>
    ///     AST walker that detects non-streaming patterns in SourceNode Initialize methods.
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
            // Check for patterns in local variable declarations
            foreach (var variable in node.Declaration.Variables)
            {
                if (variable.Initializer != null)
                    CheckForNonStreamingPatterns(variable.Initializer.Value);
            }

            base.VisitLocalDeclarationStatement(node);
        }

        public override void VisitVariableDeclaration(VariableDeclarationSyntax node)
        {
            // Check for patterns in variable declarations
            foreach (var variable in node.Variables)
            {
                if (variable.Initializer != null)
                    CheckForNonStreamingPatterns(variable.Initializer.Value);
            }

            base.VisitVariableDeclaration(node);
        }

        public override void VisitEqualsValueClause(EqualsValueClauseSyntax node)
        {
            // Check for patterns in equals value clauses (variable initializers)
            if (node.Value != null)
                CheckForNonStreamingPatterns(node.Value);

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
        ///     Checks for non-streaming patterns in expressions.
        /// </summary>
        private void CheckForNonStreamingPatterns(ExpressionSyntax expression)
        {
            if (expression is ObjectCreationExpressionSyntax objectCreation)
                CheckForNonStreamingObjectCreation(objectCreation);
            else if (expression is ArrayCreationExpressionSyntax arrayCreation)
                CheckForNonStreamingArrayCreation(arrayCreation);
            else if (expression is InvocationExpressionSyntax invocation)
                CheckForNonStreamingInvocation(invocation);
            else if (expression is MemberAccessExpressionSyntax memberAccess)
                CheckForNonStreamingMemberAccess(memberAccess);
        }

        /// <summary>
        ///     Checks for non-streaming array creation patterns.
        /// </summary>
        private void CheckForNonStreamingArrayCreation(ArrayCreationExpressionSyntax arrayCreation)
        {
            var typeInfo = semanticModel.GetTypeInfo(arrayCreation);
            var typeSymbol = typeInfo.Type;

            if (typeSymbol?.Kind == SymbolKind.ArrayType)
                _nonStreamingPatterns.Add((arrayCreation.GetLocation(), "Array allocation - consider streaming instead"));
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

            // Check for List<T> creation
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

            return typeSymbol?.Name switch
            {
                "List" or "Array" or "IEnumerable" or "ICollection" or "IList" => true,
                _ => typeSymbol?.AllInterfaces.Any(i => i.Name == "IEnumerable" && i.ContainingNamespace?.Name == "System.Collections.Generic") == true,
            };
        }

        /// <summary>
        ///     Determines if expression represents an enumerable type.
        /// </summary>
        private bool IsEnumerableType(ExpressionSyntax expression)
        {
            var typeInfo = semanticModel.GetTypeInfo(expression);
            var typeSymbol = typeInfo.Type;

            return typeSymbol?.Name switch
            {
                "IEnumerable" or "IQueryable" => true,
                _ => typeSymbol?.AllInterfaces.Any(i => i.Name == "IEnumerable" && i.ContainingNamespace?.Name == "System.Collections.Generic") == true,
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
