using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects inefficient exception handling patterns that create performance overhead
///     and may mask important errors in NPipeline pipelines.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class InefficientExceptionHandlingAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for inefficient exception handling patterns.
    /// </summary>
    public const string DiagnosticId = "NP9302";

    private static readonly DiagnosticDescriptor Rule = new(
        DiagnosticId,
        "Inefficient exception handling pattern detected",
        "Exception handling pattern '{0}' may cause performance issues or hide important errors: {1}",
        "Reliability",
        DiagnosticSeverity.Warning,
        true,
        "Inefficient exception handling can cause performance issues and mask important errors. "
        + "Use specific exception types, avoid catch-all patterns, and handle exceptions appropriately. "
        + "Avoid empty catch blocks and re-throwing with 'throw;' which loses stack trace information. "
        + "https://npipeline.dev/docs/reliability/efficient-exception-handling.");

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule];

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze try-catch statements
        context.RegisterSyntaxNodeAction(AnalyzeTryStatement, SyntaxKind.TryStatement);
    }

    private static void AnalyzeTryStatement(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not TryStatementSyntax tryStatement)
            return;

        // Check if in hot path context
        if (!IsInHotPathContext(tryStatement, context.SemanticModel))
            return;

        var analyzer = new ExceptionHandlingAnalyzer(tryStatement, context.SemanticModel);
        analyzer.Analyze();

        // Report any diagnostics found
        foreach (var diagnostic in analyzer.Diagnostics)
        {
            context.ReportDiagnostic(diagnostic);
        }
    }

    /// <summary>
    ///     Determines if a syntax node is in a hot path context.
    /// </summary>
    private static bool IsInHotPathContext(SyntaxNode node, SemanticModel semanticModel)
    {
        // Find enclosing method
        var enclosingMethod = node.FirstAncestorOrSelf<MethodDeclarationSyntax>();

        if (enclosingMethod == null)
            return false;

        var methodName = enclosingMethod.Identifier.Text;

        // Check if this is an async method (potential hot path) FIRST
        if (IsAsyncMethod(enclosingMethod, semanticModel))
        {
            // Always flag async methods in NPipeline node classes
            var isInNPipelineNode = IsInNPipelineNode(enclosingMethod, semanticModel);

            if (isInNPipelineNode)
                return true;

            // Also flag async methods if NPipeline namespaces are imported
            var compilationUnit = enclosingMethod.SyntaxTree.GetCompilationUnitRoot();

            var hasNPipelineImport = compilationUnit.Usings
                .Any(u => u.Name?.ToString().Contains("NPipeline") == true);

            if (hasNPipelineImport)
                return true;
        }
        else
        {
            // For non-async methods, check if this is a hot path method by name
            if (IsHotPathMethodByName(enclosingMethod))
                return true;
        }

        return false;
    }

    /// <summary>
    ///     Determines if a method is async.
    /// </summary>
    private static bool IsAsyncMethod(MethodDeclarationSyntax method, SemanticModel semanticModel)
    {
        // Check for async keyword
        if (method.Modifiers.Any(m => m.IsKind(SyntaxKind.AsyncKeyword)))
            return true;

        // Check if return type is Task, ValueTask, or Task<T>
        var returnTypeSymbol = semanticModel.GetTypeInfo(method.ReturnType).Type;

        return returnTypeSymbol != null && (
            returnTypeSymbol.Name == "Task" ||
            returnTypeSymbol.Name == "ValueTask" ||
            (returnTypeSymbol is INamedTypeSymbol namedType &&
             (namedType.Name == "Task" || namedType.Name == "ValueTask")));
    }

    /// <summary>
    ///     Determines if a method is a hot path method by its name.
    /// </summary>
    private static bool IsHotPathMethodByName(MethodDeclarationSyntax method)
    {
        var methodName = method.Identifier.Text;

        var hotPathMethodNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "ExecuteAsync", "ProcessAsync", "RunAsync", "HandleAsync", "Execute", "Process", "Run", "Handle",
        };

        return hotPathMethodNames.Contains(methodName);
    }

    /// <summary>
    ///     Determines if method is in a class that inherits from NPipeline node interfaces.
    /// </summary>
    private static bool IsInNPipelineNode(MethodDeclarationSyntax method, SemanticModel semanticModel)
    {
        // Get containing class
        var classDeclaration = method.FirstAncestorOrSelf<ClassDeclarationSyntax>();

        if (classDeclaration == null)
            return false;

        // Get class symbol
        var classSymbol = semanticModel.GetDeclaredSymbol(classDeclaration);

        if (classSymbol == null)
            return false;

        // Check if class inherits from any NPipeline node interface
        var nodeInterfaces = new[]
        {
            "INode", "ISourceNode", "ITransformNode", "ISinkNode", "IAggregateNode",
            "IJoinNode", "ICustomMergeNode", "ICustomMergeNodeUntyped",
        };

        foreach (var interfaceName in nodeInterfaces)
        {
            if (ImplementsInterface(classSymbol, interfaceName))
                return true;
        }

        return false;
    }

    /// <summary>
    ///     Checks if a type implements a specific interface by name.
    /// </summary>
    private static bool ImplementsInterface(INamedTypeSymbol typeSymbol, string interfaceName)
    {
        // Check direct interfaces
        foreach (var interfaceSymbol in typeSymbol.AllInterfaces)
        {
            if (interfaceSymbol.Name == interfaceName)
                return true;
        }

        // Check base types
        var baseType = typeSymbol.BaseType;

        while (baseType != null)
        {
            foreach (var interfaceSymbol in baseType.AllInterfaces)
            {
                if (interfaceSymbol.Name == interfaceName)
                    return true;
            }

            baseType = baseType.BaseType;
        }

        return false;
    }

    /// <summary>
    ///     Analyzes exception handling patterns in try-catch statements.
    /// </summary>
    private sealed class ExceptionHandlingAnalyzer
    {
        private readonly List<Diagnostic> _diagnostics = [];
        private readonly SemanticModel _semanticModel;
        private readonly TryStatementSyntax _tryStatement;

        public ExceptionHandlingAnalyzer(TryStatementSyntax tryStatement, SemanticModel semanticModel)
        {
            _tryStatement = tryStatement;
            _semanticModel = semanticModel;
        }

        public IReadOnlyList<Diagnostic> Diagnostics => _diagnostics;

        public void Analyze()
        {
            var catches = _tryStatement.Catches;

            CheckForCatchAllException(catches);
            CheckForSwallowingExceptions(catches);
            CheckForInefficientExceptionFilters(catches);
            CheckForImproperRethrow(catches);
            CheckForEmptyCatchBlocks(catches);
        }

        /// <summary>
        ///     Checks for catch-all exception handlers.
        /// </summary>
        private void CheckForCatchAllException(SyntaxList<CatchClauseSyntax> catches)
        {
            foreach (var catchClause in catches)
            {
                // Check for catch (Exception ex) or catch without type
                if (catchClause.Declaration != null)
                {
                    var exceptionType = _semanticModel.GetTypeInfo(catchClause.Declaration.Type).Type;

                    if (exceptionType?.Name == "Exception")
                    {
                        var diagnostic = Diagnostic.Create(
                            Rule,
                            catchClause.GetLocation(),
                            "Catch-all exception handler (Exception)",
                            "Consider catching specific exception types instead of catch-all");

                        _diagnostics.Add(diagnostic);
                    }
                }
                else
                {
                    // Catch without type specification
                    var diagnostic = Diagnostic.Create(
                        Rule,
                        catchClause.GetLocation(),
                        "Catch-all exception handler (no type)",
                        "Consider catching specific exception types instead of catch-all");

                    _diagnostics.Add(diagnostic);
                }
            }
        }

        /// <summary>
        ///     Checks for exception swallowing patterns.
        /// </summary>
        private void CheckForSwallowingExceptions(SyntaxList<CatchClauseSyntax> catches)
        {
            foreach (var catchClause in catches)
            {
                if (IsSwallowingException(catchClause))
                {
                    var diagnostic = Diagnostic.Create(
                        Rule,
                        catchClause.GetLocation(),
                        "Exception swallowing pattern",
                        "Consider logging exceptions or handling them appropriately instead of swallowing");

                    _diagnostics.Add(diagnostic);
                }
            }
        }

        /// <summary>
        ///     Checks for inefficient exception filters.
        /// </summary>
        private void CheckForInefficientExceptionFilters(SyntaxList<CatchClauseSyntax> catches)
        {
            foreach (var catchClause in catches)
            {
                if (catchClause.Filter != null)
                {
                    var filterExpression = catchClause.Filter.FilterExpression;

                    // Check for string operations in exception filters
                    if (HasStringOperations(filterExpression))
                    {
                        var diagnostic = Diagnostic.Create(
                            Rule,
                            catchClause.Filter.GetLocation(),
                            "Inefficient exception filtering with string operations",
                            "Consider using exception type filtering instead of string-based filtering");

                        _diagnostics.Add(diagnostic);
                    }
                }
            }
        }

        /// <summary>
        ///     Checks for improper re-throw patterns.
        /// </summary>
        private void CheckForImproperRethrow(SyntaxList<CatchClauseSyntax> catches)
        {
            foreach (var catchClause in catches)
            {
                if (HasImproperRethrow(catchClause))
                {
                    var diagnostic = Diagnostic.Create(
                        Rule,
                        catchClause.GetLocation(),
                        "Improper re-throw pattern",
                        "Use 'throw;' to preserve stack trace instead of 'throw ex;'");

                    _diagnostics.Add(diagnostic);
                }
            }
        }

        /// <summary>
        ///     Checks for empty catch blocks.
        /// </summary>
        private void CheckForEmptyCatchBlocks(SyntaxList<CatchClauseSyntax> catches)
        {
            foreach (var catchClause in catches)
            {
                if (IsEmptyCatchBlock(catchClause))
                {
                    var diagnostic = Diagnostic.Create(
                        Rule,
                        catchClause.GetLocation(),
                        "Empty catch block",
                        "Empty catch blocks hide exceptions without any handling");

                    _diagnostics.Add(diagnostic);
                }
            }
        }

        /// <summary>
        ///     Determines if a catch clause swallows exceptions.
        /// </summary>
        private static bool IsSwallowingException(CatchClauseSyntax catchClause)
        {
            var block = catchClause.Block;

            if (block == null)
                return true;

            var statements = block.Statements;

            if (statements.Count == 0)
                return true;

            // Check if only has comments or empty statements
            var hasOnlyComments = statements.All(s =>
                s is EmptyStatementSyntax ||
                s.ToString().Trim().StartsWith("//", StringComparison.Ordinal) ||
                s.ToString().Trim().StartsWith("/*", StringComparison.Ordinal));

            // Check if only logs but doesn't re-throw or handle
            var hasOnlyLogging = statements.All(IsLoggingStatement);

            return hasOnlyComments || hasOnlyLogging;
        }

        /// <summary>
        ///     Determines if a catch block is empty.
        /// </summary>
        private static bool IsEmptyCatchBlock(CatchClauseSyntax catchClause)
        {
            var block = catchClause.Block;

            if (block == null)
                return true;

            var statements = block.Statements;
            return statements.Count == 0;
        }

        /// <summary>
        ///     Determines if a catch clause has improper re-throw.
        /// </summary>
        private static bool HasImproperRethrow(CatchClauseSyntax catchClause)
        {
            var block = catchClause.Block;

            if (block == null)
                return false;

            // Look for throw statements in the catch block
            var throwStatements = block.DescendantNodes().OfType<ThrowStatementSyntax>();

            foreach (var throwStatement in throwStatements)
            {
                if (throwStatement.Expression != null)
                {
                    // throw ex; - improper re-throw
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        ///     Determines if an expression has string operations.
        /// </summary>
        private static bool HasStringOperations(ExpressionSyntax expression)
        {
            var expressionString = expression.ToString();

            // Check for common string operations in exception filters
            return expressionString.Contains(".Contains") ||
                   expressionString.Contains(".StartsWith") ||
                   expressionString.Contains(".EndsWith") ||
                   expressionString.Contains(".IndexOf") ||
                   expressionString.Contains("Message");
        }

        /// <summary>
        ///     Determines if a statement is a logging statement.
        /// </summary>
        private static bool IsLoggingStatement(StatementSyntax statement)
        {
            var statementString = statement.ToString().ToLowerInvariant();

            return statementString.Contains("log") ||
                   statementString.Contains("debug") ||
                   statementString.Contains("trace") ||
                   statementString.Contains("write");
        }
    }
}
