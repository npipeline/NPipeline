using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers
{

    /// <summary>
    ///     Analyzer that detects anonymous object creation in performance-critical NPipeline code that causes unnecessary GC pressure
    ///     and allocation overhead, particularly in high-throughput scenarios.
    /// </summary>
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public sealed class AnonymousObjectAllocationAnalyzer : DiagnosticAnalyzer
    {
        /// <summary>
        ///     Diagnostic ID for anonymous object allocation in hot paths.
        /// </summary>
        public const string AnonymousObjectAllocationId = "NP9203";

        private static readonly DiagnosticDescriptor Rule = new(
            AnonymousObjectAllocationId,
            "Anonymous object allocation detected in hot path",
            "Anonymous object allocation detected in hot path method '{0}'. This creates unnecessary GC pressure and allocation overhead. "
            + "Consider using value types, named types, or object pooling for better performance in high-frequency execution paths.",
            "Performance",
            DiagnosticSeverity.Warning,
            true,
            "Anonymous object allocations in hot paths create unnecessary GC pressure and allocation overhead. "
            + "In high-throughput NPipeline scenarios, use value types, named types, or object pooling instead of anonymous objects. "
            + "Anonymous objects are particularly problematic in loops, LINQ expressions, and async methods. "
            + "https://npipeline.dev/docs/performance/avoiding-anonymous-object-allocations.");

        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule];

        public override void Initialize(AnalysisContext context)
        {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();

            // Register to analyze anonymous object creation expressions
            context.RegisterSyntaxNodeAction(AnalyzeAnonymousObjectCreation, SyntaxKind.AnonymousObjectCreationExpression);
        }

        private static void AnalyzeAnonymousObjectCreation(SyntaxNodeAnalysisContext context)
        {
            if (context.Node is not AnonymousObjectCreationExpressionSyntax anonymousObject)
                return;

            var semanticModel = context.SemanticModel;

            // Check if this anonymous object creation is in a hot path context
            if (!IsInHotPathContext(anonymousObject, semanticModel, out var enclosingMethodName))
                return;

            // Report the diagnostic
            var diagnostic = Diagnostic.Create(
                Rule,
                anonymousObject.GetLocation(),
                enclosingMethodName);

            context.ReportDiagnostic(diagnostic);
        }

        /// <summary>
        ///     Determines if the anonymous object creation is in a hot path context.
        /// </summary>
        private static bool IsInHotPathContext(
            AnonymousObjectCreationExpressionSyntax anonymousObject,
            SemanticModel semanticModel,
            out string enclosingMethodName)
        {
            enclosingMethodName = "Unknown";

            // Find the enclosing method
            var enclosingMethod = anonymousObject.FirstAncestorOrSelf<MethodDeclarationSyntax>();

            if (enclosingMethod == null)
                return false;

            enclosingMethodName = enclosingMethod.Identifier.Text;

            // Check if this is a hot path method by name
            if (IsHotPathMethodByName(enclosingMethod))
                return true;

            // Check if this method is in a class that inherits from NPipeline node interfaces
            var isInNPipelineNode = IsInNPipelineNode(enclosingMethod, semanticModel);

            if (isInNPipelineNode)
                return true;

            // Check if this is an async method (potential hot path)
            if (IsAsyncMethod(enclosingMethod, semanticModel))
            {
                // Always flag async methods in NPipeline node classes
                if (isInNPipelineNode)
                    return true;

                // Also flag async methods if NPipeline namespaces are imported
                var compilationUnit = enclosingMethod.SyntaxTree.GetCompilationUnitRoot();

                var hasNPipelineImport = compilationUnit.Usings
                    .Any(u => u.Name?.ToString().Contains("NPipeline") == true);

                if (hasNPipelineImport)
                    return true;
            }

            // Check if the anonymous object is in a loop (especially problematic)
            if (IsInLoop(anonymousObject))
            {
                // For loops, we're more lenient - only flag if in NPipeline context or hot path method
                return isInNPipelineNode || IsHotPathMethodByName(enclosingMethod);
            }

            // Check if the anonymous object is in a LINQ expression
            if (IsInLinqExpression(anonymousObject))
            {
                // For LINQ, we're more lenient - only flag if in NPipeline context or hot path method
                return isInNPipelineNode || IsHotPathMethodByName(enclosingMethod);
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
        ///     Determines if the method is in a class that inherits from NPipeline node interfaces.
        /// </summary>
        private static bool IsInNPipelineNode(MethodDeclarationSyntax method, SemanticModel semanticModel)
        {
            // Get the containing class
            var classDeclaration = method.FirstAncestorOrSelf<ClassDeclarationSyntax>();

            if (classDeclaration == null)
                return false;

            // Get the class symbol
            var classSymbol = semanticModel.GetDeclaredSymbol(classDeclaration);

            if (classSymbol == null)
                return false;

            // Check if the class inherits from any NPipeline node interface
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
        ///     Determines if a syntax node is inside a loop.
        /// </summary>
        private static bool IsInLoop(SyntaxNode node)
        {
            var parent = node.Parent;

            while (parent != null)
            {
                if (parent is ForStatementSyntax or
                    ForEachStatementSyntax or
                    WhileStatementSyntax or
                    DoStatementSyntax)
                    return true;

                parent = parent.Parent;
            }

            return false;
        }

        /// <summary>
        ///     Determines if a syntax node is inside a LINQ expression.
        /// </summary>
        private static bool IsInLinqExpression(SyntaxNode node)
        {
            var parent = node.Parent;

            while (parent != null)
            {
                // Check if we're inside a LINQ method call
                if (parent is InvocationExpressionSyntax invocation)
                {
                    if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
                    {
                        var methodName = memberAccess.Name.Identifier.Text;

                        var linqMethods = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                    {
                        "Where", "Select", "SelectMany", "GroupBy", "OrderBy", "OrderByDescending",
                        "ThenBy", "ThenByDescending", "ToList", "ToArray", "ToDictionary", "ToHashSet",
                        "First", "FirstOrDefault", "Single", "SingleOrDefault", "Last", "LastOrDefault",
                        "Count", "LongCount", "Sum", "Average", "Min", "Max", "Aggregate", "Distinct",
                        "Union", "Intersect", "Except", "Concat", "Reverse", "Skip", "Take", "SkipWhile",
                        "TakeWhile", "Join", "GroupJoin", "Zip", "SequenceEqual", "All", "Any", "Contains",
                    };

                        if (linqMethods.Contains(methodName))
                            return true;
                    }
                }

                parent = parent.Parent;
            }

            return false;
        }
    }
}
