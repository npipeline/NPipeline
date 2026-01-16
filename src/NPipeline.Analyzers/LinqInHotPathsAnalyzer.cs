using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects LINQ operations in high-frequency execution paths that cause unnecessary allocations
///     and GC pressure, significantly impacting performance in high-throughput NPipeline scenarios.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class LinqInHotPathsAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for LINQ operations in hot paths.
    /// </summary>
    public const string LinqInHotPathsId = "NP9103";

    private static readonly DiagnosticDescriptor Rule = new(
        LinqInHotPathsId,
        "LINQ operation detected in hot path",
        "LINQ operation '{0}' detected in hot path method '{1}'. This creates unnecessary allocations and GC pressure. "
        + "Consider using imperative alternatives (foreach, for loops) for better performance in high-frequency execution paths.",
        "Performance & Optimization",
        DiagnosticSeverity.Warning,
        true,
        "LINQ operations in hot paths create unnecessary allocations and GC pressure. "
        + "In high-throughput NPipeline scenarios, use imperative alternatives like foreach/for loops instead of LINQ. "
        + "Common LINQ methods to avoid in hot paths: Where, Select, GroupBy, OrderBy, ToList, ToArray, First, Single, etc. "
        + "https://npipeline.dev/docs/performance/avoiding-linq-in-hot-paths.");

    private static readonly ImmutableHashSet<string> LinqMethodNames =
        ImmutableHashSet.Create(StringComparer.Ordinal,
            "Where", "Select", "SelectMany", "GroupBy", "OrderBy", "OrderByDescending",
            "ThenBy", "ThenByDescending", "ToList", "ToArray", "ToDictionary", "ToHashSet",
            "First", "FirstOrDefault", "Single", "SingleOrDefault", "Last", "LastOrDefault",
            "Count", "LongCount", "Sum", "Average", "Min", "Max", "Aggregate", "Distinct",
            "Union", "Intersect", "Except", "Concat", "Reverse", "Skip", "Take", "SkipWhile",
            "TakeWhile", "Join", "GroupJoin", "Zip", "SequenceEqual", "All", "Any", "Contains");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze invocation expressions for LINQ methods
        context.RegisterSyntaxNodeAction(AnalyzeInvocationExpression, SyntaxKind.InvocationExpression);
    }

    private static void AnalyzeInvocationExpression(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not InvocationExpressionSyntax invocation)
            return;

        var methodName = GetMethodName(invocation);
        var semanticModel = context.SemanticModel;

        // Check if this is a LINQ method call
        var isLinq = IsLinqMethod(invocation, semanticModel);

        if (!isLinq)
            return;

        // Get the method name for reporting
        methodName = GetMethodName(invocation);

        // Detected a LINQ call - diagnostic will be reported below.

        // Check if this LINQ call is in a hot path context
        var isInHotPath = IsInHotPathContext(invocation, semanticModel, out var enclosingMethodName);

        if (!isInHotPath)
            return;

        // Report the diagnostic
        var diagnostic = Diagnostic.Create(
            Rule,
            invocation.GetLocation(),
            methodName,
            enclosingMethodName);

        context.ReportDiagnostic(diagnostic);
    }

    /// <summary>
    ///     Determines if an invocation represents a LINQ method call.
    /// </summary>
    private static bool IsLinqMethod(InvocationExpressionSyntax invocation, SemanticModel semanticModel)
    {
        // Quick name check
        var methodName = GetMethodName(invocation);

        if (!LinqMethodNames.Contains(methodName))
            return false;

        // Prefer resolving the symbol for the invocation itself. This handles reduced extension methods
        // and regular method bindings.
        IMethodSymbol? methodSymbol = null;

        var invocationSymbolInfo = semanticModel.GetSymbolInfo(invocation);

        methodSymbol = invocationSymbolInfo.Symbol as IMethodSymbol
                       ?? invocationSymbolInfo.CandidateSymbols.OfType<IMethodSymbol>().FirstOrDefault();

        // If not found, try resolving the expression (member access) or the name identifier
        if (methodSymbol == null && invocation.Expression is MemberAccessExpressionSyntax memberAccess)
        {
            var maSymbolInfo = semanticModel.GetSymbolInfo(memberAccess);

            methodSymbol = maSymbolInfo.Symbol as IMethodSymbol
                           ?? maSymbolInfo.CandidateSymbols.OfType<IMethodSymbol>().FirstOrDefault();

            if (methodSymbol == null)
            {
                var nameSymbolInfo = semanticModel.GetSymbolInfo(memberAccess.Name);

                methodSymbol = nameSymbolInfo.Symbol as IMethodSymbol
                               ?? nameSymbolInfo.CandidateSymbols.OfType<IMethodSymbol>().FirstOrDefault();
            }
        }

        if (methodSymbol == null && invocation.Expression is IdentifierNameSyntax idName)
        {
            var idSymbolInfo = semanticModel.GetSymbolInfo(idName);

            methodSymbol = idSymbolInfo.Symbol as IMethodSymbol
                           ?? idSymbolInfo.CandidateSymbols.OfType<IMethodSymbol>().FirstOrDefault();
        }

        if (methodSymbol != null)
        {
            var impl = methodSymbol.ReducedFrom ?? methodSymbol;
            var containingType = impl.ContainingType;
            var containingNamespace = containingType?.ContainingNamespace?.ToDisplayString();

            if (!string.IsNullOrEmpty(containingNamespace) &&
                containingNamespace != null &&
                containingNamespace.StartsWith("System.Linq", StringComparison.Ordinal))
            {
                var containingTypeFull = impl.ContainingType?.ToDisplayString();

                if (string.Equals(containingTypeFull, "System.Linq.Enumerable", StringComparison.Ordinal) ||
                    string.Equals(containingTypeFull, "System.Linq.Queryable", StringComparison.Ordinal))
                    return true;
            }

            // Resolved symbol but not from System.Linq -> not a LINQ call

            // Special-case: Array methods like System.Array.Reverse are often used like LINQ Reverse
            // Treat Reverse on arrays or IEnumerable as LINQ-like for the purposes of this analyzer.
            try
            {
                if (string.Equals(methodName, "Reverse", StringComparison.Ordinal) &&
                    impl.ContainingType != null)
                {
                    // attempt to check receiver type syntactically
                    ExpressionSyntax? recvExpr = null;

                    if (invocation.Expression is MemberAccessExpressionSyntax recvAccess)
                        recvExpr = recvAccess.Expression;
                    else if (invocation.Expression is MemberBindingExpressionSyntax binding && binding.Parent is ConditionalAccessExpressionSyntax cae)
                        recvExpr = cae.Expression;

                    if (recvExpr != null)
                    {
                        var rType = semanticModel.GetTypeInfo(recvExpr).Type;

                        if (rType != null && (rType.TypeKind == TypeKind.Array || ImplementsEnumerable(rType)))
                            return true;
                    }
                }
            }
            catch
            {
            }

            return false;
        }

        // Handle conditional access/member-binding patterns: e.g. obj?.Where(...)
        ExpressionSyntax? receiverExpr = null;

        if (invocation.Expression is MemberAccessExpressionSyntax receiverMemberAccess)
            receiverExpr = receiverMemberAccess.Expression;
        else if (invocation.Expression is MemberBindingExpressionSyntax memberBinding && memberBinding.Parent is ConditionalAccessExpressionSyntax cae)
            receiverExpr = cae.Expression;

        if (receiverExpr == null)
        {
            // No receiver expression; conservative false
            return false;
        }

        var receiverType = semanticModel.GetTypeInfo(receiverExpr).Type;

        if (receiverType == null)
        {
            // Syntactic fallback: if the invocation is a member access on some expression (chained call or identifier),
            // conservatively treat it as LINQ when the method name matches known LINQ methods.
            if (invocation.Expression is MemberAccessExpressionSyntax)
                return true;

            return false;
        }

        // Arrays are clearly LINQ targets for extension methods like Reverse/ToList
        if (receiverType.TypeKind == TypeKind.Array)
            return true;

        // Check if the type itself or any implemented interface is IEnumerable<T> or non-generic IEnumerable
        bool ImplementsEnumerable(ITypeSymbol t)
        {
            var compilation = semanticModel.Compilation;

            var genericEnumerable = compilation.GetTypeByMetadataName("System.Collections.Generic.IEnumerable`1");
            var nonGenericEnumerable = compilation.GetTypeByMetadataName("System.Collections.IEnumerable");

            // Direct equality checks (handles unbound generic types and arrays)
            if (nonGenericEnumerable != null && SymbolEqualityComparer.Default.Equals(t, nonGenericEnumerable))
                return true;

            if (genericEnumerable != null)
            {
                if (t is INamedTypeSymbol named && SymbolEqualityComparer.Default.Equals(named.OriginalDefinition, genericEnumerable))
                    return true;
            }

            // Check implemented interfaces

            foreach (var iface in t.AllInterfaces)
            {
                if (nonGenericEnumerable != null && SymbolEqualityComparer.Default.Equals(iface, nonGenericEnumerable))
                    return true;

                if (genericEnumerable != null && SymbolEqualityComparer.Default.Equals(iface.OriginalDefinition, genericEnumerable))
                    return true;
            }

            // Fallback: check by fully-qualified display string for common enumerable forms
            try
            {
                var displayFully = t.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

                if (displayFully == "global::System.Collections.IEnumerable" ||
                    displayFully.IndexOf("System.Collections.Generic.IEnumerable<", StringComparison.Ordinal) >= 0)
                    return true;
            }
            catch
            {
            }

            // Fallback: check by simple name and namespace to handle symbol identity edge-cases
            try
            {
                var name = t.Name;
                var ns = t.ContainingNamespace?.ToDisplayString();

                if (string.Equals(name, "IEnumerable", StringComparison.Ordinal) &&
                    (string.Equals(ns, "System.Collections", StringComparison.Ordinal) ||
                     string.Equals(ns, "System.Collections.Generic", StringComparison.Ordinal)))
                    return true;
            }
            catch
            {
            }

            // Additional fallback: look for common collection type names in the display string
            try
            {
                var disp = t.ToDisplayString();

                if (!string.IsNullOrEmpty(disp) && (
                        disp.IndexOf("List<", StringComparison.Ordinal) >= 0 ||
                        disp.IndexOf("IList<", StringComparison.Ordinal) >= 0 ||
                        disp.IndexOf("IReadOnlyList<", StringComparison.Ordinal) >= 0 ||
                        disp.IndexOf("IReadOnlyCollection<", StringComparison.Ordinal) >= 0 ||
                        disp.IndexOf("IEnumerable<", StringComparison.Ordinal) >= 0 ||
                        disp.IndexOf("System.Collections.Generic.List<", StringComparison.Ordinal) >= 0))
                    return true;
            }
            catch
            {
            }

            return false;
        }

        if (ImplementsEnumerable(receiverType))
            return true;

        // Last-resort fallback: if the type's display string contains 'IEnumerable', treat as enumerable.
        try
        {
            var ds = receiverType.ToDisplayString();

            if (!string.IsNullOrEmpty(ds) && ds.IndexOf("IEnumerable", StringComparison.Ordinal) >= 0)
                return true;
        }
        catch
        {
        }

        return false;
    }

    /// <summary>
    ///     Gets the method name from an invocation expression.
    /// </summary>
    private static string GetMethodName(InvocationExpressionSyntax invocation)
    {
        if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
            return memberAccess.Name.Identifier.Text;

        // Handle cases like static method invocations or chained invocations
        if (invocation.Expression is IdentifierNameSyntax id)
            return id.Identifier.Text;

        return "Unknown";
    }

    /// <summary>
    ///     Determines if the LINQ call is in a hot path context.
    /// </summary>
    private static bool IsInHotPathContext(
        InvocationExpressionSyntax invocation,
        SemanticModel semanticModel,
        out string enclosingMethodName)
    {
        enclosingMethodName = "Unknown";

        // Find the enclosing method
        var enclosingMethod = invocation.FirstAncestorOrSelf<MethodDeclarationSyntax>();

        if (enclosingMethod == null)
            return false;

        enclosingMethodName = enclosingMethod.Identifier.Text;

        // Check if this is a hot path method by name
        var byName = IsHotPathMethodByName(enclosingMethod);

        if (byName)
            return true;

        // Check if this method is in a class that inherits from NPipeline node interfaces
        var isInNPipelineNode = IsInNPipelineNode(enclosingMethod, semanticModel);

        if (isInNPipelineNode)
            return true;

        // Check if this is an async method (potential hot path)
        // Flag async methods if they're in NPipeline node classes OR if NPipeline namespaces are imported
        if (IsAsyncMethod(enclosingMethod, semanticModel))
        {
            // Always flag async methods in NPipeline node classes
            if (isInNPipelineNode)
                return true;

            // Also flag async methods if NPipeline namespaces are imported (likely NPipeline-related code)
            var compilationUnit = enclosingMethod.SyntaxTree.GetCompilationUnitRoot();

            var hasNPipelineImport = compilationUnit.Usings
                .Any(u => u.Name?.ToString().Contains("NPipeline") == true);

            if (hasNPipelineImport)
                return true;
        }

        // For non-NPipeline nodes, only consider explicitly named hot path methods
        // This prevents false positives on sync methods in regular classes
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

        if (returnTypeSymbol == null)
            return false;

        // Handle named types like Task<T> and ValueTask<T>
        if (returnTypeSymbol is INamedTypeSymbol namedReturn)
        {
            var baseName = namedReturn.OriginalDefinition?.Name ?? namedReturn.Name;

            if (string.Equals(baseName, "Task", StringComparison.Ordinal) ||
                string.Equals(baseName, "ValueTask", StringComparison.Ordinal))
                return true;
        }

        // Fallback: check the simple name
        return string.Equals(returnTypeSymbol.Name, "Task", StringComparison.Ordinal) ||
               string.Equals(returnTypeSymbol.Name, "ValueTask", StringComparison.Ordinal);
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
}
