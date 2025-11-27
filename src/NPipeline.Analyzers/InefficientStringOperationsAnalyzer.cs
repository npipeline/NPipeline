using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects inefficient string operations that cause excessive allocations and GC pressure
///     in performance-critical NPipeline code, particularly in high-throughput scenarios.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class InefficientStringOperationsAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for inefficient string operations.
    /// </summary>
    public const string DiagnosticId = "NP9206";

    private static readonly DiagnosticDescriptor Rule = new(
        DiagnosticId,
        "Inefficient string operation detected",
        "String operation '{0}' creates unnecessary allocations: {1}",
        "Performance",
        DiagnosticSeverity.Warning,
        true,
        "String operations in hot paths can cause significant memory pressure and performance degradation. "
        + "Use StringBuilder for concatenation, string interpolation for formatting, and cached strings for repeated values. "
        + "Avoid string concatenation with '+' in loops and hot path methods. "
        + "https://npipeline.dev/docs/performance/efficient-string-operations.");

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule];

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze binary expressions for string concatenation
        context.RegisterSyntaxNodeAction(AnalyzeBinaryExpression, SyntaxKind.AddExpression);

        // Register to analyze interpolated string expressions
        context.RegisterSyntaxNodeAction(AnalyzeInterpolatedStringExpression, SyntaxKind.InterpolatedStringExpression);

        // Register to analyze invocation expressions for string methods
        context.RegisterSyntaxNodeAction(AnalyzeInvocationExpression, SyntaxKind.InvocationExpression);
    }

    private static void AnalyzeBinaryExpression(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not BinaryExpressionSyntax binaryExpr)
            return;

        var semanticModel = context.SemanticModel;

        // Check if this is a string concatenation
        if (!IsStringConcatenation(binaryExpr, semanticModel))
            return;

        // Check if this concatenation is in a hot path context
        if (!IsInHotPathContext(binaryExpr, semanticModel, out _))
            return;

        // Check if this is in a loop (especially problematic)
        var isInLoop = IsInLoop(binaryExpr);

        // Report diagnostic with appropriate message
        var operationType = isInLoop
            ? "String concatenation in loop"
            : "String concatenation with '+'";

        var diagnostic = Diagnostic.Create(
            Rule,
            binaryExpr.OperatorToken.GetLocation(),
            operationType,
            isInLoop
                ? "Use StringBuilder for string concatenation in loops"
                : "Consider using string interpolation or StringBuilder");

        context.ReportDiagnostic(diagnostic);
    }

    private static void AnalyzeInterpolatedStringExpression(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not InterpolatedStringExpressionSyntax interpolatedString)
            return;

        var semanticModel = context.SemanticModel;

        // Check if this interpolated string is in a hot path context
        if (!IsInHotPathContext(interpolatedString, semanticModel, out _))
            return;

        // Check for complex interpolated strings that might be inefficient
        if (IsComplexInterpolatedString(interpolatedString))
        {
            var diagnostic = Diagnostic.Create(
                Rule,
                interpolatedString.GetLocation(),
                "Complex interpolated string",
                "Consider using StringBuilder for complex string formatting");

            context.ReportDiagnostic(diagnostic);
        }
    }

    private static void AnalyzeInvocationExpression(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not InvocationExpressionSyntax invocation)
            return;

        var semanticModel = context.SemanticModel;

        // Check if this is a string method call
        if (!IsStringMethodCall(invocation, semanticModel, out var methodName))
            return;

        // Check if this string method is in a hot path context
        if (!IsInHotPathContext(invocation, semanticModel, out _))
            return;

        // Check if this is an inefficient string method
        if (IsInefficientStringMethod(methodName, invocation))
        {
            var suggestion = GetSuggestionForMethod(methodName);

            var diagnostic = Diagnostic.Create(
                Rule,
                invocation.GetLocation(),
                $"String.{methodName}",
                suggestion);

            context.ReportDiagnostic(diagnostic);
        }
    }

    /// <summary>
    ///     Determines if a binary expression is a string concatenation.
    /// </summary>
    private static bool IsStringConcatenation(BinaryExpressionSyntax binaryExpr, SemanticModel semanticModel)
    {
        // Check if the operator is '+'
        if (!binaryExpr.OperatorToken.IsKind(SyntaxKind.PlusToken))
            return false;

        // Check if either operand is a string type
        var leftType = semanticModel.GetTypeInfo(binaryExpr.Left).Type;
        var rightType = semanticModel.GetTypeInfo(binaryExpr.Right).Type;

        return leftType?.SpecialType == SpecialType.System_String ||
               rightType?.SpecialType == SpecialType.System_String;
    }

    /// <summary>
    ///     Determines if a syntax node is in a hot path context.
    /// </summary>
    private static bool IsInHotPathContext(SyntaxNode node, SemanticModel semanticModel, out string enclosingMethodName)
    {
        enclosingMethodName = "Unknown";

        // Find enclosing method
        var enclosingMethod = node.FirstAncestorOrSelf<MethodDeclarationSyntax>();

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
    ///     Determines if an interpolated string is complex enough to be inefficient.
    /// </summary>
    private static bool IsComplexInterpolatedString(InterpolatedStringExpressionSyntax interpolatedString)
    {
        var contents = interpolatedString.Contents;

        // Consider it complex if it has more than 3 interpolations or method calls
        var interpolationCount = 0;
        var hasMethodCalls = false;

        foreach (var content in contents)
        {
            if (content is InterpolationSyntax interpolation)
            {
                interpolationCount++;

                // Check for method calls in interpolation
                if (interpolation.Expression is InvocationExpressionSyntax)
                    hasMethodCalls = true;
            }
        }

        return interpolationCount > 3 || (interpolationCount > 1 && hasMethodCalls);
    }

    /// <summary>
    ///     Determines if an invocation is a string method call.
    /// </summary>
    private static bool IsStringMethodCall(InvocationExpressionSyntax invocation, SemanticModel semanticModel, out string methodName)
    {
        methodName = string.Empty;

        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return false;

        methodName = memberAccess.Name.Identifier.Text;

        // Check if this is a method on a string type
        var expressionType = semanticModel.GetTypeInfo(memberAccess.Expression).Type;

        if (expressionType?.SpecialType != SpecialType.System_String)
            return false;

        // Check for static string methods
        if (memberAccess.Expression is IdentifierNameSyntax identifier && identifier.Identifier.Text == "string")
            return true;

        return expressionType?.SpecialType == SpecialType.System_String;
    }

    /// <summary>
    ///     Determines if a string method is inefficient in hot paths.
    /// </summary>
    private static bool IsInefficientStringMethod(string methodName, InvocationExpressionSyntax invocation)
    {
        return methodName switch
        {
            "Concat" or "Join" when HasMultipleParameters(invocation) => true,
            "Format" when HasMultipleParameters(invocation) => true,
            "Substring" when CouldUseSpan(methodName) => true,
            "Replace" when CouldUseSpan(methodName) => true,
            "Split" when CouldUseSpan(methodName) => true,
            "PadLeft" or "PadRight" when InHotPath(invocation) => true,
            "Trim" when InHotPath(invocation) => true,
            "ToUpper" or "ToLower" when InHotPath(invocation) => true,
            _ => false,
        };
    }

    /// <summary>
    ///     Gets suggestion for improving a specific string method.
    /// </summary>
    private static string GetSuggestionForMethod(string methodName)
    {
        return methodName switch
        {
            "Concat" => "Consider using StringBuilder or string interpolation",
            "Join" => "Consider using string.Join with span-based operations",
            "Format" => "Consider using interpolated strings or cached format strings",
            "Substring" => "Consider using AsSpan().Slice() for zero-allocation substring",
            "Replace" => "Consider using Replace() with ReadOnlySpan for better performance",
            "Split" => "Consider using Span-based splitting methods",
            "PadLeft" or "PadRight" => "Consider using StringBuilder for complex padding",
            "Trim" => "Consider using AsSpan().Trim() for zero-allocation trimming",
            "ToUpper" or "ToLower" => "Consider using string.Create with Span for better performance",
            _ => "Consider using span-based string operations",
        };
    }

    /// <summary>
    ///     Checks if an invocation has multiple parameters (indicating potential inefficiency).
    /// </summary>
    private static bool HasMultipleParameters(InvocationExpressionSyntax invocation)
    {
        return invocation.ArgumentList?.Arguments.Count > 2;
    }

    /// <summary>
    ///     Determines if a string operation could benefit from span-based alternatives.
    /// </summary>
    private static bool CouldUseSpan(string methodName)
    {
        return methodName is "Substring" or "Replace" or "Split" or "Trim";
    }

    /// <summary>
    ///     Checks if an invocation is in a hot path (simple heuristic).
    /// </summary>
    private static bool InHotPath(InvocationExpressionSyntax invocation)
    {
        // Check if in a loop
        return IsInLoop(invocation);
    }
}
