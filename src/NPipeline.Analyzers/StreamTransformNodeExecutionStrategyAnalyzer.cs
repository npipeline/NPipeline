using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that verifies that classes implementing IStreamTransformNode also use an execution strategy
///     that implements IStreamExecutionStrategy. It warns if there's a mismatch between the node type and strategy type.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class StreamTransformNodeExecutionStrategyAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for IStreamTransformNode execution strategy mismatch.
    /// </summary>
    public const string StreamTransformNodeExecutionStrategyId = "NP9211";

    private static readonly DiagnosticDescriptor Rule = new(
        StreamTransformNodeExecutionStrategyId,
        "IStreamTransformNode should use IStreamExecutionStrategy",
        "IStreamTransformNode '{0}' uses an execution strategy that doesn't implement IStreamExecutionStrategy. Consider using a strategy that implements both IExecutionStrategy and IStreamExecutionStrategy for optimal performance.",
        "Design",
        DiagnosticSeverity.Warning,
        true,
        "IStreamTransformNode is designed to work with execution strategies that implement IStreamExecutionStrategy. "
        + "Using a regular IExecutionStrategy may result in suboptimal performance as it cannot take advantage of "
        + "stream-specific optimizations. Consider using BatchingExecutionStrategy, UnbatchingExecutionStrategy, or "
        + "creating a custom strategy that implements both interfaces. "
        + "https://npipeline.dev/docs/core-concepts/nodes/stream-transform-nodes.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterSyntaxNodeAction(AnalyzeClassDeclaration, SyntaxKind.ClassDeclaration);
    }

    private static void AnalyzeClassDeclaration(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not ClassDeclarationSyntax classDeclaration)
            return;

        var semanticModel = context.SemanticModel;
        var classSymbol = semanticModel.GetDeclaredSymbol(classDeclaration);

        if (classSymbol == null)
            return;

        // Check if the class implements IStreamTransformNode
        if (!ImplementsIStreamTransformNode(classSymbol))
            return;

        var compilation = semanticModel.Compilation;
        var streamStrategyInterface = compilation.GetTypeByMetadataName("NPipeline.Execution.IStreamExecutionStrategy");
        var executionStrategyInterface = compilation.GetTypeByMetadataName("NPipeline.Execution.IExecutionStrategy");

        if (streamStrategyInterface == null || executionStrategyInterface == null)
            return;

        CheckExecutionStrategyUsage(
            classDeclaration,
            classSymbol,
            context,
            streamStrategyInterface,
            executionStrategyInterface);
    }

    private static void CheckExecutionStrategyUsage(
        ClassDeclarationSyntax classDeclaration,
        INamedTypeSymbol classSymbol,
        SyntaxNodeAnalysisContext context,
        INamedTypeSymbol streamStrategyInterface,
        INamedTypeSymbol executionStrategyInterface)
    {
        var semanticModel = context.SemanticModel;

        foreach (var propertyDeclaration in classDeclaration.Members.OfType<PropertyDeclarationSyntax>())
        {
            if (propertyDeclaration.Identifier.Text != "ExecutionStrategy")
                continue;

            var initializer = propertyDeclaration.Initializer?.Value;

            if (initializer == null)
                continue;

            var assignedType = semanticModel.GetTypeInfo(initializer).Type;

            if (!ImplementsIStreamExecutionStrategy(assignedType, streamStrategyInterface))
                ReportDiagnostic(classSymbol.Name, initializer.GetLocation(), context);
        }

        foreach (var assignment in classDeclaration.DescendantNodes().OfType<AssignmentExpressionSyntax>())
        {
            if (!IsExecutionStrategyAssignment(assignment.Left, semanticModel, executionStrategyInterface))
                continue;

            var assignedType = semanticModel.GetTypeInfo(assignment.Right).Type;

            if (!ImplementsIStreamExecutionStrategy(assignedType, streamStrategyInterface))
                ReportDiagnostic(classSymbol.Name, assignment.Right.GetLocation(), context);
        }
    }

    /// <summary>
    ///     Checks if a type implements IStreamTransformNode.
    /// </summary>
    private static bool ImplementsIStreamTransformNode(INamedTypeSymbol typeSymbol)
    {
        return typeSymbol.AllInterfaces.Any(i =>
            i.Name == "IStreamTransformNode" &&
            i.ContainingNamespace?.ToDisplayString() == "NPipeline.Nodes");
    }

    private static bool ImplementsIStreamExecutionStrategy(ITypeSymbol? typeSymbol, INamedTypeSymbol streamStrategyInterface)
    {
        if (typeSymbol == null)
            return false;

        if (SymbolEqualityComparer.Default.Equals(typeSymbol, streamStrategyInterface))
            return true;

        if (typeSymbol is INamedTypeSymbol namedType)
            return namedType.AllInterfaces.Any(i => SymbolEqualityComparer.Default.Equals(i, streamStrategyInterface));

        return false;
    }

    private static bool IsExecutionStrategyAssignment(
        ExpressionSyntax left,
        SemanticModel semanticModel,
        INamedTypeSymbol executionStrategyInterface)
    {
        var symbol = semanticModel.GetSymbolInfo(left).Symbol;

        if (symbol is not IPropertySymbol property)
            return false;

        if (property.Name != "ExecutionStrategy")
            return false;

        var propertyType = property.Type;

        if (SymbolEqualityComparer.Default.Equals(propertyType, executionStrategyInterface))
            return true;

        return propertyType.AllInterfaces.Any(i => SymbolEqualityComparer.Default.Equals(i, executionStrategyInterface));
    }

    /// <summary>
    ///     Reports the diagnostic for execution strategy mismatch.
    /// </summary>
    private static void ReportDiagnostic(string className, Location location, SyntaxNodeAnalysisContext context)
    {
        var diagnostic = Diagnostic.Create(
            Rule,
            location,
            className);

        context.ReportDiagnostic(diagnostic);
    }
}
