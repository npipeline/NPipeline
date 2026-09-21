using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that verifies that a class implementing IStreamTransformNode and supplying a default execution
///     strategy through IExecutionStrategyProvider supplies one that implements IStreamExecutionStrategy.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class StreamTransformNodeExecutionStrategyAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for IStreamTransformNode execution strategy mismatch.
    /// </summary>
    public const string StreamTransformNodeExecutionStrategyId = "NP9402";

    private static readonly DiagnosticDescriptor Rule = new(
        StreamTransformNodeExecutionStrategyId,
        "IStreamTransformNode should use IStreamExecutionStrategy",
        "IStreamTransformNode '{0}' supplies a default execution strategy that doesn't implement IStreamExecutionStrategy. A stream transform cannot run under a per-item strategy.",
        "Design & Architecture",
        DiagnosticSeverity.Warning,
        true,
        "IStreamTransformNode is designed to work with execution strategies that implement IStreamExecutionStrategy. "
        + "Using a regular IExecutionStrategy may result in suboptimal performance as it cannot take advantage of "
        + "stream-specific optimizations. Consider using BatchingExecutionStrategy, UnbatchingExecutionStrategy, or "
        + "creating a custom strategy that implements both interfaces.");

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

        if (streamStrategyInterface == null)
            return;

        CheckExecutionStrategyUsage(
            classDeclaration,
            classSymbol,
            context,
            streamStrategyInterface);
    }

    private static void CheckExecutionStrategyUsage(
        ClassDeclarationSyntax classDeclaration,
        INamedTypeSymbol classSymbol,
        SyntaxNodeAnalysisContext context,
        INamedTypeSymbol streamStrategyInterface)
    {
        var semanticModel = context.SemanticModel;

        foreach (var propertyDeclaration in classDeclaration.Members.OfType<PropertyDeclarationSyntax>())
        {
            if (propertyDeclaration.Identifier.Text != "DefaultExecutionStrategy")
                continue;

            // The strategy is supplied either as a property initializer or as an expression body.
            var supplied = propertyDeclaration.Initializer?.Value ?? propertyDeclaration.ExpressionBody?.Expression;

            if (supplied == null)
                continue;

            var suppliedType = semanticModel.GetTypeInfo(supplied).Type;

            if (!ImplementsIStreamExecutionStrategy(suppliedType, streamStrategyInterface))
                ReportDiagnostic(classSymbol.Name, supplied.GetLocation(), context);
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
