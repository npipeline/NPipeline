using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects node implementations without public parameterless constructors.
///     Nodes without parameterless constructors cannot benefit from the optimized compiled factory
///     and require additional configuration (DI or pre-configured instances).
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class NodeParameterlessConstructorAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic identifier for nodes without parameterless constructors.
    /// </summary>
    public const string MissingParameterlessConstructorId = "NP9403";

    /// <summary>
    ///     Diagnostic identifier for performance suggestion to add parameterless constructor.
    /// </summary>
    public const string PerformanceSuggestionId = "NP9108";

    private static readonly DiagnosticDescriptor MissingParameterlessConstructorRule = new(
        MissingParameterlessConstructorId,
        "Node missing public parameterless constructor",
        "Node '{0}' does not have a public parameterless constructor and requires DI or pre-configured instances",
        "Design & Architecture",
        DiagnosticSeverity.Warning,
        true,
        "Node implementations without parameterless constructors cannot use DefaultNodeFactory directly and require DIContainerNodeFactory or pre-configured instances. Consider adding a parameterless constructor or using dependency injection.",
        "https://github.com/ChrisJacques/NPipeline/blob/main/docs/architecture/node-instantiation.md");

    private static readonly DiagnosticDescriptor PerformanceSuggestionRule = new(
        PerformanceSuggestionId,
        "Add parameterless constructor for better performance",
        "Node '{0}' could benefit from a public parameterless constructor for optimized instantiation (200-300μs faster per pipeline run)",
        "Performance & Optimization",
        DiagnosticSeverity.Info,
        true,
        "Nodes with parameterless constructors use compiled expression delegates for 3-5x faster instantiation. Consider adding a parameterless constructor if the node doesn't require constructor dependencies.",
        "https://github.com/ChrisJacques/NPipeline/blob/main/docs/performance/node-instantiation.md");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [MissingParameterlessConstructorRule, PerformanceSuggestionRule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.EnableConcurrentExecution();
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);

        // Register to analyze class declarations for node implementations
        context.RegisterSyntaxNodeAction(AnalyzeClassDeclaration, SyntaxKind.ClassDeclaration);
    }

    private static void AnalyzeClassDeclaration(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not ClassDeclarationSyntax classDeclaration)
            return;

        // Skip abstract classes
        if (classDeclaration.Modifiers.Any(SyntaxKind.AbstractKeyword))
            return;

        // Get semantic information
        var semanticModel = context.SemanticModel;
        var classSymbol = semanticModel.GetDeclaredSymbol(classDeclaration);

        if (classSymbol == null || !IsNodeImplementation(classSymbol))
            return;

        // Check if the class has a public parameterless constructor
        var hasParameterlessConstructor = HasPublicParameterlessConstructor(classSymbol);

        var hasOtherConstructors = classSymbol.Constructors.Any(c =>
            !c.IsStatic &&
            !c.IsImplicitlyDeclared &&
            c.Parameters.Length > 0);

        if (!hasParameterlessConstructor && hasOtherConstructors)
        {
            // Class has constructors with parameters but no parameterless constructor
            // This is a warning because it won't work with DefaultNodeFactory
            var diagnostic = Diagnostic.Create(
                MissingParameterlessConstructorRule,
                classDeclaration.Identifier.GetLocation(),
                classSymbol.Name);

            context.ReportDiagnostic(diagnostic);
        }

        // Note: We don't warn about implicit parameterless constructors since they already
        // use the compiled factory optimization and work perfectly fine
    }

    /// <summary>
    ///     Determines if a type implements any node interface or inherits from a node base class.
    /// </summary>
    private static bool IsNodeImplementation(INamedTypeSymbol typeSymbol)
    {
        // Node interface and base class patterns
        var nodeTypePatterns = new[]
        {
            "NPipeline.Nodes.INode",
            "NPipeline.Nodes.ISourceNode",
            "NPipeline.Nodes.ITransformNode",
            "NPipeline.Nodes.ISinkNode",
            "NPipeline.Nodes.SourceNode",
            "NPipeline.Nodes.TransformNode",
            "NPipeline.Nodes.SinkNode",
        };

        // Check base types
        var currentType = typeSymbol;

        while (currentType != null)
        {
            var fullName = currentType.OriginalDefinition?.ToDisplayString() ?? currentType.ToDisplayString();

            // Check for match with node patterns
            if (nodeTypePatterns.Any(pattern => fullName.StartsWith(pattern, StringComparison.Ordinal)))
                return true;

            currentType = currentType.BaseType;
        }

        // Check interfaces
        foreach (var interfaceType in typeSymbol.AllInterfaces)
        {
            var interfaceFullName = interfaceType.OriginalDefinition?.ToDisplayString() ?? interfaceType.ToDisplayString();

            if (nodeTypePatterns.Any(pattern => interfaceFullName.StartsWith(pattern, StringComparison.Ordinal)))
                return true;
        }

        return false;
    }

    /// <summary>
    ///     Checks if a type has a public parameterless constructor (explicit or implicit).
    /// </summary>
    private static bool HasPublicParameterlessConstructor(INamedTypeSymbol typeSymbol)
    {
        return typeSymbol.Constructors.Any(c =>
            !c.IsStatic &&
            c.DeclaredAccessibility == Accessibility.Public &&
            c.Parameters.Length == 0);
    }
}
