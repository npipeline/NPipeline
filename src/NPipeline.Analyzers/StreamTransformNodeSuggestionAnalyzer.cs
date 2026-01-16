using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects when a class implements ITransformNode but its ExecuteAsync method returns IAsyncEnumerable&lt;T&gt;.
///     It suggests using IStreamTransformNode instead for better interface segregation.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class StreamTransformNodeSuggestionAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for suggesting IStreamTransformNode.
    /// </summary>
    public const string StreamTransformNodeSuggestionId = "NP9401";

    private static readonly DiagnosticDescriptor Rule = new(
        StreamTransformNodeSuggestionId,
        "Consider using IStreamTransformNode for stream-based transformations",
        "Class '{0}' implements ITransformNode but ExecuteAsync returns IAsyncEnumerable<{1}>. Consider implementing IStreamTransformNode<{2}, {1}> instead for better interface segregation.",
        "Design & Architecture",
        DiagnosticSeverity.Info,
        true,
        "When ExecuteAsync returns IAsyncEnumerable, node is performing stream-based transformations. "
        + "IStreamTransformNode is designed specifically for this use case and provides better interface segregation. "
        + "Using IStreamTransformNode makes intent clearer and allows for more optimized execution strategies. "
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

        var transformNodeInterface = GetTransformNodeInterface(classSymbol);

        if (transformNodeInterface is null)
            return;

        if (ImplementsIStreamTransformNode(classSymbol))
            return;

        var asyncEnumerableSymbol = context.SemanticModel.Compilation.GetTypeByMetadataName("System.Collections.Generic.IAsyncEnumerable`1");

        if (asyncEnumerableSymbol is null)
            return;

        var inputType = GetInputTypeFromITransformNode(transformNodeInterface);
        var outputType = GetOutputTypeFromITransformNode(transformNodeInterface);

        if (inputType is null || outputType is null)
            return;

        if (!ReturnsIAsyncEnumerable(outputType, asyncEnumerableSymbol, out var elementType))
            return;

        var diagnostic = Diagnostic.Create(
            Rule,
            classDeclaration.Identifier.GetLocation(),
            classSymbol.Name,
            elementType.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat),
            inputType.ToDisplayString(SymbolDisplayFormat.MinimallyQualifiedFormat));

        context.ReportDiagnostic(diagnostic);
    }

    private static INamedTypeSymbol? GetTransformNodeInterface(INamedTypeSymbol typeSymbol)
    {
        return typeSymbol.AllInterfaces.FirstOrDefault(i =>
            i.Name == "ITransformNode" &&
            i.ContainingNamespace?.ToDisplayString() == "NPipeline.Nodes");
    }

    private static bool ImplementsIStreamTransformNode(INamedTypeSymbol typeSymbol)
    {
        return typeSymbol.AllInterfaces.Any(i =>
            i.Name == "IStreamTransformNode" &&
            i.ContainingNamespace?.ToDisplayString() == "NPipeline.Nodes");
    }

    private static bool ReturnsIAsyncEnumerable(
        ITypeSymbol candidate,
        INamedTypeSymbol asyncEnumerableSymbol,
        out ITypeSymbol elementType)
    {
        elementType = null!;

        if (candidate is INamedTypeSymbol namedType)
        {
            if (SymbolEqualityComparer.Default.Equals(namedType.ConstructedFrom, asyncEnumerableSymbol))
            {
                elementType = namedType.TypeArguments[0];
                return true;
            }

            if (IsTaskLike(namedType, "System.Threading.Tasks.Task") ||
                IsTaskLike(namedType, "System.Threading.Tasks.ValueTask"))
            {
                if (namedType.TypeArguments.Length == 1 &&
                    namedType.TypeArguments[0] is ITypeSymbol wrapped &&
                    ReturnsIAsyncEnumerable(wrapped, asyncEnumerableSymbol, out elementType))
                    return true;
            }
        }

        return false;
    }

    private static ITypeSymbol? GetInputTypeFromITransformNode(INamedTypeSymbol transformNodeInterface)
    {
        return transformNodeInterface.TypeArguments.Length == 2
            ? transformNodeInterface.TypeArguments[0]
            : null;
    }

    private static ITypeSymbol? GetOutputTypeFromITransformNode(INamedTypeSymbol transformNodeInterface)
    {
        return transformNodeInterface.TypeArguments.Length == 2
            ? transformNodeInterface.TypeArguments[1]
            : null;
    }

    private static bool IsTaskLike(INamedTypeSymbol namedType, string metadataName)
    {
        return namedType.ConstructedFrom?.ToDisplayString() == metadataName;
    }
}
