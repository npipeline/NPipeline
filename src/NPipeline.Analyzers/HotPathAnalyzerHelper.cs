using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace NPipeline.Analyzers;

/// <summary>
///     Shared hot-path heuristics for performance analyzers.
/// </summary>
internal static class HotPathAnalyzerHelper
{
    private static readonly HashSet<string> HotPathMethodNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "ExecuteAsync", "TransformAsync", "ConsumeAsync", "OpenStream",
        "ProcessAsync", "RunAsync", "HandleAsync", "Execute", "Process", "Run", "Handle",
    };

    private static readonly string[] NodeInterfaces =
    [
        "INode", "ISourceNode", "ITransformNode", "ISinkNode", "IAggregateNode",
        "IJoinNode", "ICustomMergeNode", "ICustomMergeNodeUntyped",
    ];

    /// <summary>
    ///     Determines whether a syntax node is in a high-frequency execution path.
    /// </summary>
    public static bool IsInHotPathContext(SyntaxNode node, SemanticModel semanticModel, out string enclosingMethodName)
    {
        enclosingMethodName = "Unknown";

        var enclosingMethod = node.FirstAncestorOrSelf<MethodDeclarationSyntax>();

        if (enclosingMethod == null)
            return false;

        enclosingMethodName = enclosingMethod.Identifier.Text;

        if (IsHotPathMethodByName(enclosingMethod))
            return true;

        var isInNPipelineNode = IsInNPipelineNode(enclosingMethod, semanticModel);

        if (isInNPipelineNode)
            return true;

        if (IsAsyncMethod(enclosingMethod, semanticModel))
        {
            var compilationUnit = enclosingMethod.SyntaxTree.GetCompilationUnitRoot();
            var hasNPipelineImport = compilationUnit.Usings.Any(u => u.Name?.ToString().Contains("NPipeline") == true);

            if (hasNPipelineImport)
                return true;
        }

        return false;
    }

    /// <summary>
    ///     Determines if a method name matches common high-frequency execution patterns.
    /// </summary>
    public static bool IsHotPathMethodByName(MethodDeclarationSyntax method)
    {
        return HotPathMethodNames.Contains(method.Identifier.Text);
    }

    /// <summary>
    ///     Determines if a method is async by modifier or return type.
    /// </summary>
    public static bool IsAsyncMethod(MethodDeclarationSyntax method, SemanticModel semanticModel)
    {
        if (method.Modifiers.Any(m => m.IsKind(SyntaxKind.AsyncKeyword)))
            return true;

        var returnTypeSymbol = semanticModel.GetTypeInfo(method.ReturnType).Type;

        if (returnTypeSymbol == null)
            return false;

        if (returnTypeSymbol is INamedTypeSymbol namedReturn)
        {
            var baseName = namedReturn.OriginalDefinition?.Name ?? namedReturn.Name;

            if (string.Equals(baseName, "Task", StringComparison.Ordinal) ||
                string.Equals(baseName, "ValueTask", StringComparison.Ordinal))
                return true;
        }

        return string.Equals(returnTypeSymbol.Name, "Task", StringComparison.Ordinal) ||
               string.Equals(returnTypeSymbol.Name, "ValueTask", StringComparison.Ordinal);
    }

    /// <summary>
    ///     Determines if a method belongs to a class that implements NPipeline node interfaces.
    /// </summary>
    public static bool IsInNPipelineNode(MethodDeclarationSyntax method, SemanticModel semanticModel)
    {
        var classDeclaration = method.FirstAncestorOrSelf<ClassDeclarationSyntax>();

        if (classDeclaration == null)
            return false;

        var classSymbol = semanticModel.GetDeclaredSymbol(classDeclaration);

        if (classSymbol == null)
            return false;

        foreach (var interfaceName in NodeInterfaces)
        {
            if (ImplementsInterface(classSymbol, interfaceName))
                return true;
        }

        return false;
    }

    private static bool ImplementsInterface(INamedTypeSymbol typeSymbol, string interfaceName)
    {
        foreach (var interfaceSymbol in typeSymbol.AllInterfaces)
        {
            if (interfaceSymbol.Name == interfaceName)
                return true;
        }

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