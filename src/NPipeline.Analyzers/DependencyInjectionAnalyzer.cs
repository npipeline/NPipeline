using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects dependency injection anti-patterns in node implementations.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class DependencyInjectionAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic identifier for the dependency injection anti-pattern warning.
    /// </summary>
    public const string DependencyInjectionAntiPatternId = "NP9401";

    private static readonly DiagnosticDescriptor DependencyInjectionAntiPatternRule = new(
        DependencyInjectionAntiPatternId,
        "Dependency injection anti-pattern detected",
        "Avoid dependency injection anti-patterns in node implementations. Use constructor injection instead.",
        "Best Practice",
        DiagnosticSeverity.Warning,
        true,
        "Node implementations should use constructor injection for dependencies instead of direct instantiation, static singletons, or service locator pattern.",
        "https://github.com/YellowCanaryOperations/NPipeline/blob/main/docs/architecture/dependency-injection.md");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [DependencyInjectionAntiPatternRule];

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

        // Check if this class is a node implementation
        var semanticModel = context.SemanticModel;
        var classSymbol = semanticModel.GetDeclaredSymbol(classDeclaration);

        if (classSymbol == null || !IsNodeImplementation(classSymbol))
            return;

        // Walk through the class to find dependency injection anti-patterns
        var walker = new DependencyInjectionWalker(semanticModel);
        walker.Visit(classDeclaration);

        // Report any anti-patterns found
        foreach (var (location, message) in walker.AntiPatterns)
        {
            var diagnostic = Diagnostic.Create(
                DependencyInjectionAntiPatternRule,
                location,
                message);

            context.ReportDiagnostic(diagnostic);
        }
    }

    /// <summary>
    ///     Determines if a type inherits from any of the node base types.
    /// </summary>
    private static bool IsNodeImplementation(INamedTypeSymbol typeSymbol)
    {
        // Check if type inherits from any of the node base types
        var nodeBaseTypes = new[]
        {
            "NPipeline.Nodes.TransformNode`2",
            "NPipeline.Nodes.SourceNode`1",
            "NPipeline.Nodes.SinkNode`1",
            "NPipeline.Nodes.INode",
            "NPipeline.Nodes.ITransformNode`2",
            "NPipeline.Nodes.ISourceNode`1",
            "NPipeline.Nodes.ISinkNode`1",
        };

        var currentType = typeSymbol;

        while (currentType != null)
        {
            var fullName = currentType.OriginalDefinition?.ToDisplayString() ?? currentType.ToDisplayString();

            if (nodeBaseTypes.Contains(fullName))
                return true;

            currentType = currentType.BaseType;
        }

        // Also check if type implements any of the node interfaces
        foreach (var interfaceType in typeSymbol.AllInterfaces)
        {
            var interfaceFullName = interfaceType.OriginalDefinition?.ToDisplayString() ?? interfaceType.ToDisplayString();

            if (nodeBaseTypes.Contains(interfaceFullName))
                return true;
        }

        return false;
    }

    /// <summary>
    ///     AST walker that detects dependency injection anti-patterns in node implementations.
    /// </summary>
    private sealed class DependencyInjectionWalker(SemanticModel semanticModel) : CSharpSyntaxWalker
    {
        private readonly List<(Location Location, string Message)> _antiPatterns = [];

        public IReadOnlyList<(Location Location, string Message)> AntiPatterns => _antiPatterns;

        public override void VisitObjectCreationExpression(ObjectCreationExpressionSyntax node)
        {
            // Get the type being instantiated
            var typeInfo = semanticModel.GetTypeInfo(node);
            var typeSymbol = typeInfo.Type;

            if (typeSymbol != null && IsService(typeSymbol))
                _antiPatterns.Add((node.GetLocation(), "Direct service instantiation detected"));

            base.VisitObjectCreationExpression(node);
        }

        public override void VisitAssignmentExpression(AssignmentExpressionSyntax node)
        {
            // Check if this is an assignment to a static field
            if (node.Left is MemberAccessExpressionSyntax memberAccess)
            {
                var symbolInfo = semanticModel.GetSymbolInfo(memberAccess);

                if (symbolInfo.Symbol is IFieldSymbol fieldSymbol && fieldSymbol.IsStatic)
                {
                    // Check if right side is an object creation
                    if (node.Right is ObjectCreationExpressionSyntax objectCreation)
                    {
                        var typeInfo = semanticModel.GetTypeInfo(objectCreation);
                        var typeSymbol = typeInfo.Type;

                        if (typeSymbol != null && IsService(typeSymbol))
                            _antiPatterns.Add((node.GetLocation(), "Static singleton assignment detected"));
                    }
                }
            }

            base.VisitAssignmentExpression(node);
        }

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            // Check for GetService or GetRequiredService calls
            if (node.Expression is MemberAccessExpressionSyntax memberAccessExpression)
            {
                var methodName = memberAccessExpression.Name.Identifier.Text;

                // Check for GetService or GetRequiredService calls
                if (methodName is "GetService" or "GetRequiredService")
                    _antiPatterns.Add((node.GetLocation(), "Service locator pattern detected"));
            }

            base.VisitInvocationExpression(node);
        }

        /// <summary>
        ///     Determines if a type is a service (not a DTO).
        /// </summary>
        private static bool IsService(ITypeSymbol typeSymbol)
        {
            // Check if it's a DTO (Data Transfer Object)
            if (IsDto(typeSymbol))
                return false;

            // Check if it's a service based on common patterns
            var containingNamespace = typeSymbol.ContainingNamespace?.ToDisplayString();

            // Types in Service, Repository, Provider, etc. namespaces are likely services
            if (containingNamespace != null &&
                (containingNamespace.Contains("Service") ||
                 containingNamespace.Contains("Repository") ||
                 containingNamespace.Contains("Provider") ||
                 containingNamespace.Contains("Handler") ||
                 containingNamespace.Contains("Manager")))
                return true;

            // Types with Service, Repository, etc. in the name are likely services
            var typeName = typeSymbol.Name;

            if (typeName.Contains("Service") ||
                typeName.Contains("Repository") ||
                typeName.Contains("Provider") ||
                typeName.Contains("Handler") ||
                typeName.Contains("Manager"))
                return true;

            // Types with methods (non-static) are likely services
            var members = typeSymbol.GetMembers();

            var hasMethods = members.Any(m =>
                m.Kind == SymbolKind.Method &&
                !m.IsStatic &&
                m.Name != ".ctor" &&
                m.Name != "ToString" &&
                m.Name != "Equals" &&
                m.Name != "GetHashCode");

            // If it has methods and is not a DTO, it's likely a service
            return hasMethods;
        }

        /// <summary>
        ///     Determines if a type is a DTO (Data Transfer Object).
        /// </summary>
        private static bool IsDto(ITypeSymbol typeSymbol)
        {
            // Records are often DTOs
            if (typeSymbol.IsRecord)
                return true;

            // Types with "Dto" or "Model" in the name are likely DTOs
            var typeName = typeSymbol.Name;

            if (typeName.Contains("Dto") || typeName.Contains("Model"))
                return true;

            // Types in Model, Dto, etc. namespaces are likely DTOs
            var containingNamespace = typeSymbol.ContainingNamespace?.ToDisplayString();

            if (containingNamespace != null &&
                (containingNamespace.Contains("Model") ||
                 containingNamespace.Contains("Dto") ||
                 containingNamespace.Contains("ViewModel")))
                return true;

            // Types with only properties are likely DTOs
            var members = typeSymbol.GetMembers();

            var hasOnlyProperties = members.All(m =>
                m.Kind == SymbolKind.Property ||
                (m.Kind == SymbolKind.Method && m.IsStatic));

            // Value types are often DTOs
            return typeSymbol.IsValueType || typeSymbol.SpecialType != SpecialType.None || hasOnlyProperties;
        }
    }
}
