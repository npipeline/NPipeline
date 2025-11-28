using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using NPipeline.Analyzers;

namespace NPipeline.CodeFixes;

/// <summary>
///     Code fix provider that suggests complete resilience configuration for node restart functionality.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(ResilientExecutionConfigurationCodeFixProvider))]
[Shared]
public sealed class ResilientExecutionConfigurationCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [ResilientExecutionConfigurationAnalyzer.IncompleteResilientConfigurationId];

    /// <inheritdoc />
    public override FixAllProvider GetFixAllProvider()
    {
        return WellKnownFixAllProviders.BatchFixer;
    }

    /// <inheritdoc />
    public override async Task RegisterCodeFixesAsync(CodeFixContext context)
    {
        var root = await context.Document.GetSyntaxRootAsync(context.CancellationToken);

        if (root == null)
            return;

        var diagnostic = context.Diagnostics.First();
        var diagnosticSpan = diagnostic.Location.SourceSpan;

        // Find the method identified by diagnostic
        var node = root.FindNode(diagnosticSpan);

        if (node == null)
            return;

        // Find the containing method
        var methodDeclaration = node.FirstAncestorOrSelf<MethodDeclarationSyntax>();

        if (methodDeclaration == null)
            return;

        // Register code fixes for adding resilience configuration
        await RegisterResilienceConfigurationFixes(context, methodDeclaration, diagnostic);
    }

    /// <summary>
    ///     Registers code fixes for adding resilience configuration.
    /// </summary>
    private static Task RegisterResilienceConfigurationFixes(
        CodeFixContext context,
        MethodDeclarationSyntax methodDeclaration,
        Diagnostic diagnostic)
    {
        // Register code fix to add ResilientExecutionStrategy wrapper
        context.RegisterCodeFix(
            CodeAction.Create(
                "Add ResilientExecutionStrategy wrapper",
                ct => AddResilientExecutionStrategyWrapperAsync(context.Document, methodDeclaration, ct),
                nameof(ResilientExecutionConfigurationCodeFixProvider) + "_AddResilientExecutionStrategy"),
            diagnostic);

        // Register code fix to add MaxNodeRestartAttempts
        context.RegisterCodeFix(
            CodeAction.Create(
                "Set MaxNodeRestartAttempts",
                ct => AddMaxNodeRestartAttemptsAsync(context.Document, methodDeclaration, ct),
                nameof(ResilientExecutionConfigurationCodeFixProvider) + "_AddMaxNodeRestartAttempts"),
            diagnostic);

        // Register code fix to add MaxMaterializedItems
        context.RegisterCodeFix(
            CodeAction.Create(
                "Set MaxMaterializedItems",
                ct => AddMaxMaterializedItemsAsync(context.Document, methodDeclaration, ct),
                nameof(ResilientExecutionConfigurationCodeFixProvider) + "_AddMaxMaterializedItems"),
            diagnostic);

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Adds a ResilientExecutionStrategy wrapper to the node class.
    /// </summary>
    private static async Task<Document> AddResilientExecutionStrategyWrapperAsync(
        Document document,
        MethodDeclarationSyntax methodDeclaration,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Find the containing class
        var classDeclaration = methodDeclaration.FirstAncestorOrSelf<ClassDeclarationSyntax>();

        if (classDeclaration == null)
            return document;

        // Get the semantic model to check if ResilientExecutionStrategy is already applied
        var semanticModel = await document.GetSemanticModelAsync(cancellationToken);
        var classSymbol = semanticModel.GetDeclaredSymbol(classDeclaration, cancellationToken);

        if (classSymbol is not INamedTypeSymbol namedTypeSymbol)
            return document;

        // Check if the class already has ResilientExecutionStrategy attribute
        var hasResilientExecutionStrategy = namedTypeSymbol.GetAttributes()
            .Any(a => a.AttributeClass?.Name == "ResilientExecutionStrategyAttribute");

        if (hasResilientExecutionStrategy)
            return document;

        // Create the ResilientExecutionStrategy attribute
        var resilientExecutionStrategyAttribute = SyntaxFactory.Attribute(
            SyntaxFactory.ParseName("ResilientExecutionStrategy"));

        var attributeList = SyntaxFactory.AttributeList(
            SyntaxFactory.SingletonSeparatedList(resilientExecutionStrategyAttribute));

        // Add the attribute to the class
        var newClassDeclaration = classDeclaration.AddAttributeLists(attributeList);
        var newRoot = root.ReplaceNode(classDeclaration, newClassDeclaration);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Adds MaxNodeRestartAttempts configuration to the node class.
    /// </summary>
    private static async Task<Document> AddMaxNodeRestartAttemptsAsync(
        Document document,
        MethodDeclarationSyntax methodDeclaration,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Find the containing class
        var classDeclaration = methodDeclaration.FirstAncestorOrSelf<ClassDeclarationSyntax>();

        if (classDeclaration == null)
            return document;

        // Find the first constructor
        var constructor = classDeclaration.ChildNodes()
            .OfType<ConstructorDeclarationSyntax>()
            .FirstOrDefault();

        if (constructor == null)
            return document;

        // Check if the constructor already has MaxNodeRestartAttempts parameter
        var hasMaxNodeRestartAttempts = constructor.ParameterList.Parameters
            .Any(p => p.Identifier.Text == "maxNodeRestartAttempts");

        if (hasMaxNodeRestartAttempts)
            return document;

        // Create the MaxNodeRestartAttempts parameter
        var maxNodeRestartAttemptsType = SyntaxFactory.ParseTypeName("int?");

        var maxNodeRestartAttemptsParam = SyntaxFactory.Parameter(
                SyntaxFactory.Identifier("maxNodeRestartAttempts"))
            .WithType(maxNodeRestartAttemptsType)
            .WithDefault(SyntaxFactory.EqualsValueClause(
                SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression)));

        // Add the parameter to the constructor
        var newParameterList = constructor.ParameterList.AddParameters(maxNodeRestartAttemptsParam);
        var newConstructor = constructor.WithParameterList(newParameterList);

        // Replace the constructor
        var newRoot = root.ReplaceNode(constructor, newConstructor);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Adds MaxMaterializedItems configuration to the node class.
    /// </summary>
    private static async Task<Document> AddMaxMaterializedItemsAsync(
        Document document,
        MethodDeclarationSyntax methodDeclaration,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Find the containing class
        var classDeclaration = methodDeclaration.FirstAncestorOrSelf<ClassDeclarationSyntax>();

        if (classDeclaration == null)
            return document;

        // Find the first constructor
        var constructor = classDeclaration.ChildNodes()
            .OfType<ConstructorDeclarationSyntax>()
            .FirstOrDefault();

        if (constructor == null)
            return document;

        // Check if the constructor already has MaxMaterializedItems parameter
        var hasMaxMaterializedItems = constructor.ParameterList.Parameters
            .Any(p => p.Identifier.Text == "maxMaterializedItems");

        if (hasMaxMaterializedItems)
            return document;

        // Create the MaxMaterializedItems parameter
        var maxMaterializedItemsType = SyntaxFactory.ParseTypeName("int?");

        var maxMaterializedItemsParam = SyntaxFactory.Parameter(
                SyntaxFactory.Identifier("maxMaterializedItems"))
            .WithType(maxMaterializedItemsType)
            .WithDefault(SyntaxFactory.EqualsValueClause(
                SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression)));

        // Add the parameter to the constructor
        var newParameterList = constructor.ParameterList.AddParameters(maxMaterializedItemsParam);
        var newConstructor = constructor.WithParameterList(newParameterList);

        // Replace the constructor
        var newRoot = root.ReplaceNode(constructor, newConstructor);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Checks if a constructor has a MaxNodeRestartAttempts parameter.
    /// </summary>
}
