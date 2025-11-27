using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace NPipeline.Analyzers;

/// <summary>
///     Code fix provider that suggests alternatives to anonymous object allocations for better performance.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(AnonymousObjectAllocationCodeFixProvider))]
[Shared]
public sealed class AnonymousObjectAllocationCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [AnonymousObjectAllocationAnalyzer.AnonymousObjectAllocationId];

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

        // Find anonymous object creation expression identified by diagnostic
        if (root.FindNode(diagnosticSpan) is not AnonymousObjectCreationExpressionSyntax anonymousObject)
            return;

        // Register code fix for converting to named type
        context.RegisterCodeFix(
            CodeAction.Create(
                "Convert to named type",
                cancellationToken => ConvertToNamedTypeAsync(context.Document, anonymousObject, cancellationToken),
                nameof(AnonymousObjectAllocationCodeFixProvider)),
            diagnostic);

        // Register code fix for converting to value tuple
        context.RegisterCodeFix(
            CodeAction.Create(
                "Convert to value tuple",
                cancellationToken => ConvertToValueTupleAsync(context.Document, anonymousObject, cancellationToken),
                nameof(AnonymousObjectAllocationCodeFixProvider) + "_ValueTuple"),
            diagnostic);
    }

    /// <summary>
    ///     Converts an anonymous object to a named type.
    /// </summary>
    private static async Task<Document> ConvertToNamedTypeAsync(
        Document document,
        AnonymousObjectCreationExpressionSyntax anonymousObject,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Generate a named type based on the anonymous object properties
        var namedTypeCode = GenerateNamedTypeCode(anonymousObject);
        var namedTypeDeclaration = SyntaxFactory.ParseCompilationUnit(namedTypeCode);

        // Get the containing class to add the named type
        var containingClass = anonymousObject.FirstAncestorOrSelf<ClassDeclarationSyntax>();

        if (containingClass == null)
            return document;

        // Generate replacement expression using the named type
        var replacementExpression = GenerateNamedTypeExpression(anonymousObject);

        // Replace the anonymous object with the named type expression
        var newRoot = root.ReplaceNode(anonymousObject, replacementExpression);

        // Add the named type declaration to the containing class
        var classWithNamedType = AddNamedTypeToClass(containingClass, namedTypeDeclaration);
        var finalRoot = newRoot.ReplaceNode(containingClass, classWithNamedType);

        return document.WithSyntaxRoot(finalRoot);
    }

    /// <summary>
    ///     Converts an anonymous object to a value tuple.
    /// </summary>
    private static async Task<Document> ConvertToValueTupleAsync(
        Document document,
        AnonymousObjectCreationExpressionSyntax anonymousObject,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Generate value tuple expression
        var valueTupleExpression = GenerateValueTupleExpression(anonymousObject);

        // Replace the anonymous object with the value tuple expression
        var newRoot = root.ReplaceNode(anonymousObject, valueTupleExpression);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Generates a named type declaration based on the anonymous object properties.
    /// </summary>
    private static string GenerateNamedTypeCode(AnonymousObjectCreationExpressionSyntax anonymousObject)
    {
        var properties = anonymousObject.Initializers;
        var className = "GeneratedType"; // Use a simple name for the replacement

        var propertyDeclarations = new List<string>();

        foreach (var initializer in properties)
        {
            if (initializer.NameEquals != null)
            {
                var propertyName = initializer.NameEquals.Name.Identifier.Text;
                propertyDeclarations.Add($"    public object {propertyName} {{ get; set; }}");
            }
        }

        return $@"
/// <summary>
/// Generated type to replace anonymous object allocation.
/// </summary>
public class {className}
{{
{string.Join("\n", propertyDeclarations)}
}}";
    }


    /// <summary>
    ///     Generates an expression using the named type.
    /// </summary>
    private static ExpressionSyntax GenerateNamedTypeExpression(AnonymousObjectCreationExpressionSyntax anonymousObject)
    {
        var className = "GeneratedType"; // Use consistent name
        var properties = anonymousObject.Initializers;

        var argumentList = new List<ArgumentSyntax>();

        foreach (var initializer in properties)
        {
            if (initializer.Expression != null)
                argumentList.Add(SyntaxFactory.Argument(initializer.Expression));
        }

        var objectCreation = SyntaxFactory.ObjectCreationExpression(
            SyntaxFactory.IdentifierName(className),
            SyntaxFactory.ArgumentList(SyntaxFactory.SeparatedList(argumentList)),
            null);

        return objectCreation;
    }

    /// <summary>
    ///     Generates a value tuple expression.
    /// </summary>
    private static ExpressionSyntax GenerateValueTupleExpression(AnonymousObjectCreationExpressionSyntax anonymousObject)
    {
        var properties = anonymousObject.Initializers;
        var expressions = new List<ExpressionSyntax>();

        foreach (var initializer in properties)
        {
            if (initializer.Expression != null)
                expressions.Add(initializer.Expression);
        }

        // Create a tuple expression by wrapping expressions in parentheses
        if (expressions.Count > 0)
        {
            // For simplicity, create a parenthesized expression as a tuple alternative
            // In a real implementation, you'd want to create proper ValueTuple syntax
            if (expressions.Count == 1)
                return expressions[0];

            // Create a tuple using ValueTuple.Create method
            var valueTupleCreate = SyntaxFactory.MemberAccessExpression(
                SyntaxKind.SimpleMemberAccessExpression,
                SyntaxFactory.IdentifierName("ValueTuple"),
                SyntaxFactory.IdentifierName("Create"));

            var arguments = new List<ArgumentSyntax>();

            foreach (var expr in expressions)
            {
                arguments.Add(SyntaxFactory.Argument(expr));
            }

            var argumentList = SyntaxFactory.ArgumentList(SyntaxFactory.SeparatedList(arguments));

            return SyntaxFactory.InvocationExpression(valueTupleCreate, argumentList);
        }

        // Fallback to default expression
        return SyntaxFactory.LiteralExpression(SyntaxKind.DefaultLiteralExpression);
    }

    /// <summary>
    ///     Adds a named type declaration to a class.
    /// </summary>
    private static ClassDeclarationSyntax AddNamedTypeToClass(
        ClassDeclarationSyntax classDeclaration,
        CompilationUnitSyntax namedTypeCompilation)
    {
        // Extract the class declaration from the compilation unit
        var namedTypeClass = namedTypeCompilation.Members
            .OfType<ClassDeclarationSyntax>()
            .FirstOrDefault();

        if (namedTypeClass == null)
            return classDeclaration;

        // Add the named type as a nested class
        var newClassDeclaration = classDeclaration.AddMembers(namedTypeClass);
        return newClassDeclaration;
    }
}
