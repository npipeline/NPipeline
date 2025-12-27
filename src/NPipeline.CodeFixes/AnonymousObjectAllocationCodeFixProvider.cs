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
    ///     Generates a named type declaration based on the anonymous object properties with proper type inference.
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
                var inferredType = InferPropertyType(initializer.Expression);
                propertyDeclarations.Add($"    public {inferredType} {propertyName} {{ get; set; }}");
            }
        }

        return $@"
/// <summary>
/// Generated type to replace anonymous object allocation for better performance.
/// Consider moving this type to a shared location if used multiple times.
/// </summary>
public class {className}
{{
{string.Join("\n", propertyDeclarations)}
}}";
    }

    /// <summary>
    ///     Infers the most appropriate type for a property based on the expression.
    /// </summary>
    private static string InferPropertyType(ExpressionSyntax? expression)
    {
        if (expression == null)
            return "object";

        return expression.Kind() switch
        {
            SyntaxKind.StringLiteralExpression => "string",
            SyntaxKind.NumericLiteralExpression => "double", // Default to double for numbers
            SyntaxKind.TrueLiteralExpression or SyntaxKind.FalseLiteralExpression => "bool",
            SyntaxKind.NullLiteralExpression => "object",
            SyntaxKind.ObjectCreationExpression => "object", // Could be enhanced to infer actual type
            SyntaxKind.InvocationExpression => "object", // Could be enhanced to infer return type
            SyntaxKind.IdentifierName => "var", // Use var for identifiers to preserve type
            _ => "object",
        };
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
    ///     Generates a value tuple expression with proper named elements.
    /// </summary>
    private static ExpressionSyntax GenerateValueTupleExpression(AnonymousObjectCreationExpressionSyntax anonymousObject)
    {
        var properties = anonymousObject.Initializers;
        var expressions = new List<ExpressionSyntax>();
        var propertyNames = new List<string>();

        foreach (var initializer in properties)
        {
            if (initializer.Expression != null && initializer.NameEquals != null)
            {
                expressions.Add(initializer.Expression);
                propertyNames.Add(initializer.NameEquals.Name.Identifier.Text);
            }
        }

        // Create a tuple expression by wrapping expressions in parentheses
        if (expressions.Count > 0)
        {
            // For single property, return the expression directly
            if (expressions.Count == 1)
                return expressions[0];

            // For multiple properties, create a tuple with named elements
            if (expressions.Count <= 7) // ValueTuple supports up to 7 elements + rest
                return CreateValueTupleExpression(expressions);

            // For more than 7 elements, use ValueTuple.Create with nested tuples
            return CreateValueTupleExpression(expressions);
        }

        // Fallback to default expression
        return SyntaxFactory.LiteralExpression(SyntaxKind.DefaultLiteralExpression);
    }

    /// <summary>
    ///     Creates a ValueTuple using ValueTuple.Create method for simplicity and reliability.
    /// </summary>
    private static ExpressionSyntax CreateValueTupleExpression(List<ExpressionSyntax> expressions)
    {
        try
        {
            if (expressions.Count == 0)
                return SyntaxFactory.LiteralExpression(SyntaxKind.DefaultLiteralExpression);

            if (expressions.Count == 1)
                return expressions[0];

            // Use ValueTuple.Create for multiple expressions
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
        catch
        {
            // Fallback to first expression if tuple creation fails
            return expressions.Count > 0
                ? expressions[0]
                : SyntaxFactory.LiteralExpression(SyntaxKind.DefaultLiteralExpression);
        }
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
