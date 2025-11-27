using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace NPipeline.Analyzers;

/// <summary>
///     Code fix provider that suggests proper dependency injection patterns for node implementations.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(DependencyInjectionCodeFixProvider))]
[Shared]
public sealed class DependencyInjectionCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [DependencyInjectionAnalyzer.DependencyInjectionAntiPatternId];

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

        // Find the node identified by diagnostic
        var node = root.FindNode(diagnosticSpan);

        if (node == null)
            return;

        // Register different code fixes based on the anti-pattern type
        if (node is ObjectCreationExpressionSyntax objectCreation)
            await RegisterObjectCreationFixes(context, objectCreation, diagnostic);
        else if (node is AssignmentExpressionSyntax assignment)
            await RegisterAssignmentFixes(context, assignment, diagnostic);
        else if (node is InvocationExpressionSyntax invocation)
            await RegisterInvocationFixes(context, invocation, diagnostic);
    }

    /// <summary>
    ///     Registers code fixes for object creation expressions.
    /// </summary>
    private static Task RegisterObjectCreationFixes(
        CodeFixContext context,
        ObjectCreationExpressionSyntax objectCreation,
        Diagnostic diagnostic)
    {
        var semanticModel = context.Document.GetSemanticModelAsync(context.CancellationToken).Result;
        var typeInfo = semanticModel.GetTypeInfo(objectCreation);
        var typeSymbol = typeInfo.Type;

        if (typeSymbol == null)
            return Task.CompletedTask;

        // Check if this is a service instantiation
        if (!IsService(typeSymbol))
            return Task.CompletedTask;

        // Register code fix to replace with constructor injection
        context.RegisterCodeFix(
            CodeAction.Create(
                "Replace with constructor injection",
                ct => ReplaceWithConstructorInjectionAsync(context.Document, objectCreation, ct),
                nameof(DependencyInjectionCodeFixProvider) + "_ReplaceWithConstructorInjection"),
            diagnostic);

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Registers code fixes for assignment expressions.
    /// </summary>
    private static Task RegisterAssignmentFixes(
        CodeFixContext context,
        AssignmentExpressionSyntax assignment,
        Diagnostic diagnostic)
    {
        // Check if this is a static singleton assignment
        if (!IsStaticSingletonAssignment(assignment))
            return Task.CompletedTask;

        // Register code fix to replace with constructor injection
        context.RegisterCodeFix(
            CodeAction.Create(
                "Replace static singleton with constructor injection",
                ct => ReplaceStaticSingletonWithConstructorInjectionAsync(context.Document, assignment, ct),
                nameof(DependencyInjectionCodeFixProvider) + "_ReplaceStaticSingleton"),
            diagnostic);

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Registers code fixes for invocation expressions.
    /// </summary>
    private static Task RegisterInvocationFixes(
        CodeFixContext context,
        InvocationExpressionSyntax invocation,
        Diagnostic diagnostic)
    {
        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return Task.CompletedTask;

        var methodName = memberAccess.Name.Identifier.Text;

        // Check for GetService or GetRequiredService calls
        if (methodName is "GetService" or "GetRequiredService")
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Replace service locator with constructor injection",
                    ct => ReplaceServiceLocatorWithConstructorInjectionAsync(context.Document, invocation, ct),
                    nameof(DependencyInjectionCodeFixProvider) + "_ReplaceServiceLocator"),
                diagnostic);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Replaces direct service instantiation with constructor injection.
    /// </summary>
    private static async Task<Document> ReplaceWithConstructorInjectionAsync(
        Document document,
        ObjectCreationExpressionSyntax objectCreation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Get the containing class
        var containingClass = objectCreation.FirstAncestorOrSelf<ClassDeclarationSyntax>();

        if (containingClass == null)
            return document;

        // Get the semantic model to determine the service type
        var semanticModel = await document.GetSemanticModelAsync(cancellationToken);
        var typeInfo = semanticModel.GetTypeInfo(objectCreation, cancellationToken);
        var serviceType = typeInfo.Type;

        if (serviceType == null)
            return document;

        // Find the constructor
        var constructor = containingClass.ChildNodes()
            .OfType<ConstructorDeclarationSyntax>()
            .FirstOrDefault();

        if (constructor == null)
            return document;

        // Replace the object creation with a parameter reference
        var parameterReference = SyntaxFactory.IdentifierName("service");
        var newRoot = root.ReplaceNode(objectCreation, parameterReference);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Replaces static singleton assignment with constructor injection.
    /// </summary>
    private static async Task<Document> ReplaceStaticSingletonWithConstructorInjectionAsync(
        Document document,
        AssignmentExpressionSyntax assignment,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Get the containing class
        var containingClass = assignment.FirstAncestorOrSelf<ClassDeclarationSyntax>();

        if (containingClass == null)
            return document;

        // Get the semantic model to determine the service type
        var semanticModel = await document.GetSemanticModelAsync(cancellationToken);

        // Get the service type from the right side of the assignment
        var serviceTypeInfo = semanticModel.GetTypeInfo(assignment.Right, cancellationToken);
        var serviceType = serviceTypeInfo.Type;

        if (serviceType == null)
            return document;

        // Find the constructor
        var constructor = containingClass.ChildNodes()
            .OfType<ConstructorDeclarationSyntax>()
            .FirstOrDefault();

        if (constructor == null)
            return document;

        // Create a field for the service
        var serviceField = SyntaxFactory.FieldDeclaration(
            SyntaxFactory.List<AttributeListSyntax>(),
            SyntaxFactory.TokenList(
                SyntaxFactory.Token(SyntaxKind.PrivateKeyword),
                SyntaxFactory.Token(SyntaxKind.ReadOnlyKeyword)),
            SyntaxFactory.VariableDeclaration(
                SyntaxFactory.ParseTypeName(serviceType.ToDisplayString()),
                SyntaxFactory.SingletonSeparatedList(
                    SyntaxFactory.VariableDeclarator(
                        SyntaxFactory.Identifier("_service")))));

        // Add the field to the class
        var newClass = containingClass.AddMembers(serviceField);

        // Replace the assignment with a field reference
        var fieldReference = SyntaxFactory.IdentifierName("_service");
        var newRoot = root.ReplaceNode(assignment.Right, fieldReference);

        // Replace the class with the updated constructor
        var finalRoot = newRoot.ReplaceNode(containingClass, newClass);

        return document.WithSyntaxRoot(finalRoot);
    }

    /// <summary>
    ///     Replaces service locator pattern with constructor injection.
    /// </summary>
    private static async Task<Document> ReplaceServiceLocatorWithConstructorInjectionAsync(
        Document document,
        InvocationExpressionSyntax serviceLocatorInvocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Get the containing class
        var containingClass = serviceLocatorInvocation.FirstAncestorOrSelf<ClassDeclarationSyntax>();

        if (containingClass == null)
            return document;

        // Get the semantic model to determine the service type
        var semanticModel = await document.GetSemanticModelAsync(cancellationToken);

        // Get the service type from the first argument
        if (serviceLocatorInvocation.ArgumentList?.Arguments.Count == 0)
            return document;

        var firstArgument = serviceLocatorInvocation.ArgumentList!.Arguments[0];
        var serviceTypeInfo = semanticModel.GetTypeInfo(firstArgument.Expression, cancellationToken);
        var serviceType = serviceTypeInfo.Type;

        if (serviceType == null)
            return document;

        // Find the constructor
        var constructor = containingClass.ChildNodes()
            .OfType<ConstructorDeclarationSyntax>()
            .FirstOrDefault();

        if (constructor == null)
            return document;

        // Create a field for the service
        var serviceField = SyntaxFactory.FieldDeclaration(
            SyntaxFactory.List<AttributeListSyntax>(),
            SyntaxFactory.TokenList(
                SyntaxFactory.Token(SyntaxKind.PrivateKeyword),
                SyntaxFactory.Token(SyntaxKind.ReadOnlyKeyword)),
            SyntaxFactory.VariableDeclaration(
                SyntaxFactory.ParseTypeName(serviceType.ToDisplayString()),
                SyntaxFactory.SingletonSeparatedList(
                    SyntaxFactory.VariableDeclarator(
                        SyntaxFactory.Identifier("_service")))));

        // Add the field to the class
        var newClass = containingClass.AddMembers(serviceField);

        // Replace the service locator call with a field reference
        var fieldReference = SyntaxFactory.IdentifierName("_service");
        var newRoot = root.ReplaceNode(serviceLocatorInvocation, fieldReference);

        // Replace the class with the updated constructor
        var finalRoot = newRoot.ReplaceNode(containingClass, newClass);

        return document.WithSyntaxRoot(finalRoot);
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

        if (containingNamespace != null &&
            (containingNamespace.Contains("Service") ||
             containingNamespace.Contains("Repository") ||
             containingNamespace.Contains("Provider") ||
             containingNamespace.Contains("Handler") ||
             containingNamespace.Contains("Manager")))
            return true;

        // Check if it has Service, Repository, etc. in the name
        var typeName = typeSymbol.Name;

        if (typeName.Contains("Service") ||
            typeName.Contains("Repository") ||
            typeName.Contains("Provider") ||
            typeName.Contains("Handler") ||
            typeName.Contains("Manager"))
            return true;

        // Check if it has methods (non-static) - likely a service
        var members = typeSymbol.GetMembers();

        var hasMethods = members.Any(m =>
            m.Kind == SymbolKind.Method &&
            !m.IsStatic &&
            m.Name != ".ctor" &&
            m.Name != "ToString" &&
            m.Name != "Equals" &&
            m.Name != "GetHashCode");

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

        // Value types are often DTOs
        if (typeSymbol.IsValueType)
            return true;

        // Types with only properties are likely DTOs
        var members = typeSymbol.GetMembers();

        var hasOnlyProperties = members.All(m =>
            m.Kind == SymbolKind.Property ||
            (m.Kind == SymbolKind.Method && m.IsStatic));

        return hasOnlyProperties;
    }

    /// <summary>
    ///     Determines if an assignment is a static singleton assignment.
    /// </summary>
    private static bool IsStaticSingletonAssignment(AssignmentExpressionSyntax assignment)
    {
        // Check if left side is a member access to a static field
        if (assignment.Left is not MemberAccessExpressionSyntax memberAccess)
            return false;

        // Check if the member access is to a static field
        if (memberAccess.Expression is not IdentifierNameSyntax identifierName)
            return false;

        // Check if the field name starts with an underscore (common static singleton pattern)
        var fieldName = identifierName.Identifier.Text;

        return fieldName.StartsWith("_", StringComparison.Ordinal) ||
               fieldName.StartsWith("s_", StringComparison.Ordinal);
    }
}
