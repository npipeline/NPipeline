using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace NPipeline.Analyzers;

/// <summary>
///     Code fix provider that suggests streaming patterns for SourceNode implementations.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(SourceNodeStreamingCodeFixProvider))]
[Shared]
public sealed class SourceNodeStreamingCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [SourceNodeStreamingAnalyzer.SourceNodeStreamingId];

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

        // Register different code fixes based on the pattern type
        if (node is ObjectCreationExpressionSyntax objectCreation)
            await RegisterObjectCreationFixes(context, objectCreation, diagnostic);
        else if (node is InvocationExpressionSyntax invocation)
            await RegisterInvocationFixes(context, invocation, diagnostic);
        else if (node is MemberAccessExpressionSyntax memberAccess)
            await RegisterMemberAccessFixes(context, memberAccess, diagnostic);
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

        // Check for List<T> creation
        if (typeSymbol.Name == "List" && typeSymbol.ContainingNamespace?.Name == "System.Collections.Generic")
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Convert List<T> to IAsyncEnumerable with yield return",
                    ct => ConvertListToAsyncEnumerableAsync(context.Document, objectCreation, ct),
                    nameof(SourceNodeStreamingCodeFixProvider) + "_ListToAsyncEnumerable"),
                diagnostic);
        }

        // Check for array creation
        if (typeSymbol.Kind == SymbolKind.ArrayType)
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Convert array to IAsyncEnumerable with yield return",
                    ct => ConvertArrayToAsyncEnumerableAsync(context.Document, objectCreation, ct),
                    nameof(SourceNodeStreamingCodeFixProvider) + "_ArrayToAsyncEnumerable"),
                diagnostic);
        }

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

        // Check for .ToAsyncEnumerable() on collections
        if (methodName == "ToAsyncEnumerable")
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Replace .ToAsyncEnumerable() with streaming implementation",
                    ct => ReplaceToAsyncEnumerableAsync(context.Document, invocation, ct),
                    nameof(SourceNodeStreamingCodeFixProvider) + "_ReplaceToAsyncEnumerable"),
                diagnostic);
        }

        // Check for .ToList() or .ToArray() on collections
        if (methodName is "ToList" or "ToArray")
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    $"Replace .{methodName}() with streaming implementation",
                    ct => ReplaceMaterializationMethodAsync(context.Document, invocation, ct),
                    nameof(SourceNodeStreamingCodeFixProvider) + "_ReplaceMaterializationMethod"),
                diagnostic);
        }

        // Check for synchronous file I/O operations
        if (IsFileIOSynchronousCall(memberAccess))
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    $"Replace File.{methodName}() with async version",
                    ct => ReplaceFileIOWithAsyncAsync(context.Document, invocation, ct),
                    nameof(SourceNodeStreamingCodeFixProvider) + "_FileIOToAsync"),
                diagnostic);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Registers code fixes for member access expressions.
    /// </summary>
    private static Task RegisterMemberAccessFixes(
        CodeFixContext context,
        MemberAccessExpressionSyntax memberAccess,
        Diagnostic diagnostic)
    {
        var methodName = memberAccess.Name.Identifier.Text;

        // Check for synchronous file I/O property access
        if (IsFileIOSynchronousProperty(memberAccess))
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Replace synchronous file I/O property with async version",
                    ct => ReplaceFileIOPropertyAsync(context.Document, memberAccess, ct),
                    nameof(SourceNodeStreamingCodeFixProvider) + "_FileIOPropertyToAsync"),
                diagnostic);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Converts a List<T> creation to IAsyncEnumerable with yield return.
    /// </summary>
    private static async Task<Document> ConvertListToAsyncEnumerableAsync(
        Document document,
        ObjectCreationExpressionSyntax listCreation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Get the containing method
        var containingMethod = listCreation.FirstAncestorOrSelf<MethodDeclarationSyntax>();

        if (containingMethod == null)
            return document;

        // Get the semantic model to determine the element type
        var semanticModel = await document.GetSemanticModelAsync(cancellationToken);
        var typeInfo = semanticModel.GetTypeInfo(listCreation, cancellationToken);

        if (typeInfo.Type is not INamedTypeSymbol typeSymbol)
            return document;

        // Get the element type from List<T>
        var elementType = typeSymbol.TypeArguments.FirstOrDefault();
        var elementTypeName = elementType?.ToDisplayString() ?? "object";

        // Create a streaming method implementation
        var streamingMethod = CreateStreamingMethodImplementation(containingMethod, elementTypeName);

        // Replace the original method with the streaming implementation
        var newRoot = root.ReplaceNode(containingMethod, streamingMethod);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Converts an array creation to IAsyncEnumerable with yield return.
    /// </summary>
    private static async Task<Document> ConvertArrayToAsyncEnumerableAsync(
        Document document,
        ObjectCreationExpressionSyntax arrayCreation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Get the containing method
        var containingMethod = arrayCreation.FirstAncestorOrSelf<MethodDeclarationSyntax>();

        if (containingMethod == null)
            return document;

        // Get the semantic model to determine the element type
        var semanticModel = await document.GetSemanticModelAsync(cancellationToken);
        var typeInfo = semanticModel.GetTypeInfo(arrayCreation, cancellationToken);

        if (typeInfo.Type is not IArrayTypeSymbol arrayType)
            return document;

        // Get the element type from the array
        var elementTypeName = arrayType.ElementType?.ToDisplayString() ?? "object";

        // Create a streaming method implementation
        var streamingMethod = CreateStreamingMethodImplementation(containingMethod, elementTypeName);

        // Replace the original method with the streaming implementation
        var newRoot = root.ReplaceNode(containingMethod, streamingMethod);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Replaces .ToAsyncEnumerable() with a streaming implementation.
    /// </summary>
    private static async Task<Document> ReplaceToAsyncEnumerableAsync(
        Document document,
        InvocationExpressionSyntax toAsyncEnumerableInvocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Get the containing method
        var containingMethod = toAsyncEnumerableInvocation.FirstAncestorOrSelf<MethodDeclarationSyntax>();

        if (containingMethod == null)
            return document;

        // Get the collection being converted
        if (toAsyncEnumerableInvocation.Expression is not MemberAccessExpressionSyntax)
            return document;

        // Create a streaming method implementation
        var streamingMethod = CreateStreamingMethodImplementation(containingMethod, "object");

        // Replace the original method with the streaming implementation
        var newRoot = root.ReplaceNode(containingMethod, streamingMethod);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Replaces .ToList() or .ToArray() with a streaming implementation.
    /// </summary>
    private static async Task<Document> ReplaceMaterializationMethodAsync(
        Document document,
        InvocationExpressionSyntax materializationInvocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Get the containing method
        var containingMethod = materializationInvocation.FirstAncestorOrSelf<MethodDeclarationSyntax>();

        if (containingMethod == null)
            return document;

        // Get the semantic model to determine the element type
        var semanticModel = await document.GetSemanticModelAsync(cancellationToken);
        var typeInfo = semanticModel.GetTypeInfo(materializationInvocation.Expression, cancellationToken);

        if (typeInfo.Type is not INamedTypeSymbol typeSymbol)
            return document;

        // Get the element type from the collection
        var elementTypeName = typeSymbol.ToDisplayString();

        // Create a streaming method implementation
        var streamingMethod = CreateStreamingMethodImplementation(containingMethod, elementTypeName);

        // Replace the original method with the streaming implementation
        var newRoot = root.ReplaceNode(containingMethod, streamingMethod);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Replaces synchronous file I/O with async version.
    /// </summary>
    private static async Task<Document> ReplaceFileIOWithAsyncAsync(
        Document document,
        InvocationExpressionSyntax fileIOInvocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        if (fileIOInvocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return document;

        var methodName = memberAccess.Name.Identifier.Text;
        var asyncMethodName = methodName + "Async";

        // Create the async file I/O method invocation
        var asyncInvocation = SyntaxFactory.InvocationExpression(
            SyntaxFactory.MemberAccessExpression(
                SyntaxKind.SimpleMemberAccessExpression,
                SyntaxFactory.IdentifierName("File"),
                SyntaxFactory.IdentifierName(asyncMethodName)),
            fileIOInvocation.ArgumentList);

        // Replace the synchronous invocation with the async one
        var newRoot = root.ReplaceNode(fileIOInvocation, asyncInvocation);

        // Make the containing method async
        var newRootWithAsync = await MakeMethodAsyncAsync(newRoot, fileIOInvocation, cancellationToken);

        return document.WithSyntaxRoot(newRootWithAsync);
    }

    /// <summary>
    ///     Replaces synchronous file I/O property with async version.
    /// </summary>
    private static async Task<Document> ReplaceFileIOPropertyAsync(
        Document document,
        MemberAccessExpressionSyntax fileIOProperty,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        var propertyName = fileIOProperty.Name.Identifier.Text;

        // Replace the property access with a comment
        var newRoot = root.ReplaceNode(fileIOProperty,
            SyntaxFactory.ParseExpression($"/* TODO: Replace synchronous File.{propertyName} with async alternative */"));

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Creates a streaming method implementation using IAsyncEnumerable with yield return.
    /// </summary>
    private static MethodDeclarationSyntax CreateStreamingMethodImplementation(
        MethodDeclarationSyntax originalMethod,
        string elementTypeName)
    {
        // Create the method signature
        var iAsyncEnumerableType = SyntaxFactory.ParseTypeName(
            $"System.Collections.Generic.IAsyncEnumerable<{elementTypeName}>");

        var cancellationTokenType = SyntaxFactory.ParseTypeName("System.Threading.CancellationToken");

        var cancellationTokenParam = SyntaxFactory.Parameter(
                SyntaxFactory.Identifier("cancellationToken"))
            .WithType(cancellationTokenType);

        var parameters = originalMethod.ParameterList.Parameters
            .Where(p => p.Type?.ToString().Contains("CancellationToken") == true)
            .ToList();

        if (parameters.Count == 0)
            parameters.Add(cancellationTokenParam);

        var newParameterList = SyntaxFactory.ParameterList(
            SyntaxFactory.SeparatedList(parameters));

        // Create the method body with yield return
        var yieldReturnStatement = SyntaxFactory.YieldStatement(
            SyntaxKind.YieldReturnStatement,
            SyntaxFactory.DefaultExpression(SyntaxFactory.ParseTypeName(elementTypeName)));

        var methodBody = SyntaxFactory.Block(yieldReturnStatement);

        // Create the new method declaration
        var newMethod = originalMethod
            .WithReturnType(iAsyncEnumerableType)
            .WithParameterList(newParameterList)
            .WithBody(methodBody)
            .WithModifiers(SyntaxFactory.TokenList(SyntaxFactory.Token(SyntaxKind.AsyncKeyword)));

        return newMethod;
    }

    /// <summary>
    ///     Makes the containing method async.
    /// </summary>
    private static Task<SyntaxNode> MakeMethodAsyncAsync(
        SyntaxNode root,
        SyntaxNode node,
        CancellationToken _)
    {
        // Find the containing method
        var methodDeclaration = node.FirstAncestorOrSelf<MethodDeclarationSyntax>();

        if (methodDeclaration == null)
            return Task.FromResult(root);

        // Check if method is already async
        if (methodDeclaration.Modifiers.Any(m => m.IsKind(SyntaxKind.AsyncKeyword)))
            return Task.FromResult(root);

        // Add async modifier
        var asyncModifier = SyntaxFactory.Token(SyntaxKind.AsyncKeyword);
        var newModifiers = methodDeclaration.Modifiers.Insert(0, asyncModifier);
        var newMethodDeclaration = methodDeclaration.WithModifiers(newModifiers);

        // Replace the method declaration
        return Task.FromResult(root.ReplaceNode(methodDeclaration, newMethodDeclaration));
    }

    /// <summary>
    ///     Checks if member access is a synchronous file I/O call.
    /// </summary>
    private static bool IsFileIOSynchronousCall(MemberAccessExpressionSyntax memberAccess)
    {
        if (memberAccess.Expression is not IdentifierNameSyntax { Identifier.Text: "File" })
            return false;

        var methodName = memberAccess.Name.Identifier.Text;

        return methodName switch
        {
            "ReadAllText" or "ReadAllLines" or "ReadAllBytes" or "WriteAllText" or
                "WriteAllLines" or "WriteAllBytes" or "AppendAllText" or "AppendAllLines" or
                "OpenRead" or "OpenWrite" or "Create" or "Delete" or "Exists" or "Copy" or "Move" => true,
            _ => false,
        };
    }

    /// <summary>
    ///     Checks if member access is a synchronous file I/O property.
    /// </summary>
    private static bool IsFileIOSynchronousProperty(MemberAccessExpressionSyntax memberAccess)
    {
        if (memberAccess.Expression is not IdentifierNameSyntax { Identifier.Text: "File" })
            return false;

        // For now, we don't have specific synchronous file I/O properties to check
        // This method is placeholder for future extensions
        return false;
    }
}
