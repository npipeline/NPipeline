using System.Collections.Immutable;
using System.Composition;
using System.Diagnostics;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using NPipeline.Analyzers;

namespace NPipeline.CodeFixes;

/// <summary>
///     Provides logging functionality for code fix providers.
/// </summary>
internal static class CodeFixLogger
{
    /// <summary>
    ///     Logs an error message using appropriate logging mechanism.
    /// </summary>
    /// <param name="message">The error message to log.</param>
    internal static void LogError(string message)
    {
        // In a production environment, this would use proper logging infrastructure
        // For now, we'll use Debug.WriteLine as a fallback
        Debug.WriteLine($"[CodeFix Error] {message}");

        // TODO: Replace with proper logging framework when available:
        // - ILogger<T> with dependency injection
        // - Serilog, NLog, or other structured logging
        // - Microsoft.Extensions.Logging
    }
}

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
        try
        {
            // Validate input parameters
            if (context.Document == null || context.Diagnostics == null || !context.Diagnostics.Any())
                return;

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
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
            throw;
        }
        catch (Exception ex)
        {
            // Log the error but don't throw to avoid breaking the analyzer
            // In production, this should use proper logging infrastructure
            CodeFixLogger.LogError($"Error in RegisterCodeFixesAsync: {ex.Message}");
        }
    }

    /// <summary>
    ///     Registers code fixes for object creation expressions.
    /// </summary>
    private static async Task RegisterObjectCreationFixes(
        CodeFixContext context,
        ObjectCreationExpressionSyntax objectCreation,
        Diagnostic diagnostic)
    {
        try
        {
            // Validate input parameters
            if (context.Document == null || objectCreation == null || diagnostic == null)
                return;

            var semanticModel = await context.Document.GetSemanticModelAsync(context.CancellationToken);

            if (semanticModel == null)
                return;

            var typeInfo = semanticModel.GetTypeInfo(objectCreation, context.CancellationToken);
            var typeSymbol = typeInfo.Type;

            if (typeSymbol == null)
                return;

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
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
            throw;
        }
        catch (Exception ex)
        {
            // Log the error but don't throw to avoid breaking the analyzer
            CodeFixLogger.LogError($"Error in RegisterObjectCreationFixes: {ex.Message}");
        }
    }

    /// <summary>
    ///     Registers code fixes for invocation expressions.
    /// </summary>
    private static async Task RegisterInvocationFixes(
        CodeFixContext context,
        InvocationExpressionSyntax invocation,
        Diagnostic diagnostic)
    {
        try
        {
            // Validate input parameters
            if (context.Document == null || invocation == null || diagnostic == null)
                return;

            if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                return;

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
                        $"Replace {memberAccess.Expression}.{methodName}() with async version",
                        ct => ReplaceFileIOWithAsyncAsync(context.Document, invocation, ct),
                        nameof(SourceNodeStreamingCodeFixProvider) + "_FileIOToAsync"),
                    diagnostic);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
            throw;
        }
        catch (Exception ex)
        {
            // Log the error but don't throw to avoid breaking the analyzer
            CodeFixLogger.LogError($"Error in RegisterInvocationFixes: {ex.Message}");
        }
    }

    /// <summary>
    ///     Registers code fixes for member access expressions.
    /// </summary>
    private static async Task RegisterMemberAccessFixes(
        CodeFixContext context,
        MemberAccessExpressionSyntax memberAccess,
        Diagnostic diagnostic)
    {
        try
        {
            // Validate input parameters
            if (context.Document == null || memberAccess == null || diagnostic == null)
                return;

            var propertyName = memberAccess.Name.Identifier.Text;

            // Check for synchronous file I/O property access
            if (IsFileIOSynchronousProperty(memberAccess))
            {
                context.RegisterCodeFix(
                    CodeAction.Create(
                        $"Replace synchronous {memberAccess.Expression}.{propertyName} with async version",
                        ct => ReplaceFileIOPropertyAsync(context.Document, memberAccess, ct),
                        nameof(SourceNodeStreamingCodeFixProvider) + "_FileIOPropertyToAsync"),
                    diagnostic);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
            throw;
        }
        catch (Exception ex)
        {
            // Log the error but don't throw to avoid breaking the analyzer
            CodeFixLogger.LogError($"Error in RegisterMemberAccessFixes: {ex.Message}");
        }
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
        try
        {
            // Validate input parameters
            if (document == null || fileIOInvocation == null)
                return document!;

            var root = await document.GetSyntaxRootAsync(cancellationToken);

            if (root == null)
                return document;

            if (fileIOInvocation.Expression is not MemberAccessExpressionSyntax memberAccess)
                return document;

            var methodName = memberAccess.Name.Identifier.Text;
            var asyncMethodName = methodName + "Async";

            // Determine the target type for the async method
            var targetTypeName = memberAccess.Expression switch
            {
                IdentifierNameSyntax { Identifier.Text: "File" } => "File",
                IdentifierNameSyntax { Identifier.Text: "Directory" } => "Directory",
                MemberAccessExpressionSyntax { Name.Identifier.Text: "FileInfo" } => "FileInfo",
                MemberAccessExpressionSyntax { Name.Identifier.Text: "DirectoryInfo" } => "DirectoryInfo",
                _ => "File", // Default to File for unknown cases
            };

            // Create the async file I/O method invocation with proper target
            var asyncInvocation = SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName(targetTypeName),
                    SyntaxFactory.IdentifierName(asyncMethodName)),
                fileIOInvocation.ArgumentList);

            // Replace the synchronous invocation with the async one
            var newRoot = root.ReplaceNode(fileIOInvocation, asyncInvocation);

            // Make the containing method async
            var newRootWithAsync = await MakeMethodAsyncAsync(newRoot, fileIOInvocation, cancellationToken);

            return document.WithSyntaxRoot(newRootWithAsync);
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
            throw;
        }
        catch (Exception ex)
        {
            // Log the error but don't throw to avoid breaking the analyzer
            CodeFixLogger.LogError($"Error in ReplaceFileIOWithAsyncAsync: {ex.Message}");
            return document;
        }
    }

    /// <summary>
    ///     Replaces synchronous file I/O property with async version.
    /// </summary>
    private static async Task<Document> ReplaceFileIOPropertyAsync(
        Document document,
        MemberAccessExpressionSyntax fileIOProperty,
        CancellationToken cancellationToken)
    {
        try
        {
            // Validate input parameters
            if (document == null || fileIOProperty == null)
                return document!;

            var root = await document.GetSyntaxRootAsync(cancellationToken);

            if (root == null)
                return document;

            var propertyName = fileIOProperty.Name.Identifier.Text;
            var semanticModel = await document.GetSemanticModelAsync(cancellationToken);

            // Get the type information to determine if we're dealing with FileInfo or DirectoryInfo
            var typeInfo = semanticModel.GetTypeInfo(fileIOProperty.Expression, cancellationToken);
            var expressionType = typeInfo.Type?.Name;

            // Get the expression text for use in replacements
            var expressionText = fileIOProperty.Expression.ToString();

            // Create appropriate replacement based on the property and type
            var replacementExpression = propertyName switch
            {
                // FileInfo.Length - no direct async alternative, but we can add a comment
                "Length" when expressionType == "FileInfo" =>
                    SyntaxFactory.ParseExpression($"/* FileInfo.Length has no direct async alternative */ {expressionText}.Length"),

                // FileInfo.Exists - use async alternative
                "Exists" when expressionType == "FileInfo" =>
                    SyntaxFactory.ParseExpression($"await new FileInfo({expressionText}.FullName).ExistsAsync()"),

                // FileInfo.Attributes - no direct async alternative
                "Attributes" when expressionType == "FileInfo" =>
                    SyntaxFactory.ParseExpression($"/* FileInfo.Attributes has no direct async alternative */ {expressionText}.Attributes"),

                // DirectoryInfo.Exists - use async alternative
                "Exists" when expressionType == "DirectoryInfo" =>
                    SyntaxFactory.ParseExpression($"await new DirectoryInfo({expressionText}.FullName).ExistsAsync()"),

                // Time properties - no direct async alternatives
                "CreationTime" when expressionType == "FileInfo" =>
                    SyntaxFactory.ParseExpression($"/* FileInfo.CreationTime has no direct async alternative */ {expressionText}.CreationTime"),
                "CreationTime" when expressionType == "DirectoryInfo" =>
                    SyntaxFactory.ParseExpression($"/* DirectoryInfo.CreationTime has no direct async alternative */ {expressionText}.CreationTime"),
                "LastAccessTime" when expressionType == "FileInfo" =>
                    SyntaxFactory.ParseExpression($"/* FileInfo.LastAccessTime has no direct async alternative */ {expressionText}.LastAccessTime"),
                "LastAccessTime" when expressionType == "DirectoryInfo" =>
                    SyntaxFactory.ParseExpression($"/* DirectoryInfo.LastAccessTime has no direct async alternative */ {expressionText}.LastAccessTime"),
                "LastWriteTime" when expressionType == "FileInfo" =>
                    SyntaxFactory.ParseExpression($"/* FileInfo.LastWriteTime has no direct async alternative */ {expressionText}.LastWriteTime"),
                "LastWriteTime" when expressionType == "DirectoryInfo" =>
                    SyntaxFactory.ParseExpression($"/* DirectoryInfo.LastWriteTime has no direct async alternative */ {expressionText}.LastWriteTime"),

                // Name properties - no async alternatives needed
                "Name" or "FullName" or "Extension" or "DirectoryName" when expressionType == "FileInfo" =>
                    SyntaxFactory.ParseExpression($"/* {expressionType}.{propertyName} is safe to use synchronously */ {expressionText}.{propertyName}"),

                "Name" or "FullName" or "Parent" or "Root" when expressionType == "DirectoryInfo" =>
                    SyntaxFactory.ParseExpression($"/* {expressionType}.{propertyName} is safe to use synchronously */ {expressionText}.{propertyName}"),

                // Default case for unknown properties - implement proper async alternatives
                _ => GenerateAsyncAlternativeForUnknownProperty(expressionType, propertyName, expressionText),
            };

            // Replace the property access with the appropriate replacement
            var newRoot = root.ReplaceNode(fileIOProperty, replacementExpression);

            // Make the containing method async if we're using await
            if (replacementExpression.ToString().Contains("await"))
                newRoot = await MakeMethodAsyncAsync(newRoot, fileIOProperty, cancellationToken);

            return document.WithSyntaxRoot(newRoot);
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
            throw;
        }
        catch (Exception ex)
        {
            // Log the error but don't throw to avoid breaking the analyzer
            CodeFixLogger.LogError($"Error in ReplaceFileIOPropertyAsync: {ex.Message}");
            return document;
        }
    }

    /// <summary>
    ///     Creates a streaming method implementation using IAsyncEnumerable with yield return.
    /// </summary>
    private static MethodDeclarationSyntax CreateStreamingMethodImplementation(
        MethodDeclarationSyntax originalMethod,
        string elementTypeName)
    {
        try
        {
            // Validate input parameters
            if (originalMethod == null || string.IsNullOrEmpty(elementTypeName))
                return originalMethod!;

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
        catch (Exception ex)
        {
            // Log the error but don't throw to avoid breaking the analyzer
            CodeFixLogger.LogError($"Error in CreateStreamingMethodImplementation: {ex.Message}");
            return originalMethod;
        }
    }

    /// <summary>
    ///     Makes the containing method async.
    /// </summary>
    private static Task<SyntaxNode> MakeMethodAsyncAsync(
        SyntaxNode root,
        SyntaxNode node,
        CancellationToken cancellationToken)
    {
        try
        {
            // Validate input parameters
            if (root == null || node == null)
                return Task.FromResult<SyntaxNode>(root!);

            // Check for cancellation
            cancellationToken.ThrowIfCancellationRequested();

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
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
            throw;
        }
        catch (Exception ex)
        {
            // Log the error but don't throw to avoid breaking the analyzer
            CodeFixLogger.LogError($"Error in MakeMethodAsyncAsync: {ex.Message}");
            return Task.FromResult(root);
        }
    }

    /// <summary>
    ///     Checks if member access is a synchronous file I/O call.
    /// </summary>
    private static bool IsFileIOSynchronousCall(MemberAccessExpressionSyntax memberAccess)
    {
        // Check for File class methods
        if (memberAccess.Expression is IdentifierNameSyntax { Identifier.Text: "File" })
        {
            var methodName = memberAccess.Name.Identifier.Text;

            // Comprehensive list of synchronous File methods that have async alternatives
            return methodName switch
            {
                // Read operations
                "ReadAllText" or "ReadAllLines" or "ReadAllBytes" or "ReadLines" or
                    "ReadText" or "OpenText" or "OpenRead" or "OpenWrite" or

                    // Write operations
                    "WriteAllText" or "WriteAllLines" or "WriteAllBytes" or "WriteLines" or
                    "AppendAllText" or "AppendAllLines" or "AppendText" or

                    // File operations
                    "Create" or "Delete" or "Copy" or "Move" or "Replace" or "Encrypt" or "Decrypt" or

                    // Check operations
                    "Exists" or

                    // Attribute operations
                    "GetAttributes" or "SetAttributes" or "GetCreationTime" or "SetCreationTime" or
                    "GetLastAccessTime" or "SetLastAccessTime" or "GetLastWriteTime" or "SetLastWriteTime" or

                    // Security operations
                    "GetAccessControl" or "SetAccessControl" or

                    // Stream operations
                    "CreateText" or "Open" => true,

                _ => false,
            };
        }

        // Check for FileInfo methods
        if (memberAccess.Expression is MemberAccessExpressionSyntax { Name.Identifier.Text: "FileInfo" } ||
            memberAccess.Expression is IdentifierNameSyntax { Identifier.Text: "fileInfo" } ||
            (memberAccess.Expression is MemberAccessExpressionSyntax innerFileInfoAccess &&
             innerFileInfoAccess.Name.Identifier.Text == "FileInfo"))
        {
            var methodName = memberAccess.Name.Identifier.Text;

            return methodName switch
            {
                // Read operations
                "OpenRead" or "OpenWrite" or "OpenText" or "CreateText" or "AppendText" or

                    // File operations
                    "Delete" or "CopyTo" or "MoveTo" or "Replace" or

                    // Check operations
                    "Refresh" or // Refresh can trigger I/O

                    // Stream operations
                    "Create" or "Open" => true,

                _ => false,
            };
        }

        // Check for DirectoryInfo methods
        if (memberAccess.Expression is MemberAccessExpressionSyntax { Name.Identifier.Text: "DirectoryInfo" } ||
            memberAccess.Expression is IdentifierNameSyntax { Identifier.Text: "directoryInfo" } ||
            (memberAccess.Expression is MemberAccessExpressionSyntax innerDirectoryAccess &&
             innerDirectoryAccess.Name.Identifier.Text == "DirectoryInfo"))
        {
            var methodName = memberAccess.Name.Identifier.Text;

            return methodName switch
            {
                // Directory operations
                "Create" or "CreateSubdirectory" or "Delete" or "MoveTo" => true,

                // Enumeration operations (these can be expensive)
                "GetFiles" or "GetDirectories" or "GetFileSystemInfos" or "EnumerateFiles" or
                    "EnumerateDirectories" or "EnumerateFileSystemInfos" => true,

                // Check operations
                "Refresh" => true, // Refresh can trigger I/O

                _ => false,
            };
        }

        // Check for Directory class methods
        if (memberAccess.Expression is IdentifierNameSyntax { Identifier.Text: "Directory" })
        {
            var methodName = memberAccess.Name.Identifier.Text;

            return methodName switch
            {
                // Directory operations
                "CreateDirectory" or "Delete" or "Move" or "GetCurrentDirectory" or "SetCurrentDirectory" or

                    // Enumeration operations
                    "GetFiles" or "GetDirectories" or "GetFileSystemInfos" or "GetLogicalDrives" or
                    "GetParent" or "GetDirectoryRoot" or

                    // Check operations
                    "Exists" or

                    // Time operations
                    "GetCreationTime" or "SetCreationTime" or "GetLastAccessTime" or "SetLastAccessTime" or
                    "GetLastWriteTime" or "SetLastWriteTime" => true,

                _ => false,
            };
        }

        return false;
    }

    /// <summary>
    ///     Checks if member access is a synchronous file I/O property.
    /// </summary>
    private static bool IsFileIOSynchronousProperty(MemberAccessExpressionSyntax memberAccess)
    {
        // Check for FileInfo properties
        if (memberAccess.Expression is MemberAccessExpressionSyntax { Expression: var parentExpr, Name.Identifier.Text: var parentName })
        {
            // Check for FileInfo properties like Length, Exists, Attributes, etc.
            if (parentName == "FileInfo" ||
                parentExpr is IdentifierNameSyntax { Identifier.Text: "fileInfo" } ||
                (parentExpr is MemberAccessExpressionSyntax innerFileInfoAccess &&
                 innerFileInfoAccess.Name.Identifier.Text == "FileInfo"))
            {
                var propertyName = memberAccess.Name.Identifier.Text;

                return propertyName switch
                {
                    "Length" or "Exists" or "Attributes" or "CreationTime" or "LastAccessTime" or
                        "LastWriteTime" or "Extension" or "FullName" or "Name" or "DirectoryName" => true,
                    _ => false,
                };
            }

            // Check for DirectoryInfo properties
            if (parentName == "DirectoryInfo" ||
                parentExpr is IdentifierNameSyntax { Identifier.Text: "directoryInfo" } ||
                (parentExpr is MemberAccessExpressionSyntax innerDirectoryAccess &&
                 innerDirectoryAccess.Name.Identifier.Text == "DirectoryInfo"))
            {
                var propertyName = memberAccess.Name.Identifier.Text;

                return propertyName switch
                {
                    "Exists" or "CreationTime" or "LastAccessTime" or "LastWriteTime" or
                        "FullName" or "Name" or "Parent" or "Root" => true,
                    _ => false,
                };
            }
        }

        // Check for direct File class properties (though most are methods)
        if (memberAccess.Expression is IdentifierNameSyntax { Identifier.Text: "File" })
        {
            var propertyName = memberAccess.Name.Identifier.Text;

            return propertyName switch
            {
                // File class doesn't have many synchronous properties, but we include for completeness
                _ => false,
            };
        }

        // Check for variable names that might be FileInfo or DirectoryInfo instances
        if (memberAccess.Expression is IdentifierNameSyntax)
        {
            var propertyName = memberAccess.Name.Identifier.Text;

            // Common FileInfo properties
            var fileInfoProperties = new HashSet<string>
            {
                "Length", "Exists", "Attributes", "CreationTime", "LastAccessTime",
                "LastWriteTime", "Extension", "FullName", "Name", "DirectoryName",
            };

            // Common DirectoryInfo properties
            var directoryInfoProperties = new HashSet<string>
            {
                "Exists", "CreationTime", "LastAccessTime", "LastWriteTime",
                "FullName", "Name", "Parent", "Root",
            };

            // If the property name matches any known file I/O properties, we'll flag it
            // In a real implementation, we'd use semantic model to determine the actual type
            return fileInfoProperties.Contains(propertyName) || directoryInfoProperties.Contains(propertyName);
        }

        return false;
    }

    /// <summary>
    ///     Generates appropriate async alternatives for unknown file I/O properties.
    /// </summary>
    private static ExpressionSyntax GenerateAsyncAlternativeForUnknownProperty(
        string? expressionType,
        string propertyName,
        string expressionText)
    {
        try
        {
            // Handle common file I/O properties with appropriate async alternatives
            return propertyName switch
            {
                // FileInfo properties that might have async alternatives
                "Length" when expressionType == "FileInfo" =>
                    SyntaxFactory.ParseExpression($"/* FileInfo.Length has no direct async alternative - consider caching */ {expressionText}.Length"),

                "Exists" when expressionType == "FileInfo" =>
                    SyntaxFactory.ParseExpression($"await new FileInfo({expressionText}.FullName).ExistsAsync()"),

                // DirectoryInfo properties
                "Exists" when expressionType == "DirectoryInfo" =>
                    SyntaxFactory.ParseExpression($"await new DirectoryInfo({expressionText}.FullName).ExistsAsync()"),

                // Properties that are generally safe to use synchronously
                "Name" or "FullName" or "Extension" or "DirectoryName" when expressionType == "FileInfo" =>
                    SyntaxFactory.ParseExpression($"/* {expressionType}.{propertyName} is safe to use synchronously */ {expressionText}.{propertyName}"),

                "Name" or "FullName" or "Parent" or "Root" when expressionType == "DirectoryInfo" =>
                    SyntaxFactory.ParseExpression($"/* {expressionType}.{propertyName} is safe to use synchronously */ {expressionText}.{propertyName}"),

                // Time-based properties - suggest async alternatives where possible
                "CreationTime" when expressionType == "FileInfo" =>
                    SyntaxFactory.ParseExpression($"/* Consider using async file system APIs for time properties */ {expressionText}.CreationTime"),
                "CreationTime" when expressionType == "DirectoryInfo" =>
                    SyntaxFactory.ParseExpression($"/* Consider using async file system APIs for time properties */ {expressionText}.CreationTime"),
                "LastAccessTime" when expressionType == "FileInfo" =>
                    SyntaxFactory.ParseExpression($"/* Consider using async file system APIs for time properties */ {expressionText}.LastAccessTime"),
                "LastAccessTime" when expressionType == "DirectoryInfo" =>
                    SyntaxFactory.ParseExpression($"/* Consider using async file system APIs for time properties */ {expressionText}.LastAccessTime"),
                "LastWriteTime" when expressionType == "FileInfo" =>
                    SyntaxFactory.ParseExpression($"/* Consider using async file system APIs for time properties */ {expressionText}.LastWriteTime"),
                "LastWriteTime" when expressionType == "DirectoryInfo" =>
                    SyntaxFactory.ParseExpression($"/* Consider using async file system APIs for time properties */ {expressionText}.LastWriteTime"),

                // Default case - provide guidance for unknown properties
                _ => SyntaxFactory.ParseExpression(
                    $"/* Unknown file I/O property {expressionType}.{propertyName} - " +
                    $"verify if async alternative exists and update this fix */ {expressionText}.{propertyName}"),
            };
        }
        catch (Exception ex)
        {
            // Fallback to a safe expression with error comment
            return SyntaxFactory.ParseExpression(
                $"/* Error generating async alternative for {expressionType}.{propertyName}: {ex.Message} */ {expressionText}.{propertyName}");
        }
    }
}
