using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace NPipeline.Analyzers;

/// <summary>
///     Code fix provider that suggests async alternatives for blocking operations in async methods.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(BlockingAsyncOperationCodeFixProvider))]
[Shared]
public sealed class BlockingAsyncOperationCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [BlockingAsyncOperationAnalyzer.BlockingAsyncOperationId];

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

        // Register different code fixes based on the blocking operation type
        if (node is MemberAccessExpressionSyntax memberAccess && memberAccess.Name.Identifier.Text == "Result")
            await RegisterResultFixes(context, memberAccess, diagnostic);
        else if (node is InvocationExpressionSyntax invocation)
            await RegisterInvocationFixes(context, invocation, diagnostic);
    }

    private static Task RegisterResultFixes(
        CodeFixContext context,
        MemberAccessExpressionSyntax memberAccess,
        Diagnostic diagnostic)
    {
        // Register code fix to replace .Result with await
        context.RegisterCodeFix(
            CodeAction.Create(
                "Replace .Result with await",
                cancellationToken => ReplaceResultWithAwaitAsync(context.Document, memberAccess, cancellationToken),
                nameof(BlockingAsyncOperationCodeFixProvider) + "_ResultToAwait"),
            diagnostic);

        // Register code fix to replace .Result with ConfigureAwait(false)
        context.RegisterCodeFix(
            CodeAction.Create(
                "Replace .Result with await and ConfigureAwait(false)",
                cancellationToken => ReplaceResultWithAwaitConfigureAwaitAsync(context.Document, memberAccess, cancellationToken),
                nameof(BlockingAsyncOperationCodeFixProvider) + "_ResultToAwaitConfigureAwait"),
            diagnostic);

        return Task.CompletedTask;
    }

    private static Task RegisterInvocationFixes(
        CodeFixContext context,
        InvocationExpressionSyntax invocation,
        Diagnostic diagnostic)
    {
        // Check for specific blocking patterns
        if (IsTaskWaitInvocation(invocation))
        {
            // Register code fix to replace .Wait() with await
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Replace .Wait() with await",
                    cancellationToken => ReplaceWaitWithAwaitAsync(context.Document, invocation, cancellationToken),
                    nameof(BlockingAsyncOperationCodeFixProvider) + "_WaitToAwait"),
                diagnostic);
        }
        else if (IsGetResultInvocation(invocation))
        {
            // Register code fix to replace GetResult() with await
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Replace GetResult() with await",
                    cancellationToken => ReplaceGetResultWithAwaitAsync(context.Document, invocation, cancellationToken),
                    nameof(BlockingAsyncOperationCodeFixProvider) + "_GetResultToAwait"),
                diagnostic);
        }
        else if (IsThreadSleepInvocation(invocation))
        {
            // Register code fix to replace Thread.Sleep() with Task.Delay()
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Replace Thread.Sleep() with Task.Delay()",
                    cancellationToken => ReplaceThreadSleepWithTaskDelayAsync(context.Document, invocation, cancellationToken),
                    nameof(BlockingAsyncOperationCodeFixProvider) + "_ThreadSleepToTaskDelay"),
                diagnostic);
        }
        else if (IsFileIOBlockingCall(invocation))
        {
            var methodName = GetMethodName(invocation);

            // Register code fix to replace File I/O with async version
            context.RegisterCodeFix(
                CodeAction.Create(
                    $"Replace File.{methodName}() with File.{methodName}Async()",
                    cancellationToken => ReplaceFileIOWithAsyncAsync(context.Document, invocation, cancellationToken),
                    nameof(BlockingAsyncOperationCodeFixProvider) + "_FileIOToAsync"),
                diagnostic);
        }
        else if (IsWebClientBlockingCall(invocation))
        {
            var methodName = GetMethodName(invocation);

            // Register code fix to replace WebClient blocking with async version
            context.RegisterCodeFix(
                CodeAction.Create(
                    $"Replace WebClient.{methodName}() with async version",
                    cancellationToken => ReplaceWebClientWithAsyncAsync(context.Document, invocation, cancellationToken),
                    nameof(BlockingAsyncOperationCodeFixProvider) + "_WebClientToAsync"),
                diagnostic);
        }
        else if (IsHttpClientBlockingCall(invocation))
        {
            var methodName = GetMethodName(invocation);

            // Register code fix to add await to HttpClient call
            context.RegisterCodeFix(
                CodeAction.Create(
                    $"Add await to HttpClient.{methodName}()",
                    cancellationToken => AddAwaitToHttpClientCallAsync(context.Document, invocation, cancellationToken),
                    nameof(BlockingAsyncOperationCodeFixProvider) + "_HttpClientToAwait"),
                diagnostic);
        }
        else if (IsStreamReaderWriterBlockingCall(invocation))
        {
            var methodName = GetMethodName(invocation);

            // Register code fix to replace StreamReader/Writer with async version
            context.RegisterCodeFix(
                CodeAction.Create(
                    $"Replace {methodName}() with async version",
                    cancellationToken => ReplaceStreamReaderWriterWithAsyncAsync(context.Document, invocation, cancellationToken),
                    nameof(BlockingAsyncOperationCodeFixProvider) + "_StreamReaderWriterToAsync"),
                diagnostic);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Determines if the invocation is a Task.Wait() call.
    /// </summary>
    private static bool IsTaskWaitInvocation(InvocationExpressionSyntax invocation)
    {
        return invocation.Expression is MemberAccessExpressionSyntax memberAccess &&
               memberAccess.Name.Identifier.Text == "Wait" &&
               memberAccess.Expression is IdentifierNameSyntax { Identifier.Text: "Task" };
    }

    /// <summary>
    ///     Determines if the invocation is a GetResult() call.
    /// </summary>
    private static bool IsGetResultInvocation(InvocationExpressionSyntax invocation)
    {
        return invocation.Expression is MemberAccessExpressionSyntax memberAccess &&
               memberAccess.Name.Identifier.Text == "GetResult";
    }

    /// <summary>
    ///     Determines if the invocation is a Thread.Sleep() call.
    /// </summary>
    private static bool IsThreadSleepInvocation(InvocationExpressionSyntax invocation)
    {
        return invocation.Expression is MemberAccessExpressionSyntax memberAccess &&
               memberAccess.Name.Identifier.Text == "Sleep" &&
               memberAccess.Expression is IdentifierNameSyntax { Identifier.Text: "Thread" };
    }

    /// <summary>
    ///     Determines if the invocation is a blocking File I/O call.
    /// </summary>
    private static bool IsFileIOBlockingCall(InvocationExpressionSyntax invocation)
    {
        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return false;

        if (memberAccess.Expression is not IdentifierNameSyntax { Identifier.Text: "File" })
            return false;

        var methodName = memberAccess.Name.Identifier.Text;

        return methodName is "ReadAllText" or "ReadAllLines" or "ReadAllBytes" or "WriteAllText" or
            "WriteAllLines" or "WriteAllBytes" or "AppendAllText" or "AppendAllLines" or
            "OpenRead" or "OpenWrite" or "Create" or "Delete" or "Exists" or "Copy" or "Move";
    }

    /// <summary>
    ///     Determines if the invocation is a blocking WebClient call.
    /// </summary>
    private static bool IsWebClientBlockingCall(InvocationExpressionSyntax invocation)
    {
        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return false;

        var methodName = memberAccess.Name.Identifier.Text;

        return methodName is "DownloadString" or "DownloadData" or "UploadString" or "UploadData";
    }

    /// <summary>
    ///     Determines if the invocation is a blocking HttpClient call.
    /// </summary>
    private static bool IsHttpClientBlockingCall(InvocationExpressionSyntax invocation)
    {
        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return false;

        var methodName = memberAccess.Name.Identifier.Text;

        return methodName is "GetStringAsync" or "GetByteArrayAsync" or "GetStreamAsync" or
            "GetAsync" or "PostAsync" or "PutAsync" or "DeleteAsync" or "SendAsync";
    }

    /// <summary>
    ///     Determines if the invocation is a blocking StreamReader/Writer call.
    /// </summary>
    private static bool IsStreamReaderWriterBlockingCall(InvocationExpressionSyntax invocation)
    {
        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return false;

        var methodName = memberAccess.Name.Identifier.Text;

        return methodName is "ReadToEnd" or "ReadLine" or "Read" or "ReadBlock" or
            "Write" or "WriteLine" or "Flush";
    }

    /// <summary>
    ///     Gets the method name from an invocation.
    /// </summary>
    private static string GetMethodName(InvocationExpressionSyntax invocation)
    {
        return invocation.Expression is MemberAccessExpressionSyntax memberAccess
            ? memberAccess.Name.Identifier.Text
            : "Unknown";
    }

    /// <summary>
    ///     Replaces .Result with await.
    /// </summary>
    private static async Task<Document> ReplaceResultWithAwaitAsync(
        Document document,
        MemberAccessExpressionSyntax memberAccess,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create await expression
        var awaitExpression = SyntaxFactory.AwaitExpression(memberAccess.Expression);

        // Replace the member access with await expression
        var newRoot = root.ReplaceNode(memberAccess, awaitExpression);

        // Make the containing method async
        var newRootWithAsync = await MakeMethodAsyncAsync(newRoot, memberAccess, cancellationToken);

        return document.WithSyntaxRoot(newRootWithAsync);
    }

    /// <summary>
    ///     Replaces .Result with await and ConfigureAwait(false).
    /// </summary>
    private static async Task<Document> ReplaceResultWithAwaitConfigureAwaitAsync(
        Document document,
        MemberAccessExpressionSyntax memberAccess,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create await expression with ConfigureAwait(false)
        var awaitExpression = SyntaxFactory.AwaitExpression(memberAccess.Expression);

        var configureAwaitInvocation = SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    awaitExpression,
                    SyntaxFactory.IdentifierName("ConfigureAwait")))
            .WithArgumentList(SyntaxFactory.ArgumentList(
                SyntaxFactory.SeparatedList(
                [
                    SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(SyntaxKind.FalseLiteralExpression)),
                ])));

        // Replace the member access with await expression
        var newRoot = root.ReplaceNode(memberAccess, configureAwaitInvocation);

        // Make the containing method async
        var newRootWithAsync = await MakeMethodAsyncAsync(newRoot, memberAccess, cancellationToken);

        return document.WithSyntaxRoot(newRootWithAsync);
    }

    /// <summary>
    ///     Replaces .Wait() with await.
    /// </summary>
    private static async Task<Document> ReplaceWaitWithAwaitAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return document;

        // Create await expression
        var awaitExpression = SyntaxFactory.AwaitExpression(memberAccess.Expression);

        // Replace the invocation with await expression
        var newRoot = root.ReplaceNode(invocation, awaitExpression);

        // Make the containing method async
        var newRootWithAsync = await MakeMethodAsyncAsync(newRoot, invocation, cancellationToken);

        return document.WithSyntaxRoot(newRootWithAsync);
    }

    /// <summary>
    ///     Replaces GetResult() with await.
    /// </summary>
    private static async Task<Document> ReplaceGetResultWithAwaitAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess ||
            memberAccess.Expression == null)
            return document;

        // Create await expression
        var awaitExpression = SyntaxFactory.AwaitExpression(memberAccess.Expression);

        // Replace the invocation with await expression
        var newRoot = root.ReplaceNode(invocation, awaitExpression);

        // Make the containing method async
        var newRootWithAsync = await MakeMethodAsyncAsync(newRoot, invocation, cancellationToken);

        return document.WithSyntaxRoot(newRootWithAsync);
    }

    /// <summary>
    ///     Replaces Thread.Sleep() with Task.Delay().
    /// </summary>
    private static async Task<Document> ReplaceThreadSleepWithTaskDelayAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        if (invocation.ArgumentList?.Arguments.Count != 1)
            return document;

        var delayArgument = invocation.ArgumentList.Arguments[0];

        // Create Task.Delay() invocation
        var taskDelayInvocation = SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName("Task"),
                    SyntaxFactory.IdentifierName("Delay")))
            .WithArgumentList(SyntaxFactory.ArgumentList(
                SyntaxFactory.SeparatedList([delayArgument])));

        // Create await expression
        var awaitExpression = SyntaxFactory.AwaitExpression(taskDelayInvocation);

        // Replace the invocation with await expression
        var newRoot = root.ReplaceNode(invocation, awaitExpression);

        // Make the containing method async
        var newRootWithAsync = await MakeMethodAsyncAsync(newRoot, invocation, cancellationToken);

        return document.WithSyntaxRoot(newRootWithAsync);
    }

    /// <summary>
    ///     Replaces File I/O with async version.
    /// </summary>
    private static async Task<Document> ReplaceFileIOWithAsyncAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return document;

        var methodName = memberAccess.Name.Identifier.Text;
        var asyncMethodName = methodName + "Async";

        // Create async File method invocation
        var asyncInvocation = SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName("File"),
                    SyntaxFactory.IdentifierName(asyncMethodName)))
            .WithArgumentList(invocation.ArgumentList);

        // Create await expression
        var awaitExpression = SyntaxFactory.AwaitExpression(asyncInvocation);

        // Replace the invocation with await expression
        var newRoot = root.ReplaceNode(invocation, awaitExpression);

        // Make the containing method async
        var newRootWithAsync = await MakeMethodAsyncAsync(newRoot, invocation, cancellationToken);

        return document.WithSyntaxRoot(newRootWithAsync);
    }

    /// <summary>
    ///     Replaces WebClient blocking with async version.
    /// </summary>
    private static async Task<Document> ReplaceWebClientWithAsyncAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return document;

        var methodName = memberAccess.Name.Identifier.Text;
        var asyncMethodName = methodName + "Async";

        // Create async WebClient method invocation
        var asyncInvocation = SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    memberAccess.Expression,
                    SyntaxFactory.IdentifierName(asyncMethodName)))
            .WithArgumentList(invocation.ArgumentList);

        // Create await expression
        var awaitExpression = SyntaxFactory.AwaitExpression(asyncInvocation);

        // Replace the invocation with await expression
        var newRoot = root.ReplaceNode(invocation, awaitExpression);

        // Make the containing method async
        var newRootWithAsync = await MakeMethodAsyncAsync(newRoot, invocation, cancellationToken);

        return document.WithSyntaxRoot(newRootWithAsync);
    }

    /// <summary>
    ///     Adds await to HttpClient call.
    /// </summary>
    private static async Task<Document> AddAwaitToHttpClientCallAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create await expression
        var awaitExpression = SyntaxFactory.AwaitExpression(invocation);

        // Replace the invocation with await expression
        var newRoot = root.ReplaceNode(invocation, awaitExpression);

        // Make the containing method async
        var newRootWithAsync = await MakeMethodAsyncAsync(newRoot, invocation, cancellationToken);

        return document.WithSyntaxRoot(newRootWithAsync);
    }

    /// <summary>
    ///     Replaces StreamReader/Writer with async version.
    /// </summary>
    private static async Task<Document> ReplaceStreamReaderWriterWithAsyncAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return document;

        var methodName = memberAccess.Name.Identifier.Text;
        var asyncMethodName = methodName + "Async";

        // Create async StreamReader/Writer method invocation
        var asyncInvocation = SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    memberAccess.Expression,
                    SyntaxFactory.IdentifierName(asyncMethodName)))
            .WithArgumentList(invocation.ArgumentList);

        // Create await expression
        var awaitExpression = SyntaxFactory.AwaitExpression(asyncInvocation);

        // Replace the invocation with await expression
        var newRoot = root.ReplaceNode(invocation, awaitExpression);

        // Make the containing method async
        var newRootWithAsync = await MakeMethodAsyncAsync(newRoot, invocation, cancellationToken);

        return document.WithSyntaxRoot(newRootWithAsync);
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
        var newRoot = root.ReplaceNode(methodDeclaration, newMethodDeclaration);

        return Task.FromResult(newRoot);
    }
}
