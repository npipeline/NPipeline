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
///     Code fix provider that suggests proper async patterns for synchronous over async anti-patterns.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(SynchronousOverAsyncCodeFixProvider))]
[Shared]
public sealed class SynchronousOverAsyncCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [SynchronousOverAsyncAnalyzer.SynchronousOverAsyncId];

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
                nameof(SynchronousOverAsyncCodeFixProvider) + "_ResultToAwait"),
            diagnostic);

        // Register code fix to replace .Result with ConfigureAwait(false)
        context.RegisterCodeFix(
            CodeAction.Create(
                "Replace .Result with await and ConfigureAwait(false)",
                cancellationToken => ReplaceResultWithAwaitConfigureAwaitAsync(context.Document, memberAccess, cancellationToken),
                nameof(SynchronousOverAsyncCodeFixProvider) + "_ResultToAwaitConfigureAwait"),
            diagnostic);

        return Task.CompletedTask;
    }

    private static Task RegisterInvocationFixes(
        CodeFixContext context,
        InvocationExpressionSyntax invocation,
        Diagnostic diagnostic)
    {
        // Check for specific patterns
        if (IsTaskWaitInvocation(invocation))
        {
            // Register code fix to replace .Wait() with await
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Replace .Wait() with await",
                    cancellationToken => ReplaceWaitWithAwaitAsync(context.Document, invocation, cancellationToken),
                    nameof(SynchronousOverAsyncCodeFixProvider) + "_WaitToAwait"),
                diagnostic);
        }
        else if (IsGetResultInvocation(invocation))
        {
            // Register code fix to replace GetResult() with await
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Replace GetResult() with await",
                    cancellationToken => ReplaceGetResultWithAwaitAsync(context.Document, invocation, cancellationToken),
                    nameof(SynchronousOverAsyncCodeFixProvider) + "_GetResultToAwait"),
                diagnostic);
        }
        else if (IsTaskRunInvocation(invocation))
        {
            // Register code fix to remove Task.Run() in async method
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Remove Task.Run() in async method",
                    cancellationToken => RemoveTaskRunAsync(context.Document, invocation, cancellationToken),
                    nameof(SynchronousOverAsyncCodeFixProvider) + "_RemoveTaskRun"),
                diagnostic);
        }
        else
        {
            // Register code fix to add await to async method call
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Add await to async method call",
                    cancellationToken => AddAwaitToAsyncCallAsync(context.Document, invocation, cancellationToken),
                    nameof(SynchronousOverAsyncCodeFixProvider) + "_AddAwait"),
                diagnostic);

            // Register code fix to add await with ConfigureAwait(false)
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Add await with ConfigureAwait(false)",
                    cancellationToken => AddAwaitConfigureAwaitAsync(context.Document, invocation, cancellationToken),
                    nameof(SynchronousOverAsyncCodeFixProvider) + "_AddAwaitConfigureAwait"),
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
    ///     Determines if the invocation is a Task.Run() call.
    /// </summary>
    private static bool IsTaskRunInvocation(InvocationExpressionSyntax invocation)
    {
        return invocation.Expression is MemberAccessExpressionSyntax memberAccess &&
               memberAccess.Name.Identifier.Text == "Run" &&
               memberAccess.Expression is IdentifierNameSyntax { Identifier.Text: "Task" };
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
    ///     Removes Task.Run() in async method.
    /// </summary>
    private static async Task<Document> RemoveTaskRunAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        if (invocation.ArgumentList?.Arguments.Count != 1)
            return document;

        var firstArgument = invocation.ArgumentList.Arguments[0];

        if (firstArgument.Expression is not LambdaExpressionSyntax lambda)
            return document;

        // Extract the lambda body
        var lambdaBody = lambda.Body;
        StatementSyntax replacementStatement;

        if (lambdaBody is ExpressionSyntax expression)
            replacementStatement = SyntaxFactory.ExpressionStatement(expression);
        else if (lambdaBody is StatementSyntax statement)
            replacementStatement = statement;
        else
            return document;

        // Find the containing statement
        var containingStatement = invocation.FirstAncestorOrSelf<StatementSyntax>();

        if (containingStatement == null)
            return document;

        // Replace the containing statement with the lambda body
        var newRoot = root.ReplaceNode(containingStatement, replacementStatement);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Adds await to async method call.
    /// </summary>
    private static async Task<Document> AddAwaitToAsyncCallAsync(
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
    ///     Adds await with ConfigureAwait(false) to async method call.
    /// </summary>
    private static async Task<Document> AddAwaitConfigureAwaitAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create await expression with ConfigureAwait(false)
        var awaitExpression = SyntaxFactory.AwaitExpression(invocation);

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

        // Replace the invocation with await expression
        var newRoot = root.ReplaceNode(invocation, configureAwaitInvocation);

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
