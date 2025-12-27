using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace NPipeline.CodeFixes;

/// <summary>
///     Provides Roslyn code fixes for NP9211 diagnostics on stream transform node execution strategies.
///     Registers fixes to replace object creation or assignments with batching or unbatching execution strategies.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(StreamTransformNodeExecutionStrategyCodeFixProvider))]
[Shared]
public class StreamTransformNodeExecutionStrategyCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public sealed override ImmutableArray<string> FixableDiagnosticIds =>
        ["NP9211"];

    /// <inheritdoc />
    public sealed override FixAllProvider GetFixAllProvider()
    {
        return WellKnownFixAllProviders.BatchFixer;
    }

    /// <inheritdoc />
    public sealed override async Task RegisterCodeFixesAsync(CodeFixContext context)
    {
        var root = await context.Document.GetSyntaxRootAsync(context.CancellationToken).ConfigureAwait(false);

        if (root == null)
            return;

        var diagnostic = context.Diagnostics.First();
        var diagnosticSpan = diagnostic.Location.SourceSpan;
        var node = root.FindNode(diagnosticSpan);

        if (node == null)
            return;

        var semanticModel = await context.Document.GetSemanticModelAsync(context.CancellationToken).ConfigureAwait(false);

        if (semanticModel == null)
            return;

        // Check for object creation expression
        if (node is ObjectCreationExpressionSyntax objectCreation)
            await RegisterObjectCreationFixes(context, objectCreation, semanticModel, diagnostic).ConfigureAwait(false);

        // Check for assignment expression
        else if (node is AssignmentExpressionSyntax assignment)
            await RegisterAssignmentFixes(context, assignment, semanticModel, diagnostic).ConfigureAwait(false);
    }

    private Task RegisterObjectCreationFixes(CodeFixContext context, ObjectCreationExpressionSyntax objectCreation, SemanticModel semanticModel,
        Diagnostic diagnostic)
    {
        var typeSymbol = semanticModel.GetTypeInfo(objectCreation.Type).Type;

        if (typeSymbol == null)
            return Task.CompletedTask;

        // Register code fix for BatchingExecutionStrategy
        context.RegisterCodeFix(
            CodeAction.Create(
                "Replace with BatchingExecutionStrategy",
                ct => ReplaceWithBatchingExecutionStrategyAsync(context.Document, objectCreation, ct),
                "ReplaceWithBatchingExecutionStrategy"),
            diagnostic);

        // Register code fix for UnbatchingExecutionStrategy
        context.RegisterCodeFix(
            CodeAction.Create(
                "Replace with UnbatchingExecutionStrategy",
                ct => ReplaceWithUnbatchingExecutionStrategyAsync(context.Document, objectCreation, ct),
                "ReplaceWithUnbatchingExecutionStrategy"),
            diagnostic);

        return Task.CompletedTask;
    }

    private Task RegisterAssignmentFixes(CodeFixContext context, AssignmentExpressionSyntax assignment, SemanticModel semanticModel,
        Diagnostic diagnostic)
    {
        var typeSymbol = semanticModel.GetTypeInfo(assignment.Right).Type;

        if (typeSymbol == null)
            return Task.CompletedTask;

        // Register code fix for BatchingExecutionStrategy
        context.RegisterCodeFix(
            CodeAction.Create(
                "Replace with BatchingExecutionStrategy",
                ct => ReplaceWithBatchingExecutionStrategyAsync(context.Document, assignment.Right, ct),
                "ReplaceWithBatchingExecutionStrategy"),
            diagnostic);

        // Register code fix for UnbatchingExecutionStrategy
        context.RegisterCodeFix(
            CodeAction.Create(
                "Replace with UnbatchingExecutionStrategy",
                ct => ReplaceWithUnbatchingExecutionStrategyAsync(context.Document, assignment.Right, ct),
                "ReplaceWithUnbatchingExecutionStrategy"),
            diagnostic);

        return Task.CompletedTask;
    }

    private async Task<Document> ReplaceWithBatchingExecutionStrategyAsync(Document document, SyntaxNode nodeToReplace, CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken).ConfigureAwait(false);

        if (root == null)
            return document;

        // Create the new BatchingExecutionStrategy with default parameters
        var batchSizeArgument = SyntaxFactory.Argument(
            SyntaxFactory.LiteralExpression(SyntaxKind.NumericLiteralExpression, SyntaxFactory.Literal(100)));

        var timeSpanArgument = SyntaxFactory.Argument(
            SyntaxFactory.MemberAccessExpression(
                SyntaxKind.SimpleMemberAccessExpression,
                SyntaxFactory.IdentifierName("TimeSpan"),
                SyntaxFactory.IdentifierName("FromSeconds")));

        var argumentList = SyntaxFactory.ArgumentList(
            SyntaxFactory.SeparatedList<ArgumentSyntax>(new[] { batchSizeArgument, timeSpanArgument }));

        var newExpression = SyntaxFactory.ObjectCreationExpression(
                SyntaxFactory.IdentifierName("BatchingExecutionStrategy"))
            .WithArgumentList(argumentList);

        var newRoot = root.ReplaceNode(nodeToReplace, newExpression);
        return document.WithSyntaxRoot(newRoot);
    }

    private async Task<Document> ReplaceWithUnbatchingExecutionStrategyAsync(Document document, SyntaxNode nodeToReplace, CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken).ConfigureAwait(false);

        if (root == null)
            return document;

        // Create the new UnbatchingExecutionStrategy with no parameters
        var newExpression = SyntaxFactory.ObjectCreationExpression(
                SyntaxFactory.IdentifierName("UnbatchingExecutionStrategy"))
            .WithArgumentList(SyntaxFactory.ArgumentList());

        var newRoot = root.ReplaceNode(nodeToReplace, newExpression);
        return document.WithSyntaxRoot(newRoot);
    }
}
