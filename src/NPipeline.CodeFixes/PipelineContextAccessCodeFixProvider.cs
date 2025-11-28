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
///     Code fix provider that suggests safe access patterns for PipelineContext properties and dictionaries.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(PipelineContextAccessCodeFixProvider))]
[Shared]
public sealed class PipelineContextAccessCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [PipelineContextAccessAnalyzer.UnsafePipelineContextAccessId];

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

        // Register different code fixes based on node type
        if (node is MemberAccessExpressionSyntax memberAccess)
            await RegisterMemberAccessFixes(context, memberAccess, diagnostic);
        else if (node is ElementAccessExpressionSyntax elementAccess)
            await RegisterElementAccessFixes(context, elementAccess, diagnostic);
        else if (node is InvocationExpressionSyntax invocation)
            await RegisterInvocationFixes(context, invocation, diagnostic);
    }

    private static Task RegisterMemberAccessFixes(
        CodeFixContext context,
        MemberAccessExpressionSyntax memberAccess,
        Diagnostic diagnostic)
    {
        var propertyName = memberAccess.Name.Identifier.Text;

        // Register code fix to add null-conditional operator
        context.RegisterCodeFix(
            CodeAction.Create(
                $"Use null-conditional operator for '{propertyName}'",
                cancellationToken => AddNullConditionalOperatorAsync(context.Document, memberAccess, cancellationToken),
                nameof(PipelineContextAccessCodeFixProvider) + "_NullConditional"),
            diagnostic);

        // Register code fix to add null check
        context.RegisterCodeFix(
            CodeAction.Create(
                $"Add null check for '{propertyName}'",
                cancellationToken => AddNullCheckAsync(context.Document, memberAccess, cancellationToken),
                nameof(PipelineContextAccessCodeFixProvider) + "_NullCheck"),
            diagnostic);

        return Task.CompletedTask;
    }

    private static Task RegisterElementAccessFixes(
        CodeFixContext context,
        ElementAccessExpressionSyntax elementAccess,
        Diagnostic diagnostic)
    {
        // Register code fix to add null-conditional operator
        context.RegisterCodeFix(
            CodeAction.Create(
                "Use null-conditional operator for dictionary access",
                cancellationToken => AddNullConditionalToElementAccessAsync(context.Document, elementAccess, cancellationToken),
                nameof(PipelineContextAccessCodeFixProvider) + "_ElementNullConditional"),
            diagnostic);

        // Register code fix to use TryGetValue pattern
        context.RegisterCodeFix(
            CodeAction.Create(
                "Use TryGetValue pattern for safe dictionary access",
                cancellationToken => UseTryGetValuePatternAsync(context.Document, elementAccess, cancellationToken),
                nameof(PipelineContextAccessCodeFixProvider) + "_TryGetValue"),
            diagnostic);

        return Task.CompletedTask;
    }

    private static Task RegisterInvocationFixes(
        CodeFixContext context,
        InvocationExpressionSyntax invocation,
        Diagnostic diagnostic)
    {
        // Register code fix to add null-conditional operator
        context.RegisterCodeFix(
            CodeAction.Create(
                "Use null-conditional operator for method call",
                cancellationToken => AddNullConditionalToInvocationAsync(context.Document, invocation, cancellationToken),
                nameof(PipelineContextAccessCodeFixProvider) + "_InvocationNullConditional"),
            diagnostic);

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Adds null-conditional operator to member access.
    /// </summary>
    private static async Task<Document> AddNullConditionalOperatorAsync(
        Document document,
        MemberAccessExpressionSyntax memberAccess,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create null-conditional member access
        var nullConditionalAccess = SyntaxFactory.ConditionalAccessExpression(
            memberAccess.Expression,
            SyntaxFactory.MemberBindingExpression(memberAccess.Name));

        // Replace the original member access
        var newRoot = root.ReplaceNode(memberAccess, nullConditionalAccess);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Adds null check for member access.
    /// </summary>
    private static async Task<Document> AddNullCheckAsync(
        Document document,
        MemberAccessExpressionSyntax memberAccess,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Find the containing statement
        var containingStatement = memberAccess.FirstAncestorOrSelf<StatementSyntax>();

        if (containingStatement == null)
            return document;

        // Create null check condition
        var nullCheckCondition = SyntaxFactory.BinaryExpression(
            SyntaxKind.NotEqualsExpression,
            memberAccess.Expression,
            SyntaxFactory.LiteralExpression(SyntaxKind.NullLiteralExpression));

        // Create if statement with the original statement as body
        var ifStatement = SyntaxFactory.IfStatement(
            nullCheckCondition,
            containingStatement);

        // Replace the original statement with the if statement
        var newRoot = root.ReplaceNode(containingStatement, ifStatement);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Adds null-conditional operator to element access.
    /// </summary>
    private static async Task<Document> AddNullConditionalToElementAccessAsync(
        Document document,
        ElementAccessExpressionSyntax elementAccess,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create null-conditional element access
        var nullConditionalAccess = SyntaxFactory.ConditionalAccessExpression(
            elementAccess.Expression,
            SyntaxFactory.ElementBindingExpression(elementAccess.ArgumentList));

        // Replace the original element access
        var newRoot = root.ReplaceNode(elementAccess, nullConditionalAccess);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Converts element access to TryGetValue pattern.
    /// </summary>
    private static async Task<Document> UseTryGetValuePatternAsync(
        Document document,
        ElementAccessExpressionSyntax elementAccess,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Find the containing statement
        var containingStatement = elementAccess.FirstAncestorOrSelf<StatementSyntax>();

        if (containingStatement == null)
            return document;

        // Get the key expression from element access
        var keyExpression = elementAccess.ArgumentList?.Arguments.FirstOrDefault()?.Expression;

        if (keyExpression == null)
            return document;

        // Generate a unique variable name
        var variableName = "value";

        // Create TryGetValue invocation
        var tryGetValueInvocation = SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    elementAccess.Expression,
                    SyntaxFactory.IdentifierName("TryGetValue")))
            .WithArgumentList(SyntaxFactory.ArgumentList(
                SyntaxFactory.SeparatedList(
                [
                    SyntaxFactory.Argument(keyExpression),
                    SyntaxFactory.Argument(
                        SyntaxFactory.DeclarationExpression(
                            SyntaxFactory.ParseTypeName("var"),
                            SyntaxFactory.SingleVariableDesignation(
                                SyntaxFactory.Identifier(variableName)))),
                ])));

        // Create if statement with TryGetValue
        var ifStatement = SyntaxFactory.IfStatement(
            tryGetValueInvocation,
            containingStatement);

        // Replace the original statement with the if statement
        var newRoot = root.ReplaceNode(containingStatement, ifStatement);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Adds null-conditional operator to invocation.
    /// </summary>
    private static async Task<Document> AddNullConditionalToInvocationAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Check if this is a member access invocation
        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return document;

        // Create null-conditional invocation
        var nullConditionalInvocation = SyntaxFactory.ConditionalAccessExpression(
            memberAccess.Expression,
            SyntaxFactory.InvocationExpression(
                    SyntaxFactory.MemberBindingExpression(memberAccess.Name))
                .WithArgumentList(invocation.ArgumentList));

        // Replace the original invocation
        var newRoot = root.ReplaceNode(invocation, nullConditionalInvocation);

        return document.WithSyntaxRoot(newRoot);
    }
}
