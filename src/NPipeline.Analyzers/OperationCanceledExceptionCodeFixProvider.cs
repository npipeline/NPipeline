using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace NPipeline.Analyzers;

/// <summary>
///     Code fix provider that suggests improvements for proper OperationCanceledException handling.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(OperationCanceledExceptionCodeFixProvider))]
[Shared]
public sealed class OperationCanceledExceptionCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [OperationCanceledExceptionAnalyzer.SwallowingOperationCanceledExceptionId];

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

        // Find the catch clause identified by diagnostic
        if (root.FindNode(diagnosticSpan) is not CatchClauseSyntax catchClause)
            return;

        // Get the exception type being caught
        var exceptionType = catchClause.Declaration?.Type;

        if (exceptionType == null)
            return;

        var semanticModel = await context.Document.GetSemanticModelAsync(context.CancellationToken);
        var exceptionTypeInfo = semanticModel.GetTypeInfo(exceptionType);
        var exceptionTypeSymbol = exceptionTypeInfo.Type;

        if (exceptionTypeSymbol == null)
            return;

        // Register different code fixes based on the exception type
        if (exceptionTypeSymbol.Name == "Exception" && exceptionTypeSymbol.ContainingNamespace?.Name == "System")
            await RegisterExceptionFixes(context, catchClause, diagnostic);
        else if (exceptionTypeSymbol.Name == "OperationCanceledException" && exceptionTypeSymbol.ContainingNamespace?.Name == "System")
            await RegisterOperationCanceledExceptionFixes(context, catchClause, diagnostic);
        else if (exceptionTypeSymbol.Name == "AggregateException" && exceptionTypeSymbol.ContainingNamespace?.Name == "System")
            await RegisterAggregateExceptionFixes(context, catchClause, diagnostic);
    }

    /// <summary>
    ///     Registers code fixes for general Exception catch blocks.
    /// </summary>
    private static Task RegisterExceptionFixes(
        CodeFixContext context,
        CatchClauseSyntax catchClause,
        Diagnostic diagnostic)
    {
        // Register code fix to add specific OperationCanceledException catch before general Exception
        context.RegisterCodeFix(
            CodeAction.Create(
                "Add OperationCanceledException catch before Exception",
                ct => AddOperationCanceledExceptionCatchAsync(context.Document, catchClause, ct),
                nameof(OperationCanceledExceptionCodeFixProvider) + "_AddOperationCanceledExceptionCatch"),
            diagnostic);

        // Register code fix to add conditional re-throw in existing catch
        context.RegisterCodeFix(
            CodeAction.Create(
                "Add conditional re-throw for OperationCanceledException",
                ct => AddConditionalReThrowAsync(context.Document, catchClause, ct),
                nameof(OperationCanceledExceptionCodeFixProvider) + "_AddConditionalReThrow"),
            diagnostic);

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Registers code fixes for OperationCanceledException catch blocks.
    /// </summary>
    private static Task RegisterOperationCanceledExceptionFixes(
        CodeFixContext context,
        CatchClauseSyntax catchClause,
        Diagnostic diagnostic)
    {
        // Register code fix to add re-throw statement
        context.RegisterCodeFix(
            CodeAction.Create(
                "Add re-throw for OperationCanceledException",
                ct => AddReThrowAsync(context.Document, catchClause, ct),
                nameof(OperationCanceledExceptionCodeFixProvider) + "_AddReThrow"),
            diagnostic);

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Registers code fixes for AggregateException catch blocks.
    /// </summary>
    private static Task RegisterAggregateExceptionFixes(
        CodeFixContext context,
        CatchClauseSyntax catchClause,
        Diagnostic diagnostic)
    {
        // Register code fix to add Flatten and Handle for OperationCanceledException
        context.RegisterCodeFix(
            CodeAction.Create(
                "Add OperationCanceledException handling to AggregateException",
                ct => AddAggregateExceptionHandlingAsync(context.Document, catchClause, ct),
                nameof(OperationCanceledExceptionCodeFixProvider) + "_AddAggregateExceptionHandling"),
            diagnostic);

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Adds a specific OperationCanceledException catch before a general Exception catch.
    /// </summary>
    private static async Task<Document> AddOperationCanceledExceptionCatchAsync(
        Document document,
        CatchClauseSyntax existingCatchClause,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create OperationCanceledException catch clause
        var operationCanceledExceptionType = SyntaxFactory.ParseTypeName("System.OperationCanceledException");
        var operationCanceledExceptionIdentifier = SyntaxFactory.Identifier("oce");

        var operationCanceledExceptionDeclaration = SyntaxFactory.CatchDeclaration(
            operationCanceledExceptionType,
            operationCanceledExceptionIdentifier);

        // Create throw statement to re-throw exception
        var throwStatement = SyntaxFactory.ThrowStatement();

        // Create catch block
        var operationCanceledCatchClause = SyntaxFactory.CatchClause(
            operationCanceledExceptionDeclaration,
            null,
            SyntaxFactory.Block(throwStatement));

        // Find the parent try statement
        var tryStatement = existingCatchClause.FirstAncestorOrSelf<TryStatementSyntax>();

        if (tryStatement == null)
            return document;

        // Insert the new catch clause before the existing one
        var newCatchClauses = tryStatement.Catches.Insert(0, operationCanceledCatchClause);
        var newTryStatement = tryStatement.WithCatches(newCatchClauses);

        // Replace the try statement
        var newRoot = root.ReplaceNode(tryStatement, newTryStatement);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Adds a conditional re-throw for OperationCanceledException in an existing catch block.
    /// </summary>
    private static async Task<Document> AddConditionalReThrowAsync(
        Document document,
        CatchClauseSyntax catchClause,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Get the exception variable name
        var exceptionIdentifier = catchClause.Declaration?.Identifier.Text ?? "ex";

        // Create an if statement to check for OperationCanceledException
        var isOperationCanceledExceptionExpression = SyntaxFactory.IsPatternExpression(
            SyntaxFactory.IdentifierName(exceptionIdentifier),
            SyntaxFactory.DeclarationPattern(
                SyntaxFactory.ParseTypeName("System.OperationCanceledException"),
                SyntaxFactory.DiscardDesignation()));

        var throwStatement = SyntaxFactory.ThrowStatement();
        var ifStatement = SyntaxFactory.IfStatement(isOperationCanceledExceptionExpression, throwStatement);

        // Get the catch block
        var catchBlock = catchClause.Block;

        if (catchBlock == null)
            return document;

        // Add the if statement at the beginning of the catch block
        var newStatements = catchBlock.Statements.Insert(0, ifStatement);
        var newCatchBlock = catchBlock.WithStatements(newStatements);
        var newCatchClause = catchClause.WithBlock(newCatchBlock);

        // Replace the catch clause
        var newRoot = root.ReplaceNode(catchClause, newCatchClause);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Adds a re-throw statement to an OperationCanceledException catch block.
    /// </summary>
    private static async Task<Document> AddReThrowAsync(
        Document document,
        CatchClauseSyntax catchClause,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create throw statement
        var throwStatement = SyntaxFactory.ThrowStatement();

        // Get the catch block
        var catchBlock = catchClause.Block;

        if (catchBlock == null)
            return document;

        // Add the throw statement at the end of the catch block
        var newStatements = catchBlock.Statements.Add(throwStatement);
        var newCatchBlock = catchBlock.WithStatements(newStatements);
        var newCatchClause = catchClause.WithBlock(newCatchBlock);

        // Replace the catch clause
        var newRoot = root.ReplaceNode(catchClause, newCatchClause);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Adds OperationCanceledException handling to an AggregateException catch block.
    /// </summary>
    private static async Task<Document> AddAggregateExceptionHandlingAsync(
        Document document,
        CatchClauseSyntax catchClause,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Get the exception variable name
        var exceptionIdentifier = catchClause.Declaration?.Identifier.Text ?? "aex";

        // Create a foreach statement to iterate through inner exceptions
        var innerExceptionIdentifier = SyntaxFactory.Identifier("innerEx");

        var innerExceptionsExpression = SyntaxFactory.InvocationExpression(
            SyntaxFactory.MemberAccessExpression(
                SyntaxKind.SimpleMemberAccessExpression,
                SyntaxFactory.IdentifierName(exceptionIdentifier),
                SyntaxFactory.IdentifierName("InnerExceptions")));

        var forEachStatement = SyntaxFactory.ForEachStatement(
            SyntaxFactory.ParseTypeName("System.Exception"),
            innerExceptionIdentifier,
            innerExceptionsExpression,
            SyntaxFactory.Block(

                // Create an if statement to check for OperationCanceledException
                SyntaxFactory.IfStatement(
                    SyntaxFactory.IsPatternExpression(
                        SyntaxFactory.IdentifierName(innerExceptionIdentifier),
                        SyntaxFactory.DeclarationPattern(
                            SyntaxFactory.ParseTypeName("System.OperationCanceledException"),
                            SyntaxFactory.DiscardDesignation())),
                    SyntaxFactory.ThrowStatement())));

        // Get the catch block
        var catchBlock = catchClause.Block;

        if (catchBlock == null)
            return document;

        // Add the foreach statement at the beginning of the catch block
        var newStatements = catchBlock.Statements.Insert(0, forEachStatement);
        var newCatchBlock = catchBlock.WithStatements(newStatements);
        var newCatchClause = catchClause.WithBlock(newCatchBlock);

        // Replace the catch clause
        var newRoot = root.ReplaceNode(catchClause, newCatchClause);

        return document.WithSyntaxRoot(newRoot);
    }
}
