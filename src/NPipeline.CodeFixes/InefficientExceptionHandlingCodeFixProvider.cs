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
///     Code fix provider that suggests improvements for inefficient exception handling patterns.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(InefficientExceptionHandlingCodeFixProvider))]
[Shared]
public sealed class InefficientExceptionHandlingCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [InefficientExceptionHandlingAnalyzer.InefficientExceptionHandlingId];

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

        // Find node identified by diagnostic
        var node = root.FindNode(diagnosticSpan);

        if (node == null)
            return;

        // Register different code fixes based on diagnostic type
        if (node is CatchClauseSyntax catchClause)
            await RegisterCatchClauseFixes(context, catchClause, diagnostic);
    }

    private static async Task RegisterCatchClauseFixes(
        CodeFixContext context,
        CatchClauseSyntax catchClause,
        Diagnostic diagnostic)
    {
        var semanticModel = await context.Document.GetSemanticModelAsync(context.CancellationToken);

        if (semanticModel == null)
            return;

        var diagnosticMessage = diagnostic.GetMessage();

        if (diagnosticMessage.Contains("Catch-all exception handler"))
            await RegisterCatchAllFixes(context, catchClause, diagnostic);
        else if (diagnosticMessage.Contains("Exception swallowing pattern"))
            await RegisterSwallowingFixes(context, catchClause, diagnostic);
        else if (diagnosticMessage.Contains("Empty catch block"))
            await RegisterEmptyCatchFixes(context, catchClause, diagnostic);
        else if (diagnosticMessage.Contains("Improper re-throw pattern"))
            await RegisterRethrowFixes(context, catchClause, diagnostic);
        else if (diagnosticMessage.Contains("Inefficient exception filtering"))
            await RegisterFilterFixes(context, catchClause, diagnostic);
    }

    private static Task RegisterCatchAllFixes(
        CodeFixContext context,
        CatchClauseSyntax catchClause,
        Diagnostic diagnostic)
    {
        // Fix 1: Replace with specific exception types
        context.RegisterCodeFix(
            CodeAction.Create(
                "Replace with specific exception type",
                cancellationToken => ReplaceWithSpecificExceptionAsync(context.Document, catchClause, cancellationToken),
                nameof(InefficientExceptionHandlingCodeFixProvider)),
            diagnostic);

        // Fix 2: Add logging before re-throw
        context.RegisterCodeFix(
            CodeAction.Create(
                "Add logging and re-throw",
                cancellationToken => AddLoggingAndRethrowAsync(context.Document, catchClause, cancellationToken),
                nameof(InefficientExceptionHandlingCodeFixProvider)),
            diagnostic);

        return Task.CompletedTask;
    }

    private static Task RegisterSwallowingFixes(
        CodeFixContext context,
        CatchClauseSyntax catchClause,
        Diagnostic diagnostic)
    {
        // Fix 1: Add proper exception handling
        context.RegisterCodeFix(
            CodeAction.Create(
                "Add proper exception handling",
                cancellationToken => AddProperHandlingAsync(context.Document, catchClause, cancellationToken),
                nameof(InefficientExceptionHandlingCodeFixProvider)),
            diagnostic);

        // Fix 2: Re-throw exception
        context.RegisterCodeFix(
            CodeAction.Create(
                "Re-throw exception",
                cancellationToken => AddRethrowAsync(context.Document, catchClause, cancellationToken),
                nameof(InefficientExceptionHandlingCodeFixProvider)),
            diagnostic);

        return Task.CompletedTask;
    }

    private static Task RegisterEmptyCatchFixes(
        CodeFixContext context,
        CatchClauseSyntax catchClause,
        Diagnostic diagnostic)
    {
        // Fix 1: Add comment explaining empty catch
        context.RegisterCodeFix(
            CodeAction.Create(
                "Add explanatory comment",
                cancellationToken => AddExplanatoryCommentAsync(context.Document, catchClause, cancellationToken),
                nameof(InefficientExceptionHandlingCodeFixProvider)),
            diagnostic);

        // Fix 2: Add re-throw
        context.RegisterCodeFix(
            CodeAction.Create(
                "Add re-throw",
                cancellationToken => AddRethrowAsync(context.Document, catchClause, cancellationToken),
                nameof(InefficientExceptionHandlingCodeFixProvider)),
            diagnostic);

        return Task.CompletedTask;
    }

    private static Task RegisterRethrowFixes(
        CodeFixContext context,
        CatchClauseSyntax catchClause,
        Diagnostic diagnostic)
    {
        // Fix: Replace throw ex; with throw;
        context.RegisterCodeFix(
            CodeAction.Create(
                "Use proper re-throw to preserve stack trace",
                cancellationToken => FixRethrowAsync(context.Document, catchClause, cancellationToken),
                nameof(InefficientExceptionHandlingCodeFixProvider)),
            diagnostic);

        return Task.CompletedTask;
    }

    private static Task RegisterFilterFixes(
        CodeFixContext context,
        CatchClauseSyntax catchClause,
        Diagnostic diagnostic)
    {
        // Fix: Suggest removing string-based filtering
        context.RegisterCodeFix(
            CodeAction.Create(
                "Remove inefficient exception filter",
                cancellationToken => RemoveFilterAsync(context.Document, catchClause, cancellationToken),
                nameof(InefficientExceptionHandlingCodeFixProvider)),
            diagnostic);

        return Task.CompletedTask;
    }

    private static async Task<Document> ReplaceWithSpecificExceptionAsync(
        Document document,
        CatchClauseSyntax catchClause,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Replace catch (Exception ex) with catch (IOException ex) as an example
        var specificExceptionType = SyntaxFactory.ParseTypeName("IOException");

        var newCatchClause = catchClause.WithDeclaration(
            SyntaxFactory.CatchDeclaration(
                specificExceptionType,
                catchClause.Declaration?.Identifier ?? SyntaxFactory.Identifier("ex")));

        var newRoot = root.ReplaceNode(catchClause, newCatchClause);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> AddLoggingAndRethrowAsync(
        Document document,
        CatchClauseSyntax catchClause,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Add logging statement before re-throw
        var loggerStatement = SyntaxFactory.ParseStatement("_logger.LogError(ex, \"An error occurred\");");
        var rethrowStatement = SyntaxFactory.ParseStatement("throw;");

        var newBlock = catchClause.Block;

        if (catchClause.Block != null)
        {
            var statements = catchClause.Block.Statements;

            var newStatements = new List<StatementSyntax>
            {
                loggerStatement,
                rethrowStatement,
            };

            newStatements.AddRange(statements);

            newBlock = catchClause.Block.WithStatements(SyntaxFactory.List(newStatements));
        }

        var newCatchClause = catchClause.WithBlock(newBlock);
        var newRoot = root.ReplaceNode(catchClause, newCatchClause);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> AddProperHandlingAsync(
        Document document,
        CatchClauseSyntax catchClause,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Add proper handling statement
        var handlingStatement = SyntaxFactory.ParseStatement("_logger.LogError(ex, \"Exception handled appropriately\");");

        var newBlock = catchClause.Block;

        if (catchClause.Block != null)
        {
            var statements = catchClause.Block.Statements;

            var newStatements = new List<StatementSyntax>
            {
                handlingStatement,
            };

            newStatements.AddRange(statements);

            newBlock = catchClause.Block.WithStatements(SyntaxFactory.List(newStatements));
        }

        var newCatchClause = catchClause.WithBlock(newBlock);
        var newRoot = root.ReplaceNode(catchClause, newCatchClause);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> AddRethrowAsync(
        Document document,
        CatchClauseSyntax catchClause,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Add re-throw statement
        var rethrowStatement = SyntaxFactory.ParseStatement("throw;");

        var newBlock = catchClause.Block;

        if (catchClause.Block != null)
        {
            var statements = catchClause.Block.Statements;

            var newStatements = new List<StatementSyntax>(statements)
            {
                rethrowStatement,
            };

            newBlock = catchClause.Block.WithStatements(SyntaxFactory.List(newStatements));
        }
        else
        {
            // Create new block with re-throw
            newBlock = SyntaxFactory.Block(rethrowStatement);
        }

        var newCatchClause = catchClause.WithBlock(newBlock);
        var newRoot = root.ReplaceNode(catchClause, newCatchClause);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> AddExplanatoryCommentAsync(
        Document document,
        CatchClauseSyntax catchClause,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Add proper exception handling with logging
        var comment = SyntaxFactory.Comment("// Consider adding proper exception handling: logging, recovery logic, or re-throw with context");
        var newBlock = catchClause.Block;

        if (catchClause.Block != null)
        {
            var statements = catchClause.Block.Statements;

            var newStatements = new List<StatementSyntax>
            {
                SyntaxFactory.EmptyStatement()
                    .WithLeadingTrivia(SyntaxFactory.TriviaList(comment)),
            };

            newStatements.AddRange(statements);

            newBlock = catchClause.Block.WithStatements(SyntaxFactory.List(newStatements));
        }
        else
        {
            // Create new block with comment
            newBlock = SyntaxFactory.Block(
                SyntaxFactory.EmptyStatement()
                    .WithLeadingTrivia(SyntaxFactory.TriviaList(comment)));
        }

        var newCatchClause = catchClause.WithBlock(newBlock);
        var newRoot = root.ReplaceNode(catchClause, newCatchClause);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> FixRethrowAsync(
        Document document,
        CatchClauseSyntax catchClause,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Find and replace throw ex; with throw;
        var throwStatements = catchClause.Block?.DescendantNodes().OfType<ThrowStatementSyntax>() ?? [];

        foreach (var throwStatement in throwStatements)
        {
            if (throwStatement.Expression != null)
            {
                // Replace with proper re-throw
                var properRethrow = SyntaxFactory.ThrowStatement();
                var newRoot = root.ReplaceNode(throwStatement, properRethrow);
                root = newRoot;
            }
        }

        return document.WithSyntaxRoot(root);
    }

    private static async Task<Document> RemoveFilterAsync(
        Document document,
        CatchClauseSyntax catchClause,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Remove the filter from the catch clause
        var newCatchClause = catchClause.WithFilter(null);
        var newRoot = root.ReplaceNode(catchClause, newCatchClause);
        return document.WithSyntaxRoot(newRoot);
    }
}
