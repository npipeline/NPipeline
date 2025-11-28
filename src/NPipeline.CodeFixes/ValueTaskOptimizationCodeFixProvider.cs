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
///     Code fix provider that suggests overriding ExecuteValueTaskAsync for TransformNode implementations.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(ValueTaskOptimizationCodeFixProvider))]
[Shared]
public sealed class ValueTaskOptimizationCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [ValueTaskOptimizationAnalyzer.MissingValueTaskOptimizationId];

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

        // Find the class declaration identified by diagnostic
        if (root.FindNode(diagnosticSpan) is not ClassDeclarationSyntax classDeclaration)
            return;

        // Register code fix to add ExecuteValueTaskAsync override
        context.RegisterCodeFix(
            CodeAction.Create(
                "Override ExecuteValueTaskAsync for better performance",
                cancellationToken => AddExecuteValueTaskAsyncOverrideAsync(context.Document, classDeclaration, cancellationToken),
                nameof(ValueTaskOptimizationCodeFixProvider)),
            diagnostic);
    }

    /// <summary>
    ///     Adds an ExecuteValueTaskAsync override to the class.
    /// </summary>
    private static async Task<Document> AddExecuteValueTaskAsyncOverrideAsync(
        Document document,
        ClassDeclarationSyntax classDeclaration,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Get the semantic model to analyze the existing ExecuteAsync method
        var semanticModel = await document.GetSemanticModelAsync(cancellationToken);

        if (semanticModel == null)
            return document;

        // Find the existing ExecuteAsync method to analyze its implementation
        var executeAsyncMethod = FindExecuteAsyncMethod(classDeclaration);

        if (executeAsyncMethod == null)
            return document;

        // Generate the ExecuteValueTaskAsync method based on ExecuteAsync
        var executeValueTaskAsyncMethod = GenerateExecuteValueTaskAsyncMethod(executeAsyncMethod);

        // Add the new method to the class
        var newClassDeclaration = classDeclaration.AddMembers(executeValueTaskAsyncMethod);

        // Replace the class in the syntax tree
        var newRoot = root.ReplaceNode(classDeclaration, newClassDeclaration);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Finds the ExecuteAsync method in the class.
    /// </summary>
    private static MethodDeclarationSyntax? FindExecuteAsyncMethod(
        ClassDeclarationSyntax classDeclaration)
    {
        foreach (var member in classDeclaration.Members)
        {
            if (member is MethodDeclarationSyntax methodDeclaration)
            {
                if (methodDeclaration.Identifier.Text == "ExecuteAsync")
                {
                    // Verify this is the TransformNode ExecuteAsync method with 3 parameters
                    if (methodDeclaration.ParameterList?.Parameters.Count == 3)
                        return methodDeclaration;
                }
            }
        }

        // Return null with proper error context to indicate method wasn't found
        // This allows the caller to handle the failure gracefully
        return null;
    }

    /// <summary>
    ///     Generates an ExecuteValueTaskAsync method based on the existing ExecuteAsync method.
    /// </summary>
    private static MethodDeclarationSyntax GenerateExecuteValueTaskAsyncMethod(MethodDeclarationSyntax executeAsyncMethod)
    {
        // Create parameters for ExecuteValueTaskAsync (same as ExecuteAsync)
        var parameters = executeAsyncMethod.ParameterList?.Parameters
            .Select(p => SyntaxFactory.Parameter(
                p.AttributeLists,
                p.Modifiers,
                p.Type,
                p.Identifier,
                p.Default))
            .ToList() ?? new List<ParameterSyntax>();

        // Create the parameter list
        var parameterList = SyntaxFactory.ParameterList(
            SyntaxFactory.SeparatedList(parameters));

        // Generate the method body based on ExecuteAsync implementation
        var methodBody = GenerateExecuteValueTaskAsyncBody(executeAsyncMethod);

        // Create the method declaration
        var methodDeclaration = SyntaxFactory.MethodDeclaration(
                SyntaxFactory.ParseTypeName("ValueTask"),
                SyntaxFactory.Identifier("ExecuteValueTaskAsync"))
            .WithModifiers(SyntaxFactory.TokenList(
                SyntaxFactory.Token(SyntaxKind.PublicKeyword),
                SyntaxFactory.Token(SyntaxKind.OverrideKeyword)))
            .WithParameterList(parameterList)
            .WithBody(methodBody);

        return methodDeclaration;
    }

    /// <summary>
    ///     Generates the body for ExecuteValueTaskAsync based on ExecuteAsync implementation.
    /// </summary>
    private static BlockSyntax GenerateExecuteValueTaskAsyncBody(MethodDeclarationSyntax executeAsyncMethod)
    {
        var statements = new List<StatementSyntax>();

        // If ExecuteAsync has a body, analyze it to generate optimized ValueTask version
        if (executeAsyncMethod.Body != null)
        {
            // Check if ExecuteAsync uses Task.FromResult
            var usesTaskFromResult = UsesTaskFromResult(executeAsyncMethod.Body);

            if (usesTaskFromResult)
            {
                // Generate optimized ValueTask.FromResult version
                var optimizedStatements = GenerateOptimizedStatements(executeAsyncMethod.Body);
                statements.AddRange(optimizedStatements);
            }
            else
            {
                // For complex async operations, just call ExecuteAsync and convert to ValueTask
                var callExecuteAsync = SyntaxFactory.ExpressionStatement(
                    SyntaxFactory.AwaitExpression(
                        SyntaxFactory.InvocationExpression(
                            SyntaxFactory.IdentifierName("ExecuteAsync"),
                            SyntaxFactory.ArgumentList(
                                SyntaxFactory.SeparatedList(
                                    executeAsyncMethod.ParameterList?.Parameters.Select(p =>
                                        SyntaxFactory.Argument(SyntaxFactory.IdentifierName(p.Identifier.Text))))))));

                statements.Add(callExecuteAsync);

                // Return ValueTask.CompletedTask if ExecuteAsync returns void
                var returnStatement = SyntaxFactory.ReturnStatement(
                    SyntaxFactory.MemberAccessExpression(
                        SyntaxKind.SimpleMemberAccessExpression,
                        SyntaxFactory.IdentifierName("ValueTask"),
                        SyntaxFactory.IdentifierName("CompletedTask")));

                statements.Add(returnStatement);
            }
        }
        else
        {
            // For abstract or interface implementations, generate a basic implementation
            statements.Add(SyntaxFactory.ReturnStatement(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName("ValueTask"),
                    SyntaxFactory.IdentifierName("CompletedTask"))));
        }

        return SyntaxFactory.Block(statements);
    }

    /// <summary>
    ///     Checks if the method body uses Task.FromResult.
    /// </summary>
    private static bool UsesTaskFromResult(BlockSyntax methodBody)
    {
        var walker = new TaskFromResultWalker();
        walker.Visit(methodBody);
        return walker.UsesTaskFromResult;
    }

    /// <summary>
    ///     Generates optimized statements by replacing Task.FromResult with ValueTask.FromResult.
    /// </summary>
    private static List<StatementSyntax> GenerateOptimizedStatements(BlockSyntax originalBody)
    {
        var statements = new List<StatementSyntax>();
        var rewriter = new TaskToValueTaskRewriter();

        foreach (var statement in originalBody.Statements)
        {
            var rewrittenStatement = rewriter.Visit(statement);

            if (rewrittenStatement is StatementSyntax stmt)
                statements.Add(stmt);
        }

        return statements;
    }

    /// <summary>
    ///     AST walker that detects if a method uses Task.FromResult.
    /// </summary>
    private sealed class TaskFromResultWalker : CSharpSyntaxWalker
    {
        public bool UsesTaskFromResult { get; private set; }

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (UsesTaskFromResult)
            {
                base.VisitInvocationExpression(node);
                return;
            }

            if (node.Expression is MemberAccessExpressionSyntax memberAccess)
            {
                var methodName = memberAccess.Name.Identifier.Text;

                // Check for Task.FromResult or Task<T>.FromResult
                if (methodName == "FromResult" &&
                    (memberAccess.Expression is IdentifierNameSyntax { Identifier.Text: "Task" } ||
                     memberAccess.Expression is GenericNameSyntax { Identifier.Text: "Task" }))
                    UsesTaskFromResult = true;
            }

            base.VisitInvocationExpression(node);
        }
    }

    /// <summary>
    ///     Syntax rewriter that converts Task.FromResult to ValueTask.FromResult.
    /// </summary>
    private sealed class TaskToValueTaskRewriter : CSharpSyntaxRewriter
    {
        public override SyntaxNode? VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            if (node.Expression is MemberAccessExpressionSyntax memberAccess)
            {
                var methodName = memberAccess.Name.Identifier.Text;

                // Replace Task.FromResult with ValueTask.FromResult
                if (methodName == "FromResult" &&
                    (memberAccess.Expression is IdentifierNameSyntax { Identifier.Text: "Task" } ||
                     memberAccess.Expression is GenericNameSyntax { Identifier.Text: "Task" }))
                {
                    // Create ValueTask.FromResult expression
                    var valueTaskFromResult = SyntaxFactory.InvocationExpression(
                        SyntaxFactory.MemberAccessExpression(
                            SyntaxKind.SimpleMemberAccessExpression,
                            SyntaxFactory.IdentifierName("ValueTask"),
                            SyntaxFactory.IdentifierName("FromResult")),
                        node.ArgumentList);

                    return valueTaskFromResult;
                }
            }

            return base.VisitInvocationExpression(node);
        }

        public override SyntaxNode? VisitReturnStatement(ReturnStatementSyntax node)
        {
            if (node.Expression != null)
            {
                var rewrittenExpression = Visit(node.Expression);
                return SyntaxFactory.ReturnStatement((ExpressionSyntax)rewrittenExpression);
            }

            return base.VisitReturnStatement(node);
        }
    }
}
