using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Simplification;
using NPipeline.Analyzers;

namespace NPipeline.CodeFixes;

/// <summary>
///     Code fix provider that suggests improvements for proper cancellation token usage.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(CancellationTokenRespectCodeFixProvider))]
[Shared]
public sealed class CancellationTokenRespectCodeFixProvider : CodeFixProvider
{
    /// <summary>
    ///     Gets the fixable diagnostic IDs.
    /// </summary>
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [CancellationTokenRespectAnalyzer.CancellationTokenNotRespectedId];

    /// <summary>
    ///     Gets the fix all provider.
    /// </summary>
    public override FixAllProvider GetFixAllProvider()
    {
        return WellKnownFixAllProviders.BatchFixer;
    }

    /// <summary>
    ///     Registers code fixes for the specified context.
    /// </summary>
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

        // Register different code fixes based on the issue type
        if (node is MethodDeclarationSyntax methodDeclaration)
            await RegisterMethodFixes(context, methodDeclaration, diagnostic);
        else if (node is InvocationExpressionSyntax invocation)
            await RegisterInvocationFixes(context, invocation, diagnostic);
        else if (node is ForStatementSyntax or WhileStatementSyntax or DoStatementSyntax or ForEachStatementSyntax)
            await RegisterLoopFixes(context, node, diagnostic);
        else if (node is ParameterSyntax parameter)
            await RegisterParameterFixes(context, parameter, diagnostic);
    }

    /// <summary>
    ///     Registers code fixes for method-level issues.
    /// </summary>
    private static Task RegisterMethodFixes(
        CodeFixContext context,
        MethodDeclarationSyntax methodDeclaration,
        Diagnostic diagnostic)
    {
        // Check if method needs CancellationToken parameter
        var semanticModel = context.Document.GetSemanticModelAsync(context.CancellationToken).Result;

        var hasCancellationTokenParam = methodDeclaration.ParameterList.Parameters
            .Any(p => semanticModel.GetTypeInfo(p.Type!).Type?.Name == "CancellationToken");

        if (!hasCancellationTokenParam)
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Add CancellationToken parameter",
                    ct => AddCancellationTokenParameterAsync(context.Document, methodDeclaration, ct),
                    nameof(CancellationTokenRespectCodeFixProvider) + ".AddCancellationTokenParameter"),
                diagnostic);
        }

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Registers code fixes for invocation-level issues.
    /// </summary>
    private static Task RegisterInvocationFixes(
        CodeFixContext context,
        InvocationExpressionSyntax invocation,
        Diagnostic diagnostic)
    {
        context.RegisterCodeFix(
            CodeAction.Create(
                "Pass CancellationToken to async call",
                ct => PassCancellationTokenToInvocationAsync(context.Document, invocation, ct),
                nameof(CancellationTokenRespectCodeFixProvider) + ".PassCancellationTokenToInvocation"),
            diagnostic);

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Registers code fixes for loop-level issues.
    /// </summary>
    private static Task RegisterLoopFixes(
        CodeFixContext context,
        SyntaxNode loop,
        Diagnostic diagnostic)
    {
        context.RegisterCodeFix(
            CodeAction.Create(
                "Add cancellation check to loop",
                ct => AddCancellationCheckToLoopAsync(context.Document, loop, ct),
                nameof(CancellationTokenRespectCodeFixProvider) + ".AddCancellationCheckToLoop"),
            diagnostic);

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Registers code fixes for parameter-level issues.
    /// </summary>
    private static Task RegisterParameterFixes(
        CodeFixContext context,
        ParameterSyntax parameter,
        Diagnostic diagnostic)
    {
        context.RegisterCodeFix(
            CodeAction.Create(
                "Add [EnumeratorCancellation] attribute",
                ct => AddEnumeratorCancellationAttributeAsync(context.Document, parameter, ct),
                nameof(CancellationTokenRespectCodeFixProvider) + ".AddEnumeratorCancellationAttribute"),
            diagnostic);

        return Task.CompletedTask;
    }

    /// <summary>
    ///     Adds a CancellationToken parameter to a method.
    /// </summary>
    private static async Task<Document> AddCancellationTokenParameterAsync(
        Document document,
        MethodDeclarationSyntax methodDeclaration,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create CancellationToken parameter
        var cancellationTokenType = SyntaxFactory.ParseTypeName("System.Threading.CancellationToken")
            .WithAdditionalAnnotations(Simplifier.Annotation);

        var cancellationTokenParam = SyntaxFactory.Parameter(
                SyntaxFactory.Identifier("cancellationToken"))
            .WithType(cancellationTokenType);

        // Add parameter to parameter list
        var newParameterList = methodDeclaration.ParameterList.AddParameters(cancellationTokenParam);
        var newMethodDeclaration = methodDeclaration.WithParameterList(newParameterList);

        // Replace the method declaration
        var newRoot = root.ReplaceNode(methodDeclaration, newMethodDeclaration);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Passes CancellationToken to an async invocation.
    /// </summary>
    private static async Task<Document> PassCancellationTokenToInvocationAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Find the containing method to get the CancellationToken parameter name
        var containingMethod = invocation.FirstAncestorOrSelf<MethodDeclarationSyntax>();

        if (containingMethod == null)
            return document;

        var semanticModel = await document.GetSemanticModelAsync(cancellationToken);

        var cancellationTokenParam = containingMethod.ParameterList.Parameters
            .FirstOrDefault(p => semanticModel?.GetTypeInfo(p.Type!).Type?.Name == "CancellationToken");

        if (cancellationTokenParam == null)
            return document;

        // Create the CancellationToken argument
        var cancellationTokenArgument = SyntaxFactory.Argument(
            SyntaxFactory.IdentifierName(cancellationTokenParam.Identifier.Text));

        // Add the argument to the invocation
        var newArgumentList = invocation.ArgumentList.AddArguments(cancellationTokenArgument);
        var newInvocation = invocation.WithArgumentList(newArgumentList);

        // Replace the invocation
        var newRoot = root.ReplaceNode(invocation, newInvocation);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Adds a cancellation check to a loop.
    /// </summary>
    private static async Task<Document> AddCancellationCheckToLoopAsync(
        Document document,
        SyntaxNode loop,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Find the containing method to get the CancellationToken parameter name
        var containingMethod = loop.FirstAncestorOrSelf<MethodDeclarationSyntax>();

        if (containingMethod == null)
            return document;

        var semanticModel = await document.GetSemanticModelAsync(cancellationToken);

        var cancellationTokenParam = containingMethod.ParameterList.Parameters
            .FirstOrDefault(p => semanticModel?.GetTypeInfo(p.Type!).Type?.Name == "CancellationToken");

        if (cancellationTokenParam == null)
            return document;

        // Create the ThrowIfCancellationRequested() statement
        var throwIfCancellationRequested = SyntaxFactory.ExpressionStatement(
            SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName(cancellationTokenParam.Identifier.Text),
                    SyntaxFactory.IdentifierName("ThrowIfCancellationRequested"))));

        // Get the loop body
        var loopBody = loop switch
        {
            ForStatementSyntax forStmt => forStmt.Statement,
            WhileStatementSyntax whileStatement => whileStatement.Statement,
            DoStatementSyntax doStatement => doStatement.Statement,
            ForEachStatementSyntax forEachStatement => forEachStatement.Statement,
            _ => null,
        };

        if (loopBody == null)
            return document;

        // Add the cancellation check at the beginning of the loop
        StatementSyntax newLoopBody;

        if (loopBody is BlockSyntax block)
        {
            // Add to the beginning of the block
            var newBlock = block.WithStatements(block.Statements.Insert(0, throwIfCancellationRequested));
            newLoopBody = newBlock;
        }
        else
        {
            // Create a new block with the cancellation check and the original statement
            newLoopBody = SyntaxFactory.Block(
                throwIfCancellationRequested,
                loopBody);
        }

        // Replace the loop with the updated version
        SyntaxNode newLoop;

        if (loop is ForStatementSyntax forStatement)
            newLoop = forStatement.WithStatement(newLoopBody);
        else if (loop is WhileStatementSyntax whileStatement)
            newLoop = whileStatement.WithStatement(newLoopBody);
        else if (loop is DoStatementSyntax doStatement)
            newLoop = doStatement.WithStatement(newLoopBody);
        else if (loop is ForEachStatementSyntax forEachStatement)
            newLoop = forEachStatement.WithStatement(newLoopBody);
        else
            return document;

        var newRoot = root.ReplaceNode(loop, newLoop);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Adds [EnumeratorCancellation] attribute to a CancellationToken parameter.
    /// </summary>
    private static async Task<Document> AddEnumeratorCancellationAttributeAsync(
        Document document,
        ParameterSyntax parameter,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create the [EnumeratorCancellation] attribute
        var enumeratorCancellationAttribute = SyntaxFactory.Attribute(
            SyntaxFactory.ParseName("System.Runtime.CompilerServices.EnumeratorCancellation"));

        var attributeList = SyntaxFactory.AttributeList(
            SyntaxFactory.SingletonSeparatedList(enumeratorCancellationAttribute));

        // Add the attribute to the parameter
        var newParameter = parameter.AddAttributeLists(attributeList);

        // Replace the parameter
        var newRoot = root.ReplaceNode(parameter, newParameter);

        return document.WithSyntaxRoot(newRoot);
    }
}
