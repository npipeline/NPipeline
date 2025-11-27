using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace NPipeline.Analyzers;

/// <summary>
///     Code fix provider that suggests adding await to foreach loops for SinkNode implementations.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(SinkNodeInputConsumptionCodeFixProvider))]
[Shared]
public sealed class SinkNodeInputConsumptionCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [SinkNodeInputConsumptionAnalyzer.SinkNodeInputNotConsumedId];

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

        // Find the method declaration identified by diagnostic
        if (root.FindNode(diagnosticSpan) is not MethodDeclarationSyntax methodDeclaration)
            return;

        // Get the input parameter name
        var inputParameter = methodDeclaration.ParameterList?.Parameters.FirstOrDefault();

        if (inputParameter == null)
            return;

        var inputParameterName = inputParameter.Identifier.Text;

        // Register code fix to add await foreach
        context.RegisterCodeFix(
            CodeAction.Create(
                "Add await foreach to consume input",
                cancellationToken => AddAwaitForEachAsync(context.Document, methodDeclaration, inputParameterName, cancellationToken),
                nameof(SinkNodeInputConsumptionCodeFixProvider)),
            diagnostic);
    }

    /// <summary>
    ///     Adds an await foreach loop to consume input parameter.
    /// </summary>
    private static async Task<Document> AddAwaitForEachAsync(
        Document document,
        MethodDeclarationSyntax methodDeclaration,
        string inputParameterName,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create the await foreach statement
        var awaitForEachStatement = CreateAwaitForEachStatement(inputParameterName);

        // Add the await foreach to the method body
        var newMethodDeclaration = AddAwaitForEachToMethod(methodDeclaration, awaitForEachStatement);

        // Replace the method in the syntax tree
        var newRoot = root.ReplaceNode(methodDeclaration, newMethodDeclaration);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Creates an await foreach statement to consume input.
    /// </summary>
    private static ForEachStatementSyntax CreateAwaitForEachStatement(string inputParameterName)
    {
        // Create the await foreach statement using ParseFromText
        var awaitForEachText = $"await foreach (var item in {inputParameterName}) {{ }}";

        if (SyntaxFactory.ParseStatement(awaitForEachText) is ForEachStatementSyntax awaitForEachStatement)
            return awaitForEachStatement;

        // Fallback to a basic foreach statement
        var fallbackText = "await foreach (var item in input) { }";

        return SyntaxFactory.ParseStatement(fallbackText) as ForEachStatementSyntax
               ?? throw new InvalidOperationException("Failed to create foreach statement");
    }

    /// <summary>
    ///     Adds the await foreach statement to the method body.
    /// </summary>
    private static MethodDeclarationSyntax AddAwaitForEachToMethod(
        MethodDeclarationSyntax methodDeclaration,
        ForEachStatementSyntax awaitForEachStatement)
    {
        // If the method has no body, create one with the await foreach
        if (methodDeclaration.Body == null)
        {
            var body = SyntaxFactory.Block(awaitForEachStatement);
            return methodDeclaration.WithBody(body);
        }

        // If the method body is empty, just add the await foreach
        if (methodDeclaration.Body.Statements.Count == 0)
        {
            var newBody = methodDeclaration.Body.AddStatements(awaitForEachStatement);
            return methodDeclaration.WithBody(newBody);
        }

        // If the method has existing statements, add the await foreach at the beginning
        var newBodyWithAwait = methodDeclaration.Body.WithStatements(
            SyntaxFactory.List(new[] { awaitForEachStatement }.Concat(methodDeclaration.Body.Statements)));

        return methodDeclaration.WithBody(newBodyWithAwait);
    }
}
