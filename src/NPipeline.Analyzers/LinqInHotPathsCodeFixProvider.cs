using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace NPipeline.Analyzers;

/// <summary>
///     Code fix provider that converts LINQ operations to imperative alternatives for better performance.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(LinqInHotPathsCodeFixProvider))]
[Shared]
public sealed class LinqInHotPathsCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [LinqInHotPathsAnalyzer.DiagnosticId];

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

        // Find invocation expression identified by diagnostic
        if (root.FindNode(diagnosticSpan) is not InvocationExpressionSyntax invocation)
            return;

        // Register code fix for converting to foreach loop
        context.RegisterCodeFix(
            CodeAction.Create(
                "Convert to foreach loop",
                cancellationToken => ConvertToForeachAsync(context.Document, invocation, cancellationToken),
                nameof(LinqInHotPathsCodeFixProvider)),
            diagnostic);
    }

    /// <summary>
    ///     Converts a LINQ invocation to a foreach loop.
    /// </summary>
    private static async Task<Document> ConvertToForeachAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Get the method name to determine the conversion strategy
        var methodName = GetMethodName(invocation);

        // Get the containing statement to replace
        var containingStatement = invocation.FirstAncestorOrSelf<StatementSyntax>();

        if (containingStatement == null)
            return document;

        // Generate a simple foreach loop as a placeholder
        // In a real implementation, you'd parse the LINQ chain and convert it properly
        var foreachCode = GenerateForeachCode(methodName);
        var replacementStatement = SyntaxFactory.ParseStatement(foreachCode);

        var newRoot = root.ReplaceNode(containingStatement, replacementStatement);
        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Gets method name from an invocation expression.
    /// </summary>
    private static string GetMethodName(InvocationExpressionSyntax invocation)
    {
        return invocation.Expression switch
        {
            MemberAccessExpressionSyntax memberAccess => memberAccess.Name.Identifier.Text,
            _ => "Unknown",
        };
    }

    /// <summary>
    ///     Generates foreach code based on the LINQ method.
    /// </summary>
    private static string GenerateForeachCode(string methodName)
    {
        return methodName switch
        {
            "Where" or "Select" or "SelectMany" => @"
var result = new List<T>();
foreach (var item in source)
{
    // TODO: Add LINQ lambda logic here
    result.Add(item);
}",
            "ToList" or "ToArray" => @"
var result = new List<T>();
foreach (var item in source)
{
    result.Add(item);
}",
            "First" or "FirstOrDefault" => @"
var result = default(T);
foreach (var item in source)
{
    result = item;
    break;
}",
            "Single" or "SingleOrDefault" => @"
var result = default(T);
var count = 0;
foreach (var item in source)
{
    if (++count > 1) throw new InvalidOperationException();
    result = item;
}
if (count != 1 && methodName == ""Single"") throw new InvalidOperationException();",
            "Count" or "LongCount" => @"
var count = 0;
foreach (var item in source)
{
    count++;
}",
            "Any" => @"
var result = false;
foreach (var item in source)
{
    result = true;
    break;
}",
            _ => @"// TODO: Convert this LINQ operation to imperative code",
        };
    }
}
