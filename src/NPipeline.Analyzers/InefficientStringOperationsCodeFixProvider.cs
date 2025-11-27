using System.Collections.Immutable;
using System.Composition;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace NPipeline.Analyzers;

/// <summary>
///     Code fix provider that converts inefficient string operations to StringBuilder alternatives.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(InefficientStringOperationsCodeFixProvider))]
[Shared]
public sealed class InefficientStringOperationsCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [InefficientStringOperationsAnalyzer.DiagnosticId];

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

        // Register different code fixes based on node type
        switch (node.Kind())
        {
            case SyntaxKind.AddExpression:
                await RegisterBinaryExpressionFixes(context, (BinaryExpressionSyntax)node, diagnostic);
                break;
            case SyntaxKind.InvocationExpression:
                await RegisterInvocationFixes(context, (InvocationExpressionSyntax)node, diagnostic);
                break;
            case SyntaxKind.InterpolatedStringExpression:
                await RegisterInterpolatedStringFixes(context, (InterpolatedStringExpressionSyntax)node, diagnostic);
                break;
        }
    }

    private static Task RegisterBinaryExpressionFixes(
        CodeFixContext context,
        BinaryExpressionSyntax binaryExpr,
        Diagnostic diagnostic)
    {
        // Check if in loop
        var isInLoop = IsInLoop(binaryExpr);

        if (isInLoop)
        {
            // Fix for string concatenation in loop
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Convert to StringBuilder",
                    cancellationToken => ConvertToStringBuilderAsync(context.Document, binaryExpr, cancellationToken),
                    nameof(InefficientStringOperationsCodeFixProvider)),
                diagnostic);
        }
        else
        {
            // Fix for simple string concatenation
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Convert to string interpolation",
                    cancellationToken => ConvertToInterpolationAsync(context.Document, binaryExpr, cancellationToken),
                    nameof(InefficientStringOperationsCodeFixProvider)),
                diagnostic);
        }

        return Task.CompletedTask;
    }

    private static async Task RegisterInvocationFixes(
        CodeFixContext context,
        InvocationExpressionSyntax invocation,
        Diagnostic diagnostic)
    {
        var semanticModel = await context.Document.GetSemanticModelAsync(context.CancellationToken);

        if (semanticModel == null)
            return;

        if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
        {
            var methodName = memberAccess.Name.Identifier.Text;

            switch (methodName)
            {
                case "Concat":
                    context.RegisterCodeFix(
                        CodeAction.Create(
                            "Convert to StringBuilder",
                            cancellationToken => ConvertConcatToStringBuilderAsync(context.Document, invocation, cancellationToken),
                            nameof(InefficientStringOperationsCodeFixProvider)),
                        diagnostic);

                    break;

                case "Format":
                    context.RegisterCodeFix(
                        CodeAction.Create(
                            "Convert to interpolated string",
                            cancellationToken => ConvertFormatToInterpolationAsync(context.Document, invocation, cancellationToken),
                            nameof(InefficientStringOperationsCodeFixProvider)),
                        diagnostic);

                    break;

                case "Substring":
                    context.RegisterCodeFix(
                        CodeAction.Create(
                            "Convert to AsSpan().Slice()",
                            cancellationToken => ConvertSubstringToSpanAsync(context.Document, invocation, cancellationToken),
                            nameof(InefficientStringOperationsCodeFixProvider)),
                        diagnostic);

                    break;
            }
        }
    }

    private static Task RegisterInterpolatedStringFixes(
        CodeFixContext context,
        InterpolatedStringExpressionSyntax interpolatedString,
        Diagnostic diagnostic)
    {
        context.RegisterCodeFix(
            CodeAction.Create(
                "Convert to StringBuilder",
                cancellationToken => ConvertInterpolatedStringToStringBuilderAsync(context.Document, interpolatedString, cancellationToken),
                nameof(InefficientStringOperationsCodeFixProvider)),
            diagnostic);

        return Task.CompletedTask;
    }

    private static async Task<Document> ConvertToStringBuilderAsync(
        Document document,
        BinaryExpressionSyntax binaryExpr,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Find containing statement to replace
        var containingStatement = binaryExpr.FirstAncestorOrSelf<StatementSyntax>();

        if (containingStatement == null)
            return document;

        // Generate StringBuilder code
        var stringBuilderCode = GenerateStringBuilderCode(binaryExpr);
        var replacementStatement = SyntaxFactory.ParseStatement(stringBuilderCode);

        var newRoot = root.ReplaceNode(containingStatement, replacementStatement);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> ConvertToInterpolationAsync(
        Document document,
        BinaryExpressionSyntax binaryExpr,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Convert binary expression to interpolated string
        var interpolatedString = ConvertBinaryToInterpolatedString(binaryExpr);
        var newRoot = root.ReplaceNode(binaryExpr, interpolatedString);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> ConvertConcatToStringBuilderAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Find containing statement
        var containingStatement = invocation.FirstAncestorOrSelf<StatementSyntax>();

        if (containingStatement == null)
            return document;

        // Generate StringBuilder code for string.Concat
        var stringBuilderCode = GenerateConcatStringBuilderCode(invocation);
        var replacementStatement = SyntaxFactory.ParseStatement(stringBuilderCode);

        var newRoot = root.ReplaceNode(containingStatement, replacementStatement);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> ConvertFormatToInterpolationAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Convert string.Format to interpolated string
        var interpolatedString = ConvertFormatToInterpolatedString(invocation);
        var newRoot = root.ReplaceNode(invocation, interpolatedString);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> ConvertSubstringToSpanAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Convert string.Substring to AsSpan().Slice()
        var spanExpression = ConvertSubstringToSpan(invocation);
        var newRoot = root.ReplaceNode(invocation, spanExpression);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> ConvertInterpolatedStringToStringBuilderAsync(
        Document document,
        InterpolatedStringExpressionSyntax interpolatedString,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Find containing statement
        var containingStatement = interpolatedString.FirstAncestorOrSelf<StatementSyntax>();

        if (containingStatement == null)
            return document;

        // Generate StringBuilder code for complex interpolated string
        var stringBuilderCode = GenerateInterpolatedStringBuilderCode(interpolatedString);
        var replacementStatement = SyntaxFactory.ParseStatement(stringBuilderCode);

        var newRoot = root.ReplaceNode(containingStatement, replacementStatement);
        return document.WithSyntaxRoot(newRoot);
    }

    private static string GenerateStringBuilderCode(BinaryExpressionSyntax binaryExpr)
    {
        return $@"
var sb = new StringBuilder();
{GenerateStringBuilderAppends(binaryExpr)}
var result = sb.ToString();";
    }

    private static string GenerateStringBuilderAppends(BinaryExpressionSyntax binaryExpr)
    {
        var left = binaryExpr.Left.ToString();
        var right = binaryExpr.Right.ToString();

        // Handle nested concatenations
        if (binaryExpr.Left is BinaryExpressionSyntax leftBinary)
            left = GenerateStringBuilderAppends(leftBinary);

        var result = left;

        if (!result.Contains("sb.Append"))
            result = $"sb.Append({left});";

        result += $"\nsb.Append({right});";

        return result;
    }

    private static InterpolatedStringExpressionSyntax ConvertBinaryToInterpolatedString(BinaryExpressionSyntax binaryExpr)
    {
        var leftExpr = binaryExpr.Left;
        var rightExpr = binaryExpr.Right;

        // Create interpolated string with two expressions
        var interpolation1 = SyntaxFactory.Interpolation(leftExpr);
        var interpolation2 = SyntaxFactory.Interpolation(rightExpr);

        var interpolatedString = SyntaxFactory.InterpolatedStringExpression(
            SyntaxFactory.Token(SyntaxKind.InterpolatedStringStartToken),
            SyntaxFactory.List(new InterpolatedStringContentSyntax[]
            {
                interpolation1,
                SyntaxFactory.InterpolatedStringText(SyntaxFactory.Token(SyntaxKind.InterpolatedStringTextToken)),
            }),
            SyntaxFactory.Token(SyntaxKind.InterpolatedStringEndToken));

        // Add the second interpolation
        interpolatedString = interpolatedString.WithContents(
            SyntaxFactory.List(new InterpolatedStringContentSyntax[]
            {
                interpolation1,
                SyntaxFactory.InterpolatedStringText(SyntaxFactory.Token(SyntaxKind.InterpolatedStringTextToken)),
                interpolation2,
            }));

        return interpolatedString;
    }

    private static string GenerateConcatStringBuilderCode(InvocationExpressionSyntax invocation)
    {
        var arguments = invocation.ArgumentList?.Arguments ?? [];
        var argumentStrings = arguments.Select(a => a.Expression.ToString());

        var appendCode = string.Join("\n", argumentStrings.Select(arg => $"sb.Append({arg});"));

        return $@"
var sb = new StringBuilder();
{appendCode}
var result = sb.ToString();";
    }

    private static ExpressionSyntax ConvertFormatToInterpolatedString(InvocationExpressionSyntax invocation)
    {
        var arguments = invocation.ArgumentList?.Arguments ?? [];

        if (arguments.Count == 0)
            return SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(""));

        // For simplicity, just convert first argument (format string) and ignore placeholders
        // In a real implementation, you'd parse the format string and replace placeholders
        var formatString = arguments[0].Expression.ToString();

        // Remove quotes from format string
        if (formatString.StartsWith("\"", StringComparison.Ordinal) && formatString.EndsWith("\"", StringComparison.Ordinal))
            formatString = formatString[1..^1];

        return SyntaxFactory.LiteralExpression(
            SyntaxKind.StringLiteralExpression,
            SyntaxFactory.Literal(formatString));
    }

    private static ExpressionSyntax ConvertSubstringToSpan(InvocationExpressionSyntax invocation)
    {
        var arguments = invocation.ArgumentList?.Arguments ?? [];

        if (arguments.Count < 1)
            return invocation;

        var target = (invocation.Expression as MemberAccessExpressionSyntax)?.Expression ?? invocation.Expression;

        // Create AsSpan().Slice() expression - simplified for now
        // This is a placeholder - in a real implementation you'd handle the full conversion
        return SyntaxFactory.InvocationExpression(
            SyntaxFactory.MemberAccessExpression(
                SyntaxKind.SimpleMemberAccessExpression,
                target,
                SyntaxFactory.IdentifierName("AsSpan")),
            SyntaxFactory.ArgumentList());
    }

    private static string GenerateInterpolatedStringBuilderCode(InterpolatedStringExpressionSyntax interpolatedString)
    {
        var appendCode = new StringBuilder();

        foreach (var content in interpolatedString.Contents)
        {
            if (content is InterpolatedStringTextSyntax text)
                _ = appendCode.AppendLine($"sb.Append(\"{text.TextToken.Text}\");");
            else if (content is InterpolationSyntax interpolation)
                _ = appendCode.AppendLine($"sb.Append({interpolation.Expression});");
        }

        return $@"
var sb = new StringBuilder();
{appendCode}
var result = sb.ToString();";
    }

    private static bool IsInLoop(SyntaxNode node)
    {
        var parent = node.Parent;

        while (parent != null)
        {
            if (parent is ForStatementSyntax or
                ForEachStatementSyntax or
                WhileStatementSyntax or
                DoStatementSyntax)
                return true;

            parent = parent.Parent;
        }

        return false;
    }
}
