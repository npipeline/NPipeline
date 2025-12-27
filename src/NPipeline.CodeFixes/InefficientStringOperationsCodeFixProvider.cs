using System.Collections.Immutable;
using System.Composition;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using NPipeline.Analyzers;

namespace NPipeline.CodeFixes;

/// <summary>
///     Code fix provider that converts inefficient string operations to StringBuilder alternatives.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(InefficientStringOperationsCodeFixProvider))]
[Shared]
public sealed class InefficientStringOperationsCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [InefficientStringOperationsAnalyzer.InefficientStringOperationsId];

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

        // Check if we're in an expression context (like assignment or return)
        if (binaryExpr.Parent is ExpressionSyntax parentExpression &&
            (parentExpression.Parent is AssignmentExpressionSyntax ||
             parentExpression.Parent is ReturnStatementSyntax))
        {
            // We're in an expression context, need to handle differently
            var replacementStatement = GenerateStringBuilderInExpressionContext(binaryExpr);
            var newRoot = root.ReplaceNode(containingStatement, replacementStatement);
            return document.WithSyntaxRoot(newRoot);
        }
        else
        {
            // Generate StringBuilder code
            var replacementStatement = GenerateStringBuilderCode(binaryExpr);
            var newRoot = root.ReplaceNode(containingStatement, replacementStatement);
            return document.WithSyntaxRoot(newRoot);
        }
    }

    private static StatementSyntax GenerateStringBuilderInExpressionContext(BinaryExpressionSyntax binaryExpr)
    {
        try
        {
            // Extract all parts of the concatenation
            var stringParts = ExtractStringParts(binaryExpr);

            // Generate unique variable names based on context
            var stringBuilderName = GenerateUniqueVariableName("sb");

            // Create local declaration statements
            var statements = new List<StatementSyntax>();

            // StringBuilder declaration with capacity estimation if possible
            var stringBuilderDeclaration = CreateStringBuilderDeclaration(stringBuilderName, stringParts);
            statements.Add(stringBuilderDeclaration);

            // Append statements for each part
            var appendStatements = GenerateAppendStatements(stringBuilderName, stringParts);
            statements.AddRange(appendStatements);

            // Create the final expression
            var toStringExpression = SyntaxFactory.InvocationExpression(
                    SyntaxFactory.MemberAccessExpression(
                        SyntaxKind.SimpleMemberAccessExpression,
                        SyntaxFactory.IdentifierName(stringBuilderName),
                        SyntaxFactory.IdentifierName("ToString")))
                .WithArgumentList(SyntaxFactory.ArgumentList());

            // If we're in an assignment, we need to replace the entire assignment
            if (binaryExpr.Parent is AssignmentExpressionSyntax assignment)
            {
                var newAssignment = assignment.WithRight(toStringExpression);
                return SyntaxFactory.ExpressionStatement(newAssignment);
            }

            // If we're in a return statement
            if (binaryExpr.Parent is ReturnStatementSyntax returnStmt)
                return returnStmt.WithExpression(toStringExpression);

            // Default case - create a block with the StringBuilder operations and return the result
            statements.Add(SyntaxFactory.ReturnStatement(toStringExpression));
            return SyntaxFactory.Block(statements);
        }
        catch (Exception)
        {
            // If anything goes wrong, fall back to a simple expression statement
            return SyntaxFactory.ExpressionStatement(binaryExpr);
        }
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
        var replacementStatement = GenerateConcatStringBuilderCode(invocation);

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
        var replacementStatement = GenerateInterpolatedStringBuilderCode(interpolatedString);

        var newRoot = root.ReplaceNode(containingStatement, replacementStatement);
        return document.WithSyntaxRoot(newRoot);
    }

    private static StatementSyntax GenerateStringBuilderCode(BinaryExpressionSyntax binaryExpr)
    {
        try
        {
            // Extract all parts of the concatenation
            var stringParts = ExtractStringParts(binaryExpr);

            // Generate unique variable names based on context
            var stringBuilderName = GenerateUniqueVariableName("sb");
            var resultName = GenerateUniqueVariableName("result");

            // Create local declaration statements
            var statements = new List<StatementSyntax>();

            // StringBuilder declaration with capacity estimation if possible
            var stringBuilderDeclaration = CreateStringBuilderDeclaration(stringBuilderName, stringParts);
            statements.Add(stringBuilderDeclaration);

            // Append statements for each part
            var appendStatements = GenerateAppendStatements(stringBuilderName, stringParts);
            statements.AddRange(appendStatements);

            // Result declaration
            var resultDeclaration = SyntaxFactory.LocalDeclarationStatement(
                SyntaxFactory.VariableDeclaration(
                        SyntaxFactory.IdentifierName("var"))
                    .WithVariables(
                        SyntaxFactory.SingletonSeparatedList(
                            SyntaxFactory.VariableDeclarator(
                                    SyntaxFactory.Identifier(resultName))
                                .WithInitializer(
                                    SyntaxFactory.EqualsValueClause(
                                        SyntaxFactory.InvocationExpression(
                                                SyntaxFactory.MemberAccessExpression(
                                                    SyntaxKind.SimpleMemberAccessExpression,
                                                    SyntaxFactory.IdentifierName(stringBuilderName),
                                                    SyntaxFactory.IdentifierName("ToString")))
                                            .WithArgumentList(SyntaxFactory.ArgumentList()))))));

            statements.Add(resultDeclaration);

            // If this is a single statement context, return the last statement
            // Otherwise, return a block statement
            return statements.Count == 1
                ? statements[0]
                : SyntaxFactory.Block(statements);
        }
        catch (Exception)
        {
            // If anything goes wrong, fall back to a simple expression statement
            return SyntaxFactory.ExpressionStatement(binaryExpr);
        }
    }

    private static List<ExpressionSyntax> ExtractStringParts(BinaryExpressionSyntax binaryExpr)
    {
        var parts = new List<ExpressionSyntax>();
        ExtractStringPartsRecursive(binaryExpr, parts);
        return parts;
    }

    private static void ExtractStringPartsRecursive(ExpressionSyntax expression, List<ExpressionSyntax> parts)
    {
        if (expression is BinaryExpressionSyntax binaryExpr &&
            binaryExpr.IsKind(SyntaxKind.AddExpression))
        {
            ExtractStringPartsRecursive(binaryExpr.Left, parts);
            ExtractStringPartsRecursive(binaryExpr.Right, parts);
        }
        else
            parts.Add(expression);
    }

    private static string GenerateUniqueVariableName(string baseName)
    {
        // Simple implementation - in a real scenario, you'd want to check for conflicts
        // with existing variables in the scope
        return baseName;
    }

    private static LocalDeclarationStatementSyntax CreateStringBuilderDeclaration(string variableName, List<ExpressionSyntax> stringParts)
    {
        // Try to estimate capacity if we have string literals
        var estimatedCapacity = EstimateCapacity(stringParts);

        var stringBuilderType = SyntaxFactory.QualifiedName(
            SyntaxFactory.IdentifierName("System.Text"),
            SyntaxFactory.IdentifierName("StringBuilder"));

        var objectCreation = estimatedCapacity.HasValue
            ? SyntaxFactory.ObjectCreationExpression(
                stringBuilderType,
                SyntaxFactory.ArgumentList(
                    SyntaxFactory.SingletonSeparatedList(
                        SyntaxFactory.Argument(
                            SyntaxFactory.LiteralExpression(
                                SyntaxKind.NumericLiteralExpression,
                                SyntaxFactory.Literal(estimatedCapacity.Value))))),
                null)
            : SyntaxFactory.ObjectCreationExpression(
                stringBuilderType,
                SyntaxFactory.ArgumentList(),
                null);

        return SyntaxFactory.LocalDeclarationStatement(
            SyntaxFactory.VariableDeclaration(
                    SyntaxFactory.IdentifierName("var"))
                .WithVariables(
                    SyntaxFactory.SingletonSeparatedList(
                        SyntaxFactory.VariableDeclarator(
                                SyntaxFactory.Identifier(variableName))
                            .WithInitializer(
                                SyntaxFactory.EqualsValueClause(objectCreation)))));
    }

    private static int? EstimateCapacity(List<ExpressionSyntax> stringParts)
    {
        try
        {
            var totalLength = 0;
            var hasUnknownParts = false;

            foreach (var part in stringParts)
            {
                if (part is LiteralExpressionSyntax literal &&
                    literal.IsKind(SyntaxKind.StringLiteralExpression))
                    totalLength += literal.Token.ValueText?.Length ?? 0;
                else
                    hasUnknownParts = true;
            }

            // Only return capacity if we have some string literals
            return totalLength > 0
                ? totalLength + (hasUnknownParts
                    ? 16
                    : 0)
                : null;
        }
        catch (Exception ex)
        {
            // Log the error but return null to indicate capacity estimation failed
            Debug.WriteLine($"Error estimating StringBuilder capacity: {ex.Message}");
            return null;
        }
    }

    private static List<StatementSyntax> GenerateAppendStatements(string stringBuilderName, List<ExpressionSyntax> stringParts)
    {
        var statements = new List<StatementSyntax>();

        foreach (var part in stringParts)
        {
            var appendInvocation = SyntaxFactory.InvocationExpression(
                    SyntaxFactory.MemberAccessExpression(
                        SyntaxKind.SimpleMemberAccessExpression,
                        SyntaxFactory.IdentifierName(stringBuilderName),
                        SyntaxFactory.IdentifierName("Append")))
                .WithArgumentList(
                    SyntaxFactory.ArgumentList(
                        SyntaxFactory.SingletonSeparatedList(
                            SyntaxFactory.Argument(part))));

            statements.Add(SyntaxFactory.ExpressionStatement(appendInvocation));
        }

        return statements;
    }

    private static InterpolatedStringExpressionSyntax ConvertBinaryToInterpolatedString(BinaryExpressionSyntax binaryExpr)
    {
        try
        {
            // Extract all parts of the concatenation recursively
            var stringParts = new List<ExpressionSyntax>();
            ExtractStringPartsRecursive(binaryExpr, stringParts);

            // Convert each part to appropriate interpolated string content
            var contents = new List<InterpolatedStringContentSyntax>();

            foreach (var part in stringParts)
            {
                if (part is LiteralExpressionSyntax literal && literal.IsKind(SyntaxKind.StringLiteralExpression))
                {
                    // For string literals, add as text content
                    var text = literal.Token.ValueText ?? "";

                    if (!string.IsNullOrEmpty(text))
                    {
                        contents.Add(SyntaxFactory.InterpolatedStringText(
                            SyntaxFactory.Literal(text, text)));
                    }
                }
                else
                {
                    // For expressions, add as interpolation
                    // Check if we need to wrap in parentheses for complex expressions
                    var expressionToUse = NeedsParentheses(part)
                        ? SyntaxFactory.ParenthesizedExpression(part)
                        : part;

                    contents.Add(SyntaxFactory.Interpolation(expressionToUse));
                }
            }

            // Create the interpolated string expression
            return SyntaxFactory.InterpolatedStringExpression(
                SyntaxFactory.Token(SyntaxKind.InterpolatedStringStartToken),
                SyntaxFactory.List(contents),
                SyntaxFactory.Token(SyntaxKind.InterpolatedStringEndToken));
        }
        catch (Exception)
        {
            // If conversion fails, fall back to the original binary expression
            // This will be wrapped in a way that maintains the original behavior
            return SyntaxFactory.InterpolatedStringExpression(
                SyntaxFactory.Token(SyntaxKind.InterpolatedStringStartToken),
                SyntaxFactory.List(new InterpolatedStringContentSyntax[]
                {
                    SyntaxFactory.Interpolation(binaryExpr),
                }),
                SyntaxFactory.Token(SyntaxKind.InterpolatedStringEndToken));
        }
    }

    /// <summary>
    ///     Determines if an expression needs to be wrapped in parentheses when used in an interpolation.
    /// </summary>
    private static bool NeedsParentheses(ExpressionSyntax expression)
    {
        // Already parenthesized expressions don't need additional parentheses
        if (expression is ParenthesizedExpressionSyntax)
            return false;

        // Simple expressions that don't need parentheses
        if (expression is IdentifierNameSyntax or
            LiteralExpressionSyntax or
            MemberAccessExpressionSyntax or
            InvocationExpressionSyntax)
            return false;

        // Binary expressions, conditional expressions, and complex expressions need parentheses
        if (expression is BinaryExpressionSyntax or
            ConditionalExpressionSyntax or
            AssignmentExpressionSyntax)
            return true;

        // Cast expressions might need parentheses depending on context
        if (expression is CastExpressionSyntax)
            return true;

        // Default to wrapping in parentheses for safety
        return true;
    }

    private static StatementSyntax GenerateConcatStringBuilderCode(InvocationExpressionSyntax invocation)
    {
        try
        {
            var arguments = invocation.ArgumentList?.Arguments ?? [];
            var stringParts = arguments.Select(a => a.Expression).ToList();

            // Generate unique variable names based on context
            var stringBuilderName = GenerateUniqueVariableName("sb");
            var resultName = GenerateUniqueVariableName("result");

            // Create local declaration statements
            var statements = new List<StatementSyntax>();

            // StringBuilder declaration with capacity estimation if possible
            var stringBuilderDeclaration = CreateStringBuilderDeclaration(stringBuilderName, stringParts);
            statements.Add(stringBuilderDeclaration);

            // Append statements for each part
            var appendStatements = GenerateAppendStatements(stringBuilderName, stringParts);
            statements.AddRange(appendStatements);

            // Result declaration
            var resultDeclaration = SyntaxFactory.LocalDeclarationStatement(
                SyntaxFactory.VariableDeclaration(
                        SyntaxFactory.IdentifierName("var"))
                    .WithVariables(
                        SyntaxFactory.SingletonSeparatedList(
                            SyntaxFactory.VariableDeclarator(
                                    SyntaxFactory.Identifier(resultName))
                                .WithInitializer(
                                    SyntaxFactory.EqualsValueClause(
                                        SyntaxFactory.InvocationExpression(
                                                SyntaxFactory.MemberAccessExpression(
                                                    SyntaxKind.SimpleMemberAccessExpression,
                                                    SyntaxFactory.IdentifierName(stringBuilderName),
                                                    SyntaxFactory.IdentifierName("ToString")))
                                            .WithArgumentList(SyntaxFactory.ArgumentList()))))));

            statements.Add(resultDeclaration);

            // Return a block statement
            return SyntaxFactory.Block(statements);
        }
        catch (Exception)
        {
            // If anything goes wrong, fall back to a simple expression statement
            return SyntaxFactory.ExpressionStatement(invocation);
        }
    }

    private static ExpressionSyntax ConvertFormatToInterpolatedString(InvocationExpressionSyntax invocation)
    {
        var arguments = invocation.ArgumentList?.Arguments ?? [];

        if (arguments.Count == 0)
            return SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal(""));

        // Extract the format string (first argument)
        var formatStringExpr = arguments[0].Expression;
        var formatString = ExtractStringValue(formatStringExpr);

        if (formatString == null)
        {
            // If we can't extract the format string, return the original invocation
            return invocation;
        }

        // Extract the arguments to be interpolated (remaining arguments)
        var formatArguments = arguments.Skip(1).Select(a => a.Expression).ToArray();

        try
        {
            // Parse the format string and convert to interpolated string
            return ParseFormatStringToInterpolatedString(formatString, formatArguments);
        }
        catch (Exception ex)
        {
            // If parsing fails, log the error and return the original invocation
            Debug.WriteLine($"Error parsing format string for interpolation: {ex.Message}");
            return invocation;
        }
    }

    private static string? ExtractStringValue(ExpressionSyntax expression)
    {
        // Handle literal string expressions
        if (expression is LiteralExpressionSyntax literal && literal.IsKind(SyntaxKind.StringLiteralExpression))
            return literal.Token.ValueText;

        // If it's not a literal string, we can't extract the value at compile time
        return null;
    }

    private static InterpolatedStringExpressionSyntax ParseFormatStringToInterpolatedString(string formatString, ExpressionSyntax[] arguments)
    {
        var contents = new List<InterpolatedStringContentSyntax>();
        var regex = new Regex(@"\{(\d+)(,(-?\d+))?(:(.*?))?\}");
        var lastIndex = 0;

        foreach (Match match in regex.Matches(formatString))
        {
            // Add text before the placeholder
            if (match.Index > lastIndex)
            {
                var text = formatString.Substring(lastIndex, match.Index - lastIndex);

                contents.Add(SyntaxFactory.InterpolatedStringText(
                    SyntaxFactory.Literal(text, text)));
            }

            // Parse the placeholder
            var indexStr = match.Groups[1].Value;

            var alignmentStr = match.Groups[3].Success
                ? match.Groups[3].Value
                : null;

            var formatStr = match.Groups[5].Success
                ? match.Groups[5].Value
                : null;

            if (int.TryParse(indexStr, out var index) && index >= 0 && index < arguments.Length)
            {
                var argument = arguments[index];

                // Apply format and alignment if present
                if (alignmentStr != null || formatStr != null)
                {
                    // Create an interpolated string with format specifier
                    var innerContents = new List<InterpolatedStringContentSyntax>
                    {
                        SyntaxFactory.Interpolation(argument),
                    };

                    if (formatStr != null)
                    {
                        // Apply format string
                        var formatInterpolation = SyntaxFactory.Interpolation(argument)
                            .WithFormatClause(SyntaxFactory.InterpolationFormatClause(
                                SyntaxFactory.Token(SyntaxKind.ColonToken),
                                SyntaxFactory.Literal(formatStr, formatStr)));

                        innerContents[0] = formatInterpolation;
                    }

                    // For alignment, we need to handle it differently since C# interpolated strings
                    // don't directly support alignment like string.Format
                    // We'll create a separate expression with alignment
                    if (alignmentStr != null && int.TryParse(alignmentStr, out var alignment))
                    {
                        // Create a string with alignment using PadLeft/PadRight
                        var alignmentExpression = CreateAlignmentExpression(argument, alignment, formatStr);
                        contents.Add(SyntaxFactory.Interpolation(alignmentExpression));
                    }
                    else
                        contents.Add(SyntaxFactory.Interpolation(argument));
                }
                else
                {
                    // Simple placeholder without format or alignment
                    contents.Add(SyntaxFactory.Interpolation(argument));
                }
            }
            else
            {
                // Invalid index, add the original placeholder as text
                contents.Add(SyntaxFactory.InterpolatedStringText(
                    SyntaxFactory.Literal(match.Value, match.Value)));
            }

            lastIndex = match.Index + match.Length;
        }

        // Add remaining text after the last placeholder
        if (lastIndex < formatString.Length)
        {
            var text = formatString.Substring(lastIndex);

            contents.Add(SyntaxFactory.InterpolatedStringText(
                SyntaxFactory.Literal(text, text)));
        }

        return SyntaxFactory.InterpolatedStringExpression(
            SyntaxFactory.Token(SyntaxKind.InterpolatedStringStartToken),
            SyntaxFactory.List(contents),
            SyntaxFactory.Token(SyntaxKind.InterpolatedStringEndToken));
    }

    private static ExpressionSyntax CreateAlignmentExpression(ExpressionSyntax expression, int alignment, string? formatString)
    {
        // Create an expression for alignment using PadLeft or PadRight
        var stringExpression = formatString != null
            ? SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    expression,
                    SyntaxFactory.IdentifierName("ToString")),
                SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(
                    SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(
                        SyntaxKind.StringLiteralExpression,
                        SyntaxFactory.Literal(formatString))))))
            : (ExpressionSyntax)SyntaxFactory.BinaryExpression(
                SyntaxKind.AddExpression,
                SyntaxFactory.LiteralExpression(SyntaxKind.StringLiteralExpression, SyntaxFactory.Literal("")),
                expression);

        var methodName = alignment >= 0
            ? "PadLeft"
            : "PadRight";

        var paddingValue = Math.Abs(alignment);

        return SyntaxFactory.InvocationExpression(
            SyntaxFactory.MemberAccessExpression(
                SyntaxKind.SimpleMemberAccessExpression,
                stringExpression,
                SyntaxFactory.IdentifierName(methodName)),
            SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(
                SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(
                    SyntaxKind.NumericLiteralExpression,
                    SyntaxFactory.Literal(paddingValue))))));
    }

    private static ExpressionSyntax ConvertSubstringToSpan(InvocationExpressionSyntax invocation)
    {
        var arguments = invocation.ArgumentList?.Arguments ?? [];

        if (arguments.Count is not (1 or 2))
            return invocation; // Only support Substring(startIndex) or Substring(startIndex, length)

        var target = (invocation.Expression as MemberAccessExpressionSyntax)?.Expression ?? invocation.Expression;

        if (target == null)
            return invocation;

        try
        {
            // Create the AsSpan() invocation
            var asSpanInvocation = SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    target,
                    SyntaxFactory.IdentifierName("AsSpan")),
                SyntaxFactory.ArgumentList());

            // Create the Slice() arguments based on the Substring parameters
            var sliceArguments = new List<ArgumentSyntax>
            {
                SyntaxFactory.Argument(arguments[0].Expression),
            };

            // Add length parameter if present (Substring with two parameters)
            if (arguments.Count == 2)
                sliceArguments.Add(SyntaxFactory.Argument(arguments[1].Expression));

            // Create the Slice() invocation chained to AsSpan()
            var sliceInvocation = SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    asSpanInvocation,
                    SyntaxFactory.IdentifierName("Slice")),
                SyntaxFactory.ArgumentList(SyntaxFactory.SeparatedList(sliceArguments)));

            return sliceInvocation;
        }
        catch
        {
            // If anything goes wrong during the conversion, return the original invocation
            return invocation;
        }
    }

    private static StatementSyntax GenerateInterpolatedStringBuilderCode(InterpolatedStringExpressionSyntax interpolatedString)
    {
        try
        {
            var stringParts = ExtractStringPartsFromInterpolatedString(interpolatedString);

            // Generate unique variable names based on context
            var stringBuilderName = GenerateUniqueVariableName("sb");
            var resultName = GenerateUniqueVariableName("result");

            // Create local declaration statements
            var statements = new List<StatementSyntax>();

            // StringBuilder declaration with capacity estimation if possible
            var stringBuilderDeclaration = CreateStringBuilderDeclaration(stringBuilderName, stringParts);
            statements.Add(stringBuilderDeclaration);

            // Append statements for each part
            var appendStatements = GenerateAppendStatements(stringBuilderName, stringParts);
            statements.AddRange(appendStatements);

            // Result declaration
            var resultDeclaration = SyntaxFactory.LocalDeclarationStatement(
                SyntaxFactory.VariableDeclaration(
                        SyntaxFactory.IdentifierName("var"))
                    .WithVariables(
                        SyntaxFactory.SingletonSeparatedList(
                            SyntaxFactory.VariableDeclarator(
                                    SyntaxFactory.Identifier(resultName))
                                .WithInitializer(
                                    SyntaxFactory.EqualsValueClause(
                                        SyntaxFactory.InvocationExpression(
                                                SyntaxFactory.MemberAccessExpression(
                                                    SyntaxKind.SimpleMemberAccessExpression,
                                                    SyntaxFactory.IdentifierName(stringBuilderName),
                                                    SyntaxFactory.IdentifierName("ToString")))
                                            .WithArgumentList(SyntaxFactory.ArgumentList()))))));

            statements.Add(resultDeclaration);

            // Return a block statement
            return SyntaxFactory.Block(statements);
        }
        catch (Exception)
        {
            // If anything goes wrong, fall back to a simple expression statement
            return SyntaxFactory.ExpressionStatement(interpolatedString);
        }
    }

    private static List<ExpressionSyntax> ExtractStringPartsFromInterpolatedString(InterpolatedStringExpressionSyntax interpolatedString)
    {
        var parts = new List<ExpressionSyntax>();

        foreach (var content in interpolatedString.Contents)
        {
            if (content is InterpolatedStringTextSyntax text)
            {
                // Convert text to a string literal
                var stringLiteral = SyntaxFactory.LiteralExpression(
                    SyntaxKind.StringLiteralExpression,
                    SyntaxFactory.Literal(text.TextToken.Text ?? ""));

                parts.Add(stringLiteral);
            }
            else if (content is InterpolationSyntax interpolation)
            {
                // Add the interpolated expression
                parts.Add(interpolation.Expression);
            }
        }

        return parts;
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
