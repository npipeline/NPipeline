using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace NPipeline.Analyzers;

/// <summary>
///     Code fix provider that suggests optimal batching configurations.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(BatchingConfigurationMismatchCodeFixProvider))]
[Shared]
public sealed class BatchingConfigurationMismatchCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [BatchingConfigurationMismatchAnalyzer.BatchingConfigurationMismatchId];

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
        if (node is ObjectCreationExpressionSyntax objectCreation)
            await RegisterObjectCreationFixes(context, objectCreation, diagnostic);
    }

    private static async Task RegisterObjectCreationFixes(
        CodeFixContext context,
        ObjectCreationExpressionSyntax objectCreation,
        Diagnostic diagnostic)
    {
        var semanticModel = await context.Document.GetSemanticModelAsync(context.CancellationToken);

        if (semanticModel == null)
            return;

        var typeSymbol = semanticModel.GetTypeInfo(objectCreation).Type;

        if (typeSymbol == null)
            return;

        if (typeSymbol.Name == "BatchingOptions")
            await RegisterBatchingOptionsFixes(context, objectCreation, semanticModel, diagnostic);
        else if (typeSymbol.Name == "BatchingStrategy")
            await RegisterBatchingStrategyFixes(context, objectCreation, semanticModel, diagnostic);
    }

    private static Task RegisterBatchingOptionsFixes(
        CodeFixContext context,
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        Diagnostic diagnostic)
    {
        // Try to extract current values
        if (!TryExtractCurrentBatchingOptions(objectCreation, semanticModel, out var currentBatchSize, out var currentTimeoutMs))
            return Task.CompletedTask;

        // Generate fixes based on mismatch type
        var fixes = GenerateBatchingOptionsFixes(currentBatchSize, currentTimeoutMs);

        foreach (var (title, newBatchSize, newTimeoutMs) in fixes)
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    title,
                    cancellationToken => UpdateBatchingOptionsAsync(context.Document, objectCreation, newBatchSize, newTimeoutMs, cancellationToken),
                    nameof(BatchingConfigurationMismatchCodeFixProvider)),
                diagnostic);
        }

        return Task.CompletedTask;
    }

    private static Task RegisterBatchingStrategyFixes(
        CodeFixContext context,
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        Diagnostic diagnostic)
    {
        // Try to extract current values
        if (!TryExtractCurrentBatchingStrategy(objectCreation, semanticModel, out var currentBatchSize, out var currentMaxWaitTimeMs))
            return Task.CompletedTask;

        // Generate fixes based on mismatch type
        var fixes = GenerateBatchingStrategyFixes(currentBatchSize, currentMaxWaitTimeMs);

        foreach (var (title, newBatchSize, newMaxWaitTimeMs) in fixes)
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    title,
                    cancellationToken => UpdateBatchingStrategyAsync(context.Document, objectCreation, newBatchSize, newMaxWaitTimeMs, cancellationToken),
                    nameof(BatchingConfigurationMismatchCodeFixProvider)),
                diagnostic);
        }

        return Task.CompletedTask;
    }

    private static async Task<Document> UpdateBatchingOptionsAsync(
        Document document,
        ObjectCreationExpressionSyntax objectCreation,
        int newBatchSize,
        double newTimeoutMs,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        var arguments = new List<ArgumentSyntax>();

        // Add or update BatchSize argument
        var batchSizeArgument = SyntaxFactory.Argument(
            SyntaxFactory.NameColon("BatchSize"),
            SyntaxFactory.Token(SyntaxKind.ColonToken),
            SyntaxFactory.LiteralExpression(SyntaxKind.NumericLiteralExpression, SyntaxFactory.Literal(newBatchSize)));

        arguments.Add(batchSizeArgument);

        // Add or update Timeout argument
        var timeoutArgument = SyntaxFactory.Argument(
            SyntaxFactory.NameColon("Timeout"),
            SyntaxFactory.Token(SyntaxKind.ColonToken),
            CreateTimeSpanExpression(newTimeoutMs));

        arguments.Add(timeoutArgument);

        var newObjectCreation = objectCreation.WithArgumentList(
            SyntaxFactory.ArgumentList(SyntaxFactory.SeparatedList(arguments)));

        var newRoot = root.ReplaceNode(objectCreation, newObjectCreation);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> UpdateBatchingStrategyAsync(
        Document document,
        ObjectCreationExpressionSyntax objectCreation,
        int newBatchSize,
        double newMaxWaitTimeMs,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        var arguments = new List<ArgumentSyntax>();

        // Add or update BatchSize argument
        var batchSizeArgument = SyntaxFactory.Argument(
            SyntaxFactory.NameColon("BatchSize"),
            SyntaxFactory.Token(SyntaxKind.ColonToken),
            SyntaxFactory.LiteralExpression(SyntaxKind.NumericLiteralExpression, SyntaxFactory.Literal(newBatchSize)));

        arguments.Add(batchSizeArgument);

        // Add or update MaxWaitTime argument
        var maxWaitTimeArgument = SyntaxFactory.Argument(
            SyntaxFactory.NameColon("MaxWaitTime"),
            SyntaxFactory.Token(SyntaxKind.ColonToken),
            CreateTimeSpanExpression(newMaxWaitTimeMs));

        arguments.Add(maxWaitTimeArgument);

        var newObjectCreation = objectCreation.WithArgumentList(
            SyntaxFactory.ArgumentList(SyntaxFactory.SeparatedList(arguments)));

        var newRoot = root.ReplaceNode(objectCreation, newObjectCreation);
        return document.WithSyntaxRoot(newRoot);
    }

    private static ExpressionSyntax CreateTimeSpanExpression(double milliseconds)
    {
        // Choose most appropriate TimeSpan method based on value
        if (milliseconds < 1000)
        {
            return SyntaxFactory.InvocationExpression(
                    SyntaxFactory.MemberAccessExpression(
                        SyntaxKind.SimpleMemberAccessExpression,
                        SyntaxFactory.IdentifierName("TimeSpan"),
                        SyntaxFactory.Token(SyntaxKind.DotToken),
                        SyntaxFactory.IdentifierName("FromMilliseconds")))
                .WithArgumentList(SyntaxFactory.ArgumentList(
                    SyntaxFactory.SingletonSeparatedList(
                        SyntaxFactory.Argument(
                            SyntaxFactory.LiteralExpression(
                                SyntaxKind.NumericLiteralExpression,
                                SyntaxFactory.Literal(milliseconds))))));
        }

        if (milliseconds < 60000)
        {
            return SyntaxFactory.InvocationExpression(
                    SyntaxFactory.MemberAccessExpression(
                        SyntaxKind.SimpleMemberAccessExpression,
                        SyntaxFactory.IdentifierName("TimeSpan"),
                        SyntaxFactory.Token(SyntaxKind.DotToken),
                        SyntaxFactory.IdentifierName("FromSeconds")))
                .WithArgumentList(SyntaxFactory.ArgumentList(
                    SyntaxFactory.SingletonSeparatedList(
                        SyntaxFactory.Argument(
                            SyntaxFactory.LiteralExpression(
                                SyntaxKind.NumericLiteralExpression,
                                SyntaxFactory.Literal(milliseconds / 1000))))));
        }

        if (milliseconds < 3600000)
        {
            return SyntaxFactory.InvocationExpression(
                    SyntaxFactory.MemberAccessExpression(
                        SyntaxKind.SimpleMemberAccessExpression,
                        SyntaxFactory.IdentifierName("TimeSpan"),
                        SyntaxFactory.Token(SyntaxKind.DotToken),
                        SyntaxFactory.IdentifierName("FromMinutes")))
                .WithArgumentList(SyntaxFactory.ArgumentList(
                    SyntaxFactory.SingletonSeparatedList(
                        SyntaxFactory.Argument(
                            SyntaxFactory.LiteralExpression(
                                SyntaxKind.NumericLiteralExpression,
                                SyntaxFactory.Literal(milliseconds / 60000))))));
        }

        return SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName("TimeSpan"),
                    SyntaxFactory.Token(SyntaxKind.DotToken),
                    SyntaxFactory.IdentifierName("FromHours")))
            .WithArgumentList(SyntaxFactory.ArgumentList(
                SyntaxFactory.SingletonSeparatedList(
                    SyntaxFactory.Argument(
                        SyntaxFactory.LiteralExpression(
                            SyntaxKind.NumericLiteralExpression,
                            SyntaxFactory.Literal(milliseconds / 3600000))))));
    }

    private static bool TryExtractCurrentBatchingOptions(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        out int batchSize,
        out double timeoutMs)
    {
        timeoutMs = 0;

        // Try to extract BatchSize
        if (!TryExtractBatchSize(objectCreation, out batchSize))
            return false;

        // Try to extract Timeout
        if (!TryExtractTimeout(objectCreation, semanticModel, out timeoutMs))
            return false;

        return true;
    }

    private static bool TryExtractCurrentBatchingStrategy(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        out int batchSize,
        out double maxWaitTimeMs)
    {
        maxWaitTimeMs = 0;

        // Try to extract BatchSize
        if (!TryExtractBatchingStrategyBatchSize(objectCreation, out batchSize))
            return false;

        // Try to extract MaxWaitTime
        if (!TryExtractMaxWaitTime(objectCreation, semanticModel, out maxWaitTimeMs))
            return false;

        return true;
    }

    private static bool TryExtractBatchSize(
        ObjectCreationExpressionSyntax objectCreation,
        out int batchSize)
    {
        batchSize = 0;

        if (objectCreation.ArgumentList == null)
            return false;

        foreach (var argument in objectCreation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text == "BatchSize")
                return TryExtractIntValue(argument.Expression, out batchSize);

            // If it's the first argument and not named, assume it's BatchSize
            if (argument.NameColon == null && objectCreation.ArgumentList.Arguments.IndexOf(argument) == 0)
                return TryExtractIntValue(argument.Expression, out batchSize);
        }

        return false;
    }

    private static bool TryExtractTimeout(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        out double timeoutMs)
    {
        timeoutMs = 0;

        if (objectCreation.ArgumentList == null)
            return false;

        foreach (var argument in objectCreation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text == "Timeout")
                return TryExtractTimeSpanValue(argument.Expression, semanticModel, out timeoutMs);

            // If it's the second argument and not named, assume it's Timeout
            if (argument.NameColon == null && objectCreation.ArgumentList.Arguments.IndexOf(argument) == 1)
                return TryExtractTimeSpanValue(argument.Expression, semanticModel, out timeoutMs);
        }

        return false;
    }

    private static bool TryExtractBatchingStrategyBatchSize(
        ObjectCreationExpressionSyntax objectCreation,
        out int batchSize)
    {
        batchSize = 0;

        if (objectCreation.ArgumentList == null)
            return false;

        foreach (var argument in objectCreation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text == "BatchSize")
                return TryExtractIntValue(argument.Expression, out batchSize);

            // If it's the first argument and not named, assume it's BatchSize
            if (argument.NameColon == null && objectCreation.ArgumentList.Arguments.IndexOf(argument) == 0)
                return TryExtractIntValue(argument.Expression, out batchSize);
        }

        return false;
    }

    private static bool TryExtractMaxWaitTime(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        out double maxWaitTimeMs)
    {
        maxWaitTimeMs = 0;

        if (objectCreation.ArgumentList == null)
            return false;

        foreach (var argument in objectCreation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text == "MaxWaitTime")
                return TryExtractTimeSpanValue(argument.Expression, semanticModel, out maxWaitTimeMs);

            // If it's the second argument and not named, assume it's MaxWaitTime
            if (argument.NameColon == null && objectCreation.ArgumentList.Arguments.IndexOf(argument) == 1)
                return TryExtractTimeSpanValue(argument.Expression, semanticModel, out maxWaitTimeMs);
        }

        return false;
    }

    private static bool TryExtractIntValue(ExpressionSyntax expression, out int value)
    {
        value = 0;

        if (expression is LiteralExpressionSyntax literal)
        {
            if (int.TryParse(literal.Token.ValueText, out var parsedValue))
            {
                value = parsedValue;
                return true;
            }
        }

        return false;
    }

    private static bool TryExtractTimeSpanValue(ExpressionSyntax expression, SemanticModel semanticModel, out double milliseconds)
    {
        milliseconds = 0;

        // Handle TimeSpan.FromXxx methods
        if (expression is InvocationExpressionSyntax invocation)
        {
            if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
            {
                var methodName = memberAccess.Name.Identifier.Text;
                var containingType = semanticModel.GetTypeInfo(memberAccess.Expression).Type?.Name;

                if (containingType == "TimeSpan")
                {
                    if (invocation.ArgumentList?.Arguments.Count > 0 &&
                        TryExtractDoubleValue(invocation.ArgumentList.Arguments[0].Expression, out var value))
                    {
                        milliseconds = methodName switch
                        {
                            "FromMilliseconds" => value,
                            "FromSeconds" => value * 1000,
                            "FromMinutes" => value * 60000,
                            "FromHours" => value * 3600000,
                            "FromDays" => value * 86400000,
                            _ => 0,
                        };

                        return milliseconds > 0;
                    }
                }
            }
        }

        return false;
    }

    private static bool TryExtractDoubleValue(ExpressionSyntax expression, out double value)
    {
        value = 0;

        if (expression is LiteralExpressionSyntax literal)
        {
            if (double.TryParse(literal.Token.ValueText, out var parsedValue))
            {
                value = parsedValue;
                return true;
            }
        }

        return false;
    }

    private static List<(string title, int batchSize, double timeoutMs)> GenerateBatchingOptionsFixes(
        int currentBatchSize,
        double currentTimeoutMs)
    {
        var fixes = new List<(string, int, double)>();

        // Large batch size with short timeout
        if (currentBatchSize > 100 && currentTimeoutMs < 1000)
        {
            fixes.Add(("Increase timeout to 5 seconds for large batch", currentBatchSize, 5000));
            fixes.Add(("Reduce batch size to 50 with current timeout", 50, currentTimeoutMs));
            fixes.Add(("Use balanced configuration (100, 2s)", 100, 2000));
        }

        // Small batch size with long timeout
        else if (currentBatchSize < 10 && currentTimeoutMs > 10000)
        {
            fixes.Add(("Reduce timeout to 2 seconds for small batch", currentBatchSize, 2000));
            fixes.Add(("Increase batch size to 50 with current timeout", 50, currentTimeoutMs));
            fixes.Add(("Use balanced configuration (50, 2s)", 50, 2000));
        }

        // Medium batch size with disproportionate timeout
        else if (currentBatchSize is >= 10 and <= 100)
        {
            var expectedMinTimeout = currentBatchSize * 10;
            var expectedMaxTimeout = currentBatchSize * 100;

            if (currentTimeoutMs < expectedMinTimeout)
            {
                fixes.Add(($"Increase timeout to {expectedMinTimeout}ms for batch size {currentBatchSize}", currentBatchSize, expectedMinTimeout));
                fixes.Add(("Use balanced timeout (1s per 10 items)", currentBatchSize, currentBatchSize * 100));
            }

            if (currentTimeoutMs > expectedMaxTimeout)
            {
                fixes.Add(($"Reduce timeout to {expectedMaxTimeout}ms for batch size {currentBatchSize}", currentBatchSize, expectedMaxTimeout));
                fixes.Add(("Use balanced timeout (1s per 10 items)", currentBatchSize, currentBatchSize * 100));
            }
        }

        // Always add a balanced option
        fixes.Add(("Use balanced configuration (50, 2s)", 50, 2000));

        return fixes;
    }

    private static List<(string title, int batchSize, double maxWaitTimeMs)> GenerateBatchingStrategyFixes(
        int currentBatchSize,
        double currentMaxWaitTimeMs)
    {
        var fixes = new List<(string, int, double)>();

        // Large batch size with short timeout
        if (currentBatchSize > 100 && currentMaxWaitTimeMs < 1000)
        {
            fixes.Add(("Increase MaxWaitTime to 5 seconds for large batch", currentBatchSize, 5000));
            fixes.Add(("Reduce batch size to 50 with current MaxWaitTime", 50, currentMaxWaitTimeMs));
            fixes.Add(("Use balanced configuration (100, 2s)", 100, 2000));
        }

        // Small batch size with long timeout
        else if (currentBatchSize < 10 && currentMaxWaitTimeMs > 10000)
        {
            fixes.Add(("Reduce MaxWaitTime to 2 seconds for small batch", currentBatchSize, 2000));
            fixes.Add(("Increase batch size to 50 with current MaxWaitTime", 50, currentMaxWaitTimeMs));
            fixes.Add(("Use balanced configuration (50, 2s)", 50, 2000));
        }

        // Medium batch size with disproportionate timeout
        else if (currentBatchSize is >= 10 and <= 100)
        {
            var expectedMinTimeout = currentBatchSize * 10;
            var expectedMaxTimeout = currentBatchSize * 100;

            if (currentMaxWaitTimeMs < expectedMinTimeout)
            {
                fixes.Add(($"Increase MaxWaitTime to {expectedMinTimeout}ms for batch size {currentBatchSize}", currentBatchSize, expectedMinTimeout));
                fixes.Add(("Use balanced MaxWaitTime (1s per 10 items)", currentBatchSize, currentBatchSize * 100));
            }

            if (currentMaxWaitTimeMs > expectedMaxTimeout)
            {
                fixes.Add(($"Reduce MaxWaitTime to {expectedMaxTimeout}ms for batch size {currentBatchSize}", currentBatchSize, expectedMaxTimeout));
                fixes.Add(("Use balanced MaxWaitTime (1s per 10 items)", currentBatchSize, currentBatchSize * 100));
            }
        }

        // Always add a balanced option
        fixes.Add(("Use balanced configuration (50, 2s)", 50, 2000));

        return fixes;
    }
}
