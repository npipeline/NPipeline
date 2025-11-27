using System.Collections.Immutable;
using System.Composition;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace NPipeline.Analyzers;

/// <summary>
///     Code fix provider that suggests optimal parallelism configurations.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(InappropriateParallelismConfigurationCodeFixProvider))]
[Shared]
public sealed class InappropriateParallelismConfigurationCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [InappropriateParallelismConfigurationAnalyzer.DiagnosticId];

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

        // Register different code fixes based on diagnostic type
        if (node is ObjectCreationExpressionSyntax objectCreation)
            await RegisterObjectCreationFixes(context, objectCreation, diagnostic);
        else if (node is InvocationExpressionSyntax invocation)
            await RegisterInvocationFixes(context, invocation, diagnostic);
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

        if (typeSymbol.Name == "ParallelExecutionStrategy")
            await RegisterParallelExecutionStrategyFixes(context, objectCreation, semanticModel, diagnostic);
        else if (typeSymbol.Name == "ParallelOptions")
            await RegisterParallelOptionsFixes(context, objectCreation, semanticModel, diagnostic);
    }

    private static Task RegisterParallelExecutionStrategyFixes(
        CodeFixContext context,
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        Diagnostic diagnostic)
    {
        // Try to extract current parallelism value
        if (!TryExtractCurrentParallelism(objectCreation, semanticModel, out var currentParallelism))
            return Task.CompletedTask;

        // Determine workload type
        var workloadType = DetermineWorkloadType(objectCreation);

        // Generate fixes based on workload type
        var fixes = GenerateParallelExecutionStrategyFixes(workloadType, currentParallelism);

        foreach (var (title, newParallelism) in fixes)
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    title,
                    cancellationToken => UpdateParallelExecutionStrategyAsync(context.Document, objectCreation, newParallelism, cancellationToken),
                    nameof(InappropriateParallelismConfigurationCodeFixProvider)),
                diagnostic);
        }

        return Task.CompletedTask;
    }

    private static Task RegisterParallelOptionsFixes(
        CodeFixContext context,
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        Diagnostic diagnostic)
    {
        // Try to extract current parallelism and preserve ordering values
        if (!TryExtractCurrentParallelism(objectCreation, semanticModel, out var currentParallelism))
            return Task.CompletedTask;

        var preserveOrdering = TryExtractPreserveOrdering(objectCreation, out var preserveOrderingValue) && preserveOrderingValue;

        // Generate fixes for PreserveOrdering with high parallelism
        if (preserveOrdering && currentParallelism > 4)
        {
            // Fix 1: Reduce parallelism
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Reduce parallelism to 4",
                    cancellationToken => UpdateParallelOptionsAsync(context.Document, objectCreation, 4, true, cancellationToken),
                    nameof(InappropriateParallelismConfigurationCodeFixProvider)),
                diagnostic);

            // Fix 2: Disable preserve ordering
            context.RegisterCodeFix(
                CodeAction.Create(
                    "Disable PreserveOrdering",
                    cancellationToken => UpdateParallelOptionsAsync(context.Document, objectCreation, currentParallelism, false, cancellationToken),
                    nameof(InappropriateParallelismConfigurationCodeFixProvider)),
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

        // Try to extract current parallelism value
        if (!TryExtractParallelismFromInvocation(invocation, out var currentParallelism))
            return;

        // Determine workload type
        var workloadType = DetermineWorkloadType(invocation);

        // Generate fixes based on workload type
        var fixes = GenerateWithParallelismFixes(workloadType, currentParallelism);

        foreach (var (title, newParallelism) in fixes)
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    title,
                    cancellationToken => UpdateWithParallelismAsync(context.Document, invocation, newParallelism, cancellationToken),
                    nameof(InappropriateParallelismConfigurationCodeFixProvider)),
                diagnostic);
        }
    }

    private static async Task<Document> UpdateParallelExecutionStrategyAsync(
        Document document,
        ObjectCreationExpressionSyntax objectCreation,
        int newParallelism,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create new argument with the suggested parallelism
        var newArgument = SyntaxFactory.Argument(
            SyntaxFactory.NameColon("degreeOfParallelism"),
            SyntaxFactory.Token(SyntaxKind.ColonToken),
            SyntaxFactory.LiteralExpression(SyntaxKind.NumericLiteralExpression, SyntaxFactory.Literal(newParallelism)));

        // Replace the first argument or add it if none exists
        ObjectCreationExpressionSyntax newObjectCreation;

        if (objectCreation.ArgumentList?.Arguments.Count > 0)
        {
            newObjectCreation = objectCreation.WithArgumentList(
                objectCreation.ArgumentList.WithArguments(
                    objectCreation.ArgumentList.Arguments.Replace(objectCreation.ArgumentList.Arguments[0], newArgument)));
        }
        else
        {
            newObjectCreation = objectCreation.WithArgumentList(
                SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(newArgument)));
        }

        var newRoot = root.ReplaceNode(objectCreation, newObjectCreation);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> UpdateParallelOptionsAsync(
        Document document,
        ObjectCreationExpressionSyntax objectCreation,
        int newParallelism,
        bool preserveOrdering,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        var arguments = new List<ArgumentSyntax>();

        // Add or update MaxDegreeOfParallelism argument
        var parallelismArgument = SyntaxFactory.Argument(
            SyntaxFactory.NameColon("maxDegreeOfParallelism"),
            SyntaxFactory.Token(SyntaxKind.ColonToken),
            SyntaxFactory.LiteralExpression(SyntaxKind.NumericLiteralExpression, SyntaxFactory.Literal(newParallelism)));

        arguments.Add(parallelismArgument);

        // Add or update PreserveOrdering argument
        var orderingArgument = SyntaxFactory.Argument(
            SyntaxFactory.NameColon("preserveOrdering"),
            SyntaxFactory.Token(SyntaxKind.ColonToken),
            SyntaxFactory.LiteralExpression(SyntaxKind.TrueLiteralExpression, SyntaxFactory.Token(SyntaxKind.TrueKeyword)));

        if (!preserveOrdering)
        {
            orderingArgument = orderingArgument.WithExpression(
                SyntaxFactory.LiteralExpression(SyntaxKind.FalseLiteralExpression, SyntaxFactory.Token(SyntaxKind.FalseKeyword)));
        }

        arguments.Add(orderingArgument);

        var newObjectCreation = objectCreation.WithArgumentList(
            SyntaxFactory.ArgumentList(SyntaxFactory.SeparatedList(arguments)));

        var newRoot = root.ReplaceNode(objectCreation, newObjectCreation);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> UpdateWithParallelismAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        int newParallelism,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create new argument with the suggested parallelism
        var newArgument = SyntaxFactory.Argument(
            SyntaxFactory.LiteralExpression(SyntaxKind.NumericLiteralExpression, SyntaxFactory.Literal(newParallelism)));

        // Replace the first argument
        var newInvocation = invocation;

        if (invocation.ArgumentList?.Arguments.Count > 0)
        {
            newInvocation = invocation.WithArgumentList(
                invocation.ArgumentList.WithArguments(
                    invocation.ArgumentList.Arguments.Replace(invocation.ArgumentList.Arguments[0], newArgument)));
        }

        var newRoot = root.ReplaceNode(invocation, newInvocation);
        return document.WithSyntaxRoot(newRoot);
    }

    private static bool TryExtractCurrentParallelism(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        out int parallelism)
    {
        parallelism = 0;

        if (objectCreation.ArgumentList == null)
            return false;

        foreach (var argument in objectCreation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text is "degreeOfParallelism" or "maxDegreeOfParallelism")
                return TryExtractIntValue(argument.Expression, out parallelism);

            // If it's the first argument and not named, assume it's degreeOfParallelism
            if (argument.NameColon == null && objectCreation.ArgumentList.Arguments.IndexOf(argument) == 0)
                return TryExtractIntValue(argument.Expression, out parallelism);
        }

        return false;
    }

    private static bool TryExtractParallelismFromInvocation(
        InvocationExpressionSyntax invocation,
        out int parallelism)
    {
        parallelism = 0;

        if (invocation.ArgumentList == null || invocation.ArgumentList.Arguments.Count == 0)
            return false;

        var firstArgument = invocation.ArgumentList.Arguments[0];
        return TryExtractIntValue(firstArgument.Expression, out parallelism);
    }

    private static bool TryExtractPreserveOrdering(
        ObjectCreationExpressionSyntax objectCreation,
        out bool preserveOrdering)
    {
        preserveOrdering = false;

        if (objectCreation.ArgumentList == null)
            return false;

        foreach (var argument in objectCreation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text == "preserveOrdering")
                return TryExtractBoolValue(argument.Expression, out preserveOrdering);
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

    private static bool TryExtractBoolValue(ExpressionSyntax expression, out bool value)
    {
        value = false;

        if (expression is LiteralExpressionSyntax literal &&
            bool.TryParse(literal.Token.ValueText, out var parsedValue))
        {
            value = parsedValue;
            return true;
        }

        return false;
    }

    private static WorkloadType DetermineWorkloadType(SyntaxNode node)
    {
        // Find the containing class
        var classDeclaration = node.FirstAncestorOrSelf<ClassDeclarationSyntax>();

        if (classDeclaration == null)
            return WorkloadType.Unknown;

        var className = classDeclaration.Identifier.Text;

        // Check for I/O-bound indicators
        var ioIndicators = new[] { "Io", "Database", "File", "Network", "Http", "Api", "Stream" };

        if (ioIndicators.Any(indicator => className.Contains(indicator, StringComparison.OrdinalIgnoreCase)))
            return WorkloadType.IoBound;

        // Check for CPU-bound indicators
        var cpuIndicators = new[] { "Cpu", "Compute", "Process", "Transform", "Calculate", "Math" };

        if (cpuIndicators.Any(indicator => className.Contains(indicator, StringComparison.OrdinalIgnoreCase)))
            return WorkloadType.CpuBound;

        // Default to CPU-bound for transforms
        return className.Contains("Transform", StringComparison.OrdinalIgnoreCase)
            ? WorkloadType.CpuBound
            : WorkloadType.Unknown;
    }

    private static List<(string title, int parallelism)> GenerateParallelExecutionStrategyFixes(
        WorkloadType workloadType,
        int currentParallelism)
    {
        var fixes = new List<(string, int)>();

        switch (workloadType)
        {
            case WorkloadType.CpuBound:
                if (currentParallelism > 4)
                    fixes.Add(("Use processor count (4)", 4));
                else if (currentParallelism == 1)
                    fixes.Add(("Use processor count (4)", 4));

                break;
            case WorkloadType.IoBound:
                if (currentParallelism > 8)
                    fixes.Add(("Use moderate parallelism (8)", 8));

                fixes.Add(("Use processor count (4)", 4));
                break;
            case WorkloadType.Unknown:
                if (currentParallelism > 8)
                {
                    fixes.Add(("Use processor count (4)", 4));
                    fixes.Add(("Use moderate parallelism (8)", 8));
                }
                else
                    fixes.Add(("Use processor count (4)", 4));

                break;
        }

        return fixes;
    }

    private static List<(string title, int parallelism)> GenerateWithParallelismFixes(
        WorkloadType workloadType,
        int currentParallelism)
    {
        var fixes = new List<(string, int)>();

        switch (workloadType)
        {
            case WorkloadType.CpuBound:
                if (currentParallelism > 4)
                    fixes.Add(("Use processor count (4)", 4));
                else if (currentParallelism == 1)
                    fixes.Add(("Use processor count (4)", 4));

                break;
            case WorkloadType.IoBound:
                if (currentParallelism > 8)
                    fixes.Add(("Use moderate parallelism (8)", 8));

                fixes.Add(("Use processor count (4)", 4));
                break;
            case WorkloadType.Unknown:
                if (currentParallelism > 8)
                {
                    fixes.Add(("Use processor count (4)", 4));
                    fixes.Add(("Use moderate parallelism (8)", 8));
                }
                else
                    fixes.Add(("Use processor count (4)", 4));

                break;
        }

        return fixes;
    }

    private enum WorkloadType
    {
        Unknown,
        CpuBound,
        IoBound,
    }
}
