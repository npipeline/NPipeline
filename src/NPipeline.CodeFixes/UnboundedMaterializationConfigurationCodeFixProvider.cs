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
///     Code fix provider that suggests fixes for unbounded materialization configuration.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(UnboundedMaterializationConfigurationCodeFixProvider))]
[Shared]
public sealed class UnboundedMaterializationConfigurationCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [UnboundedMaterializationConfigurationAnalyzer.UnboundedMaterializationConfigurationId];

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

        // Register code fixes for PipelineRetryOptions object creation
        if (node is ObjectCreationExpressionSyntax objectCreation)
            await RegisterObjectCreationFixes(context, objectCreation, diagnostic);

        // Also handle the case where the diagnostic points to a specific argument
        else if (node is ArgumentSyntax argument)
        {
            var parentObjectCreation = node.FirstAncestorOrSelf<ObjectCreationExpressionSyntax>();

            if (parentObjectCreation != null)
                await RegisterObjectCreationFixes(context, parentObjectCreation, diagnostic);
        }
    }

    private static async Task RegisterObjectCreationFixes(
        CodeFixContext context,
        ObjectCreationExpressionSyntax objectCreation,
        Diagnostic diagnostic)
    {
        var semanticModel = await context.Document.GetSemanticModelAsync(context.CancellationToken);

        if (semanticModel == null)
            return;

        var typeInfo = semanticModel.GetTypeInfo(objectCreation);

        if (!IsPipelineRetryOptions(typeInfo.Type))
            return;

        // Determine workload type to suggest appropriate values
        var workloadType = DetermineWorkloadType(objectCreation);

        // Generate fixes based on workload type
        var fixes = GenerateFixes(workloadType);

        foreach (var (title, maxItems) in fixes)
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    title,
                    cancellationToken => UpdateMaxMaterializedItemsAsync(context.Document, objectCreation, maxItems, cancellationToken),
                    nameof(UnboundedMaterializationConfigurationCodeFixProvider)),
                diagnostic);
        }
    }

    private static async Task<Document> UpdateMaxMaterializedItemsAsync(
        Document document,
        ObjectCreationExpressionSyntax objectCreation,
        int maxItems,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create new MaxMaterializedItems argument
        var newArgument = SyntaxFactory.Argument(
            SyntaxFactory.NameColon("MaxMaterializedItems"),
            SyntaxFactory.Token(SyntaxKind.ColonToken),
            SyntaxFactory.LiteralExpression(
                SyntaxKind.NumericLiteralExpression,
                SyntaxFactory.Literal(maxItems)));

        // Check if MaxMaterializedItems argument already exists
        var hasMaxMaterializedItemsArg = false;
        ArgumentSyntax? existingMaxMaterializedItemsArg = null;

        if (objectCreation.ArgumentList != null)
        {
            foreach (var argument in objectCreation.ArgumentList.Arguments)
            {
                if (IsMaxMaterializedItemsArgument(argument))
                {
                    hasMaxMaterializedItemsArg = true;
                    existingMaxMaterializedItemsArg = argument;
                    break;
                }
            }
        }

        ObjectCreationExpressionSyntax newObjectCreation;

        if (hasMaxMaterializedItemsArg && existingMaxMaterializedItemsArg != null)
        {
            // Replace existing MaxMaterializedItems argument
            newObjectCreation = objectCreation.WithArgumentList(
                objectCreation.ArgumentList!.WithArguments(
                    objectCreation.ArgumentList.Arguments.Replace(existingMaxMaterializedItemsArg, newArgument)));
        }
        else
        {
            // Add MaxMaterializedItems argument
            if (objectCreation.ArgumentList != null && objectCreation.ArgumentList.Arguments.Count > 0)
            {
                newObjectCreation = objectCreation.WithArgumentList(
                    objectCreation.ArgumentList.WithArguments(
                        objectCreation.ArgumentList.Arguments.Add(newArgument)));
            }
            else
            {
                newObjectCreation = objectCreation.WithArgumentList(
                    SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(newArgument)));
            }
        }

        var newRoot = root.ReplaceNode(objectCreation, newObjectCreation);
        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Determines if the type is PipelineRetryOptions.
    /// </summary>
    private static bool IsPipelineRetryOptions(ITypeSymbol? type)
    {
        return type != null &&
               type.Name == "PipelineRetryOptions" &&
               type.ContainingNamespace?.Name == "Configuration" &&
               type.ContainingNamespace.ContainingNamespace?.Name == "NPipeline";
    }

    /// <summary>
    ///     Determines if an argument is the MaxMaterializedItems parameter.
    /// </summary>
    private static bool IsMaxMaterializedItemsArgument(ArgumentSyntax argument)
    {
        // Check for named argument
        if (argument.NameColon != null)
        {
            var argumentName = argument.NameColon.Name.Identifier.Text;
            return argumentName.Equals("MaxMaterializedItems", StringComparison.OrdinalIgnoreCase);
        }

        // For positional arguments, we need to check the parameter position
        // MaxMaterializedItems is the 4th parameter (index 3) in PipelineRetryOptions constructor
        return GetParameterPosition(argument) == 3;
    }

    /// <summary>
    ///     Gets the zero-based position of a positional argument.
    /// </summary>
    private static int GetParameterPosition(ArgumentSyntax argument)
    {
        if (argument.NameColon != null)
            return -1; // Named argument

        // Find the position by counting preceding positional arguments
        if (argument.Parent is not ArgumentListSyntax parent)
            return -1;

        var position = 0;

        foreach (var arg in parent.Arguments)
        {
            if (arg == argument)
                return position;

            if (arg.NameColon == null) // Positional argument
                position++;
        }

        return -1;
    }

    /// <summary>
    ///     Determines the workload type based on context.
    /// </summary>
    private static WorkloadType DetermineWorkloadType(SyntaxNode node)
    {
        // Find the containing class
        var classDeclaration = node.FirstAncestorOrSelf<ClassDeclarationSyntax>();

        if (classDeclaration == null)
            return WorkloadType.Unknown;

        var className = classDeclaration.Identifier.Text;

        // Check for high-throughput indicators
        var highThroughputIndicators = new[] { "Stream", "Batch", "Bulk", "High", "Performance", "Large" };

        if (highThroughputIndicators.Any(indicator => className.IndexOf(indicator, StringComparison.OrdinalIgnoreCase) >= 0))
            return WorkloadType.HighThroughput;

        // Check for memory-constrained indicators
        var memoryConstrainedIndicators = new[] { "Mobile", "Embedded", "Lightweight", "Memory", "Constrained" };

        if (memoryConstrainedIndicators.Any(indicator => className.IndexOf(indicator, StringComparison.OrdinalIgnoreCase) >= 0))
            return WorkloadType.MemoryConstrained;

        // Default to standard
        return WorkloadType.Standard;
    }

    /// <summary>
    ///     Generates appropriate MaxMaterializedItems values based on workload type.
    /// </summary>
    private static List<(string title, int maxItems)> GenerateFixes(WorkloadType workloadType)
    {
        var fixes = new List<(string, int)>();

        switch (workloadType)
        {
            case WorkloadType.HighThroughput:
                fixes.Add(("Set MaxMaterializedItems to 10000 for high-throughput scenarios", 10000));
                fixes.Add(("Set MaxMaterializedItems to 5000 for high-throughput scenarios", 5000));
                fixes.Add(("Set MaxMaterializedItems to 1000 for high-throughput scenarios", 1000));
                break;

            case WorkloadType.MemoryConstrained:
                fixes.Add(("Set MaxMaterializedItems to 100 for memory-constrained scenarios", 100));
                fixes.Add(("Set MaxMaterializedItems to 250 for memory-constrained scenarios", 250));
                fixes.Add(("Set MaxMaterializedItems to 500 for memory-constrained scenarios", 500));
                break;

            case WorkloadType.Standard:
                fixes.Add(("Set MaxMaterializedItems to 1000 (recommended)", 1000));
                fixes.Add(("Set MaxMaterializedItems to 500", 500));
                fixes.Add(("Set MaxMaterializedItems to 2500", 2500));
                break;

            case WorkloadType.Unknown:
            default:
                fixes.Add(("Set MaxMaterializedItems to 1000 (recommended)", 1000));
                fixes.Add(("Set MaxMaterializedItems to 500", 500));
                fixes.Add(("Set MaxMaterializedItems to 2500", 2500));
                break;
        }

        return fixes;
    }

    private enum WorkloadType
    {
        Unknown,
        Standard,
        HighThroughput,
        MemoryConstrained,
    }
}
