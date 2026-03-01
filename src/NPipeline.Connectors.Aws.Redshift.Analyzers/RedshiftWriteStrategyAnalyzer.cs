using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Connectors.Aws.Redshift.Analyzers;

/// <summary>
///     Roslyn analyzer that detects inefficient write strategy patterns in Redshift configurations.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public class RedshiftWriteStrategyAnalyzer : DiagnosticAnalyzer
{
    private const string RedshiftConfigurationTypeName = "RedshiftConfiguration";
    private const string RedshiftSinkTypeName = "RedshiftSink";
    private const string WriteStrategyProperty = "WriteStrategy";
    private const string BatchSizeProperty = "BatchSize";
    private const string CopyFromS3Value = "CopyFromS3";
    private const string BatchValue = "Batch";

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = ImmutableArray.Create(
        WriteStrategyDescriptors.REDSHIFT005_InefficientWriteStrategy,
        WriteStrategyDescriptors.REDSHIFT006_MissingWriteStrategy);

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterSyntaxNodeAction(AnalyzeObjectCreation, SyntaxKind.ObjectCreationExpression);
        context.RegisterSyntaxNodeAction(AnalyzeObjectCreation, SyntaxKind.ImplicitObjectCreationExpression);
    }

    private void AnalyzeObjectCreation(SyntaxNodeAnalysisContext context)
    {
        var node = (BaseObjectCreationExpressionSyntax)context.Node;
        var typeInfo = context.SemanticModel.GetTypeInfo(node, context.CancellationToken);

        if (typeInfo.Type is not { } type)
            return;

        // Check for RedshiftConfiguration or RedshiftSink types
        if (type.Name != RedshiftConfigurationTypeName && type.Name != RedshiftSinkTypeName)
            return;

        var initializer = node.Initializer;

        if (initializer is null)
            return;

        var propertyAssignments = initializer.Expressions
            .OfType<AssignmentExpressionSyntax>()
            .ToDictionary(
                a => a.Left.ToString().Trim(),
                a => a.Right.ToString().Trim(),
                StringComparer.OrdinalIgnoreCase);

        CheckInefficientWriteStrategy(context, node, propertyAssignments);
        CheckMissingWriteStrategy(context, node, propertyAssignments);
    }

    private void CheckInefficientWriteStrategy(
        SyntaxNodeAnalysisContext context,
        BaseObjectCreationExpressionSyntax node,
        Dictionary<string, string> properties)
    {
        // Check if using Batch strategy with high-frequency writes scenario
        if (!properties.TryGetValue(WriteStrategyProperty, out var strategyValue))
            return;

        if (!strategyValue.Contains(BatchValue))
            return;

        // If BatchSize is set and very small, suggest CopyFromS3 might be overkill
        // but if BatchSize is large, it's already handled by REDSHIFT004
        // This analyzer focuses on other inefficiencies

        // Check for patterns that suggest inefficient use:
        // - Using Batch when dealing with bulk operations (detected via context)
        // For now, this is a placeholder for additional strategy analysis
    }

    private void CheckMissingWriteStrategy(
        SyntaxNodeAnalysisContext context,
        BaseObjectCreationExpressionSyntax node,
        Dictionary<string, string> properties)
    {
        // If no WriteStrategy is explicitly set, warn about relying on defaults
        if (properties.ContainsKey(WriteStrategyProperty))
            return;

        // Only warn if other write-related properties are set
        var hasWriteRelatedProperties = properties.ContainsKey(BatchSizeProperty) ||
                                        properties.ContainsKey("UseUpsert") ||
                                        properties.ContainsKey("S3BucketName") ||
                                        properties.ContainsKey("IamRoleArn");

        if (!hasWriteRelatedProperties)
            return;

        var diagnostic = Diagnostic.Create(
            WriteStrategyDescriptors.REDSHIFT006_MissingWriteStrategy,
            node.GetLocation());

        context.ReportDiagnostic(diagnostic);
    }
}

file static class WriteStrategyDescriptors
{
    public static readonly DiagnosticDescriptor REDSHIFT005_InefficientWriteStrategy = new(
        "REDSHIFT005",
        "Inefficient write strategy detected",
        "Consider reviewing the write strategy configuration for optimal performance",
        "Performance",
        DiagnosticSeverity.Warning,
        true,
        "The current write strategy configuration may not be optimal for the expected data volume.");

    public static readonly DiagnosticDescriptor REDSHIFT006_MissingWriteStrategy = new(
        "REDSHIFT006",
        "Missing explicit write strategy",
        "WriteStrategy should be explicitly set when configuring write-related properties",
        "Configuration",
        DiagnosticSeverity.Info,
        true,
        "Explicitly setting WriteStrategy improves code clarity and avoids relying on default behavior.");
}
