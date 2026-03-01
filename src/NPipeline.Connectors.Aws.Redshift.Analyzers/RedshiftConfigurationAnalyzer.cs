using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Connectors.Aws.Redshift.Analyzers;

/// <summary>
///     Roslyn analyzer that detects configuration issues in RedshiftConfiguration initializations.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public class RedshiftConfigurationAnalyzer : DiagnosticAnalyzer
{
    private const string RedshiftConfigurationTypeName = "RedshiftConfiguration";
    private const string WriteStrategyProperty = "WriteStrategy";
    private const string IamRoleArnProperty = "IamRoleArn";
    private const string S3BucketNameProperty = "S3BucketName";
    private const string UseUpsertProperty = "UseUpsert";
    private const string UpsertKeyColumnsProperty = "UpsertKeyColumns";
    private const string BatchSizeProperty = "BatchSize";
    private const string CopyFromS3Value = "CopyFromS3";

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics { get; } = ImmutableArray.Create(
        Descriptors.REDSHIFT001_MissingIamRoleArn,
        Descriptors.REDSHIFT002_MissingS3BucketName,
        Descriptors.REDSHIFT003_MissingUpsertKeyColumns,
        Descriptors.REDSHIFT004_ConsiderCopyFromS3ForLargeBatches);

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

        // Get the type symbol for the object being created
        var typeInfo = context.SemanticModel.GetTypeInfo(node, context.CancellationToken);

        if (typeInfo.Type is not { } type)
            return;

        if (type.Name != RedshiftConfigurationTypeName)
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

        CheckCopyFromS3Configuration(context, node, propertyAssignments);
        CheckUpsertConfiguration(context, node, propertyAssignments);
        CheckBatchSizeWarning(context, node, propertyAssignments);
    }

    private void CheckCopyFromS3Configuration(
        SyntaxNodeAnalysisContext context,
        BaseObjectCreationExpressionSyntax node,
        Dictionary<string, string> properties)
    {
        if (!properties.TryGetValue(WriteStrategyProperty, out var strategyValue))
            return;

        if (!strategyValue.Contains(CopyFromS3Value))
            return;

        // REDSHIFT001: Missing IamRoleArn
        if (!properties.TryGetValue(IamRoleArnProperty, out var iamRoleValue) ||
            IsEmptyOrNullOrEmptyString(iamRoleValue))
        {
            var diagnostic = Diagnostic.Create(
                Descriptors.REDSHIFT001_MissingIamRoleArn,
                node.GetLocation());

            context.ReportDiagnostic(diagnostic);
        }

        // REDSHIFT002: Missing S3BucketName
        if (!properties.TryGetValue(S3BucketNameProperty, out var s3BucketValue) ||
            IsEmptyOrNullOrEmptyString(s3BucketValue))
        {
            var diagnostic = Diagnostic.Create(
                Descriptors.REDSHIFT002_MissingS3BucketName,
                node.GetLocation());

            context.ReportDiagnostic(diagnostic);
        }
    }

    private void CheckUpsertConfiguration(
        SyntaxNodeAnalysisContext context,
        BaseObjectCreationExpressionSyntax node,
        Dictionary<string, string> properties)
    {
        if (!properties.TryGetValue(UseUpsertProperty, out var useUpsertValue))
            return;

        if (useUpsertValue != "true" && useUpsertValue != "True")
            return;

        // REDSHIFT003: Missing UpsertKeyColumns
        if (!properties.TryGetValue(UpsertKeyColumnsProperty, out var upsertKeyValue) ||
            IsEmptyArray(upsertKeyValue))
        {
            var diagnostic = Diagnostic.Create(
                Descriptors.REDSHIFT003_MissingUpsertKeyColumns,
                node.GetLocation());

            context.ReportDiagnostic(diagnostic);
        }
    }

    private void CheckBatchSizeWarning(
        SyntaxNodeAnalysisContext context,
        BaseObjectCreationExpressionSyntax node,
        Dictionary<string, string> properties)
    {
        // Only warn if using Batch strategy
        if (properties.TryGetValue(WriteStrategyProperty, out var strategyValue) &&
            strategyValue.Contains(CopyFromS3Value))
            return;

        if (!properties.TryGetValue(BatchSizeProperty, out var batchSizeValue))
            return;

        if (int.TryParse(batchSizeValue, out var batchSize) && batchSize > 10_000)
        {
            var diagnostic = Diagnostic.Create(
                Descriptors.REDSHIFT004_ConsiderCopyFromS3ForLargeBatches,
                node.GetLocation(),
                batchSize);

            context.ReportDiagnostic(diagnostic);
        }
    }

    private static bool IsEmptyOrNullOrEmptyString(string value)
    {
        return value == "null" ||
               value == "\"\"" ||
               value == "string.Empty" ||
               value == "String.Empty";
    }

    private static bool IsEmptyArray(string value)
    {
        if (value == "null" || value == "[]" || value == "Array.Empty<string>()")
            return true;

        var normalized = value.Replace(" ", string.Empty);
        return normalized is "newstring[]{}" or "new[]{}";
    }
}

file static class Descriptors
{
    public static readonly DiagnosticDescriptor REDSHIFT001_MissingIamRoleArn = new(
        DiagnosticIds.MissingIamRoleArn,
        "Missing IamRoleArn for CopyFromS3 strategy",
        "IamRoleArn must be set when WriteStrategy is CopyFromS3",
        "Configuration",
        DiagnosticSeverity.Error,
        true,
        "The CopyFromS3 write strategy requires an IAM role ARN for Redshift to access S3.");

    public static readonly DiagnosticDescriptor REDSHIFT002_MissingS3BucketName = new(
        DiagnosticIds.MissingS3BucketName,
        "Missing S3BucketName for CopyFromS3 strategy",
        "S3BucketName must be set when WriteStrategy is CopyFromS3",
        "Configuration",
        DiagnosticSeverity.Error,
        true,
        "The CopyFromS3 write strategy requires an S3 bucket name for staging data files.");

    public static readonly DiagnosticDescriptor REDSHIFT003_MissingUpsertKeyColumns = new(
        DiagnosticIds.MissingUpsertKeyColumns,
        "Missing UpsertKeyColumns for upsert",
        "UpsertKeyColumns must be set when UseUpsert is true",
        "Configuration",
        DiagnosticSeverity.Error,
        true,
        "Upsert operations require key columns to match existing rows.");

    public static readonly DiagnosticDescriptor REDSHIFT004_ConsiderCopyFromS3ForLargeBatches = new(
        DiagnosticIds.ConsiderCopyFromS3ForLargeBatches,
        "Consider CopyFromS3 for large batch sizes",
        "Batch size {0} is large. Consider using CopyFromS3 strategy for better performance with large data volumes.",
        "Performance",
        DiagnosticSeverity.Warning,
        true,
        "For batch sizes over 10,000 rows, the CopyFromS3 strategy typically provides better performance.");
}
