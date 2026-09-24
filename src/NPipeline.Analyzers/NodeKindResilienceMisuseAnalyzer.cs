using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Detects <c>ItemRetry</c>, <c>NodeRestart</c>, or <c>CircuitBreaker</c> set through
///     <c>PipelineBuilder.WithResilience(handle, ...)</c> for a source, sink, aggregate, or join handle. Only transform
///     nodes retry items, restart their stream, or have their attempts guarded by a breaker, so building the pipeline
///     fails. This reports the same mistake at compile time.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class NodeKindResilienceMisuseAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for a transform-only resilience setting on another kind of node.
    /// </summary>
    public const string NodeKindResilienceMisuseId = "NP9204";

    private static readonly ImmutableHashSet<string> TransformOnlySettings =
        ImmutableHashSet.Create("ItemRetry", "NodeRestart", "CircuitBreaker");

    private static readonly ImmutableDictionary<string, string> NonTransformHandles = new Dictionary<string, string>
    {
        ["SourceNodeHandle`1"] = "source",
        ["SinkNodeHandle`1"] = "sink",
        ["AggregateNodeHandle`2"] = "aggregate",
        ["JoinNodeHandle`3"] = "join",
    }.ToImmutableDictionary();

    private static readonly DiagnosticDescriptor Rule = new(
        NodeKindResilienceMisuseId,
        "Transform-only resilience setting on a non-transform node",
        "'{0}' is a {1} node, but its resilience options set {2}, which only transform nodes use; building the pipeline will fail",
        "Reliability & Error Handling",
        DiagnosticSeverity.Error,
        true,
        "Item retry, node restart, and circuit breakers apply only to transform nodes. For a source or sink, retry "
        + "reads and writes in the connector, or use NodeRetry to execute the whole node again before it has consumed input. "
        + "https://docs.npipeline.net/analyzers/reliability#np9204-transform-only-resilience-setting.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterCompilationStartAction(start =>
        {
            var builderType = start.Compilation.GetTypeByMetadataName("NPipeline.Pipeline.PipelineBuilder");
            var optionsType = start.Compilation.GetTypeByMetadataName("NPipeline.Reliability.PipelineResilienceOptions");

            if (builderType is null || optionsType is null)
                return;

            start.RegisterSyntaxNodeAction(
                nodeContext => AnalyzeInvocation(nodeContext, builderType, optionsType),
                SyntaxKind.InvocationExpression);
        });
    }

    private static void AnalyzeInvocation(SyntaxNodeAnalysisContext context, INamedTypeSymbol builderType, INamedTypeSymbol optionsType)
    {
        var invocation = (InvocationExpressionSyntax)context.Node;

        if (invocation.Expression is not MemberAccessExpressionSyntax { Name.Identifier.Text: "WithResilience" }
            || invocation.ArgumentList.Arguments.Count != 2)
            return;

        if (context.SemanticModel.GetSymbolInfo(invocation, context.CancellationToken).Symbol is not IMethodSymbol method
            || !SymbolEqualityComparer.Default.Equals(method.ContainingType, builderType)
            || method.Parameters.Length != 2)
            return;

        var handleArgument = invocation.ArgumentList.Arguments[0].Expression;
        var handleType = context.SemanticModel.GetTypeInfo(handleArgument, context.CancellationToken).Type as INamedTypeSymbol;

        if (handleType?.ContainingNamespace?.ToDisplayString() != "NPipeline.Graph"
            || !NonTransformHandles.TryGetValue(handleType.MetadataName, out var kind))
            return;

        if (invocation.ArgumentList.Arguments[1].Expression is not LambdaExpressionSyntax lambda)
            return;

        var lambdaParameter = lambda switch
        {
            SimpleLambdaExpressionSyntax simple => simple.Parameter.Identifier.Text,
            ParenthesizedLambdaExpressionSyntax { ParameterList.Parameters.Count: 1 } parenthesized =>
                parenthesized.ParameterList.Parameters[0].Identifier.Text,
            _ => null,
        };

        foreach (var assignment in lambda.Body.DescendantNodesAndSelf().OfType<AssignmentExpressionSyntax>())
        {
            if (assignment.Left is not IdentifierNameSyntax target
                || !TransformOnlySettings.Contains(target.Identifier.Text)
                || assignment.Parent is not InitializerExpressionSyntax initializer)
                continue;

            // Only initializers that build the node's options: `o with { ... }` or `new PipelineResilienceOptions { ... }`.
            var initializedType = initializer.Parent switch
            {
                WithExpressionSyntax with => context.SemanticModel.GetTypeInfo(with, context.CancellationToken).Type,
                BaseObjectCreationExpressionSyntax creation => context.SemanticModel.GetTypeInfo(creation, context.CancellationToken).Type,
                _ => null,
            };

            if (!SymbolEqualityComparer.Default.Equals(initializedType, optionsType))
                continue;

            // `ItemRetry = o.ItemRetry` keeps the pipeline's value, which the build accepts.
            if (IsPipelineValue(assignment.Right, lambdaParameter, target.Identifier.Text))
                continue;

            context.ReportDiagnostic(Diagnostic.Create(Rule, assignment.GetLocation(), handleArgument.ToString(), kind, target.Identifier.Text));
        }
    }

    private static bool IsPipelineValue(ExpressionSyntax value, string? lambdaParameter, string setting) =>
        lambdaParameter is not null
        && value is MemberAccessExpressionSyntax { Expression: IdentifierNameSyntax receiver } member
        && receiver.Identifier.Text == lambdaParameter
        && member.Name.Identifier.Text == setting;
}
