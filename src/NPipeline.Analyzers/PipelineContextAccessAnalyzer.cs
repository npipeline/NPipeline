using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects unsafe access patterns on PipelineContext properties and dictionaries.
///     This analyzer checks for:
///     1. Unsafe property access on nullable context features (PipelineErrorHandler, DeadLetterSink, StateManager, StatefulRegistry)
///     2. Dictionary access without type checking (Items, Parameters, Properties dictionaries)
///     3. Unsafe casting from dictionary values
///     4. Direct access to computed properties that might be null
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class PipelineContextAccessAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for unsafe PipelineContext access patterns.
    /// </summary>
    public const string UnsafePipelineContextAccessId = "NP9303";

    private static readonly DiagnosticDescriptor UnsafePipelineContextAccessRule = new(
        UnsafePipelineContextAccessId,
        "Unsafe access pattern on PipelineContext",
        "Unsafe access pattern '{0}' detected on PipelineContext. This can lead to NullReferenceException at runtime. Use null-conditional operators (?.) or check for null before accessing these properties.",
        "Reliability",
        DiagnosticSeverity.Warning,
        true,
        "PipelineContext has several nullable properties and dictionary access patterns that require careful handling. "
        + "Always use null-conditional operators or explicit null checks to prevent runtime exceptions. "
        + "For dictionary access, use TryGetValue pattern or null-conditional operators with proper type checking. "
        + "https://npipeline.dev/docs/core-concepts/pipeline-context/safe-access-patterns.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [UnsafePipelineContextAccessRule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze member access expressions
        context.RegisterSyntaxNodeAction(AnalyzeMemberAccess, SyntaxKind.SimpleMemberAccessExpression);

        // Register to analyze element access expressions (for dictionaries)
        context.RegisterSyntaxNodeAction(AnalyzeElementAccess, SyntaxKind.ElementAccessExpression);

        // Register to analyze invocation expressions (for method calls on nullable properties)
        context.RegisterSyntaxNodeAction(AnalyzeInvocation, SyntaxKind.InvocationExpression);
    }

    private static void AnalyzeMemberAccess(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not MemberAccessExpressionSyntax memberAccess)
            return;

        // Check if this is a PipelineContext member access
        if (!IsPipelineContextMemberAccess(memberAccess, context.SemanticModel))
            return;

        var propertyName = memberAccess.Name.Identifier.Text;

        // Check for unsafe access to nullable properties
        if (IsNullableProperty(propertyName))
        {
            // Check if null-conditional operator is used or if access is within an if-null check / pattern match
            if (!IsNullConditionalAccess(memberAccess) && !IsWithinNullCheck(memberAccess))
            {
                var diagnostic = Diagnostic.Create(
                    UnsafePipelineContextAccessRule,
                    memberAccess.GetLocation(),
                    $"Direct access to nullable property '{propertyName}'",
                    $"Use null-conditional operator: context.{propertyName}?.Method() or check for null first");

                context.ReportDiagnostic(diagnostic);
            }
        }
    }

    private static void AnalyzeElementAccess(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not ElementAccessExpressionSyntax elementAccess)
            return;

        // Check if this is a dictionary access on PipelineContext
        if (!IsPipelineContextDictionaryAccess(elementAccess, context.SemanticModel))
            return;

        // Check if null-conditional operator is used or if access is within an if-null check / pattern match
        if (!IsNullConditionalAccess(elementAccess) && !IsWithinNullCheck(elementAccess))
        {
            var diagnostic = Diagnostic.Create(
                UnsafePipelineContextAccessRule,
                elementAccess.GetLocation(),
                "Dictionary access without null-conditional operator",
                "Use null-conditional operator: context.Dictionary[key]?.Method() or check for null first");

            context.ReportDiagnostic(diagnostic);
        }
    }

    private static void AnalyzeInvocation(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not InvocationExpressionSyntax invocation)
            return;

        // Check if this is a method call on a nullable PipelineContext property
        if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
        {
            // Handle calls like: context.PipelineErrorHandler.Handle... => nestedAccess is context.PipelineErrorHandler
            var nestedAccess = memberAccess.Expression as MemberAccessExpressionSyntax;

            if (nestedAccess != null)
            {
                if (!IsPipelineContextMemberAccess(nestedAccess, context.SemanticModel))
                    return;
            }
            else
            {
                // Fallback: check outer memberAccess directly
                if (!IsPipelineContextMemberAccess(memberAccess, context.SemanticModel))
                    return;
            }

            var propertyName = nestedAccess is not null
                ? nestedAccess.Name.Identifier.Text
                : memberAccess.Expression is IdentifierNameSyntax identifier
                    ? identifier.Identifier.Text
                    : string.Empty;

            // Check if this is a method call on a nullable property and not within a null-check
            if (IsNullableProperty(propertyName) && !IsWithinNullCheck(invocation))
            {
                var methodName = memberAccess.Name.Identifier.Text;

                var diagnostic = Diagnostic.Create(
                    UnsafePipelineContextAccessRule,
                    invocation.GetLocation(),
                    $"Method call '{methodName}' on nullable property '{propertyName}' without null check",
                    $"Use null-conditional operator: context.{propertyName}?.{methodName}() or check for null first");

                context.ReportDiagnostic(diagnostic);
            }
        }
    }

    /// <summary>
    ///     Determines if member access is on a PipelineContext instance.
    /// </summary>
    private static bool IsPipelineContextMemberAccess(MemberAccessExpressionSyntax memberAccess, SemanticModel semanticModel)
    {
        // Get the type of expression being accessed
        var typeInfo = semanticModel.GetTypeInfo(memberAccess.Expression);
        var typeSymbol = typeInfo.Type;

        if (typeSymbol == null)
            return false;

        // Check if type is PipelineContext
        return typeSymbol.Name == "PipelineContext" &&
               typeSymbol.ContainingNamespace?.Name == "Pipeline";
    }

    /// <summary>
    ///     Determines if element access is on a PipelineContext dictionary property.
    /// </summary>
    private static bool IsPipelineContextDictionaryAccess(ElementAccessExpressionSyntax elementAccess, SemanticModel semanticModel)
    {
        // Check if expression is a member access to a PipelineContext property
        if (elementAccess.Expression is not MemberAccessExpressionSyntax memberAccess)
            return false;

        // Check if this is a PipelineContext member access
        if (!IsPipelineContextMemberAccess(memberAccess, semanticModel))
            return false;

        // Check if property is one of the dictionary properties
        var propertyName = memberAccess.Name.Identifier.Text;
        return propertyName is "Parameters" or "Items" or "Properties";
    }

    /// <summary>
    ///     Determines if property is nullable.
    /// </summary>
    private static bool IsNullableProperty(string propertyName)
    {
        // Check for direct access to nullable properties
        return propertyName is "PipelineErrorHandler" or "DeadLetterSink" or "StateManager" or "StatefulRegistry";
    }

    /// <summary>
    ///     Determines if null-conditional operator is used.
    /// </summary>
    private static bool IsNullConditionalAccess(ExpressionSyntax expression)
    {
        // Walk up syntax tree to check for null-conditional operator
        SyntaxNode? current = expression;

        while (current != null)
        {
            if (current is ConditionalAccessExpressionSyntax)
                return true;

            current = current.Parent as ExpressionSyntax;
        }

        return false;
    }

    /// <summary>
    ///     Determines if invocation is within a null check context.
    /// </summary>
    private static bool IsWithinNullCheck(SyntaxNode node)
    {
        // Walk up the syntax tree to find an enclosing if-statement and inspect its condition
        var current = node.Parent;

        while (current != null)
        {
            if (current is IfStatementSyntax ifStatement)
            {
                if (IsNullCheckCondition(ifStatement.Condition))
                    return true;
            }

            current = current.Parent;
        }

        return false;
    }

    /// <summary>
    ///     Determines if condition is a null check.
    /// </summary>
    private static bool IsNullCheckCondition(ExpressionSyntax? condition)
    {
        if (condition == null)
            return false;

        // Handle compound logical expressions (e.g., pattern && null check)
        if (condition is BinaryExpressionSyntax binaryExpression)
        {
            // Logical AND/OR: inspect both sides
            if (binaryExpression.Kind() is SyntaxKind.LogicalAndExpression or SyntaxKind.LogicalOrExpression)
                return IsNullCheckCondition(binaryExpression.Left) || IsNullCheckCondition(binaryExpression.Right);

            // Equality / inequality null checks
            if (binaryExpression.Kind() is SyntaxKind.NotEqualsExpression or SyntaxKind.EqualsExpression)
            {
                // Check if one side is a null literal
                return IsNullLiteralExpression(binaryExpression.Left) || IsNullLiteralExpression(binaryExpression.Right);
            }
        }

        // Check for patterns like: context.Property is not null OR declaration/pattern matches
        if (condition is IsPatternExpressionSyntax isPattern)
        {
            // ConstantPatternSyntax with null literal => null check
            if (isPattern.Pattern is ConstantPatternSyntax constantPattern &&
                IsNullLiteralExpression(constantPattern.Expression))
                return true;

            // Declaration pattern or recursive pattern (e.g., 'is { }' or 'is var h') implies non-null
            if (isPattern.Pattern is DeclarationPatternSyntax || isPattern.Pattern is RecursivePatternSyntax)
                return true;
        }

        return false;
    }

    /// <summary>
    ///     Determines if expression is a null literal.
    /// </summary>
    private static bool IsNullLiteralExpression(ExpressionSyntax expression)
    {
        return expression is LiteralExpressionSyntax literal &&
               literal.Kind() == SyntaxKind.NullLiteralExpression;
    }
}
