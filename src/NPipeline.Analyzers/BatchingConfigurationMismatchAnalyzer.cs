using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects batching configurations where batch sizes and timeouts are misaligned,
///     causing either excessive latency from large batches or inefficient processing from small batches.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class BatchingConfigurationMismatchAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for batching configuration mismatch.
    /// </summary>
    public const string BatchingConfigurationMismatchId = "NP9503";

    private static readonly DiagnosticDescriptor Rule = new(
        BatchingConfigurationMismatchId,
        "Batching configuration mismatch detected",
        "Batching configuration mismatch: {0}. This can cause {1}.",
        "Performance",
        DiagnosticSeverity.Warning,
        true,
        "Batching configuration mismatches can cause performance issues. "
        + "Large batch sizes with short timeouts may never fill the batch, causing frequent processing. "
        + "Small batch sizes with long timeouts cause unnecessary latency. "
        + "Ensure batch sizes and timeouts are properly aligned for optimal performance. "
        + "https://npipeline.dev/docs/performance/batching-configuration.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze object creation expressions for BatchingOptions
        context.RegisterSyntaxNodeAction(AnalyzeObjectCreation, SyntaxKind.ObjectCreationExpression);

        // Register to analyze invocation expressions for WithBatching methods
        context.RegisterSyntaxNodeAction(AnalyzeInvocationExpression, SyntaxKind.InvocationExpression);
    }

    private static void AnalyzeObjectCreation(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not ObjectCreationExpressionSyntax objectCreation)
            return;

        var semanticModel = context.SemanticModel;
        var typeSymbol = semanticModel.GetTypeInfo(objectCreation).Type;

        if (typeSymbol == null)
            return;

        var typeName = typeSymbol.Name;

        // Analyze BatchingOptions constructor calls
        if (typeName == "BatchingOptions")
        {
            // Only analyze if not part of a WithBatching method call
            // Check if parent is an argument to WithBatching
            var parent = objectCreation.Parent;
            var isPartOfWithBatching = false;

            while (parent != null)
            {
                if (parent is ArgumentSyntax arg &&
                    arg.Parent is ArgumentListSyntax argList &&
                    argList.Parent is InvocationExpressionSyntax invocation &&
                    invocation.Expression is MemberAccessExpressionSyntax memberAccess &&
                    memberAccess.Name.Identifier.Text == "WithBatching")
                {
                    isPartOfWithBatching = true;
                    break;
                }

                parent = parent.Parent;
            }

            if (!isPartOfWithBatching)
                AnalyzeBatchingOptions(objectCreation, semanticModel, context);
        }

        // Analyze BatchingStrategy constructor calls
        else if (typeName == "BatchingStrategy")
            AnalyzeBatchingStrategy(objectCreation, semanticModel, context);
    }

    private static void AnalyzeInvocationExpression(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not InvocationExpressionSyntax invocation)
            return;

        var semanticModel = context.SemanticModel;

        // Check if this is a WithBatching method call
        if (!IsWithBatchingMethod(invocation))
            return;

        // Get the BatchingOptions argument
        if (!TryGetBatchingOptionsArgument(invocation, out var batchingOptionsArg))
            return;

        // Analyze the BatchingOptions if it's an object creation
        if (batchingOptionsArg is ObjectCreationExpressionSyntax optionsCreation)
            AnalyzeBatchingOptions(optionsCreation, semanticModel, context);
    }

    private static void AnalyzeBatchingOptions(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        SyntaxNodeAnalysisContext context)
    {
        var batchSize = 0;
        var timeoutMs = 0.0;

        // First try to extract from constructor arguments
        var hasConstructorArgs = TryExtractBatchSize(objectCreation, semanticModel, out batchSize) &&
                                 TryExtractTimeout(objectCreation, semanticModel, out timeoutMs);

        // If constructor extraction failed, try property initializers
        var hasInitializerArgs = false;

        if (!hasConstructorArgs)
        {
            hasInitializerArgs = TryExtractBatchSizeFromInitializer(objectCreation, semanticModel, out batchSize) &&
                                 TryExtractTimeoutFromInitializer(objectCreation, semanticModel, out timeoutMs);
        }

        if (hasConstructorArgs || hasInitializerArgs)
        {
            // Analyze the configuration for mismatches
            var mismatch = AnalyzeBatchingConfiguration(batchSize, timeoutMs);

            if (mismatch != null)
            {
                var diagnostic = Diagnostic.Create(
                    Rule,
                    objectCreation.GetLocation(),
                    mismatch.Description,
                    mismatch.Impact);

                context.ReportDiagnostic(diagnostic);
            }
        }
    }

    private static void AnalyzeBatchingStrategy(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        SyntaxNodeAnalysisContext context)
    {
        // Try to extract BatchSize and MaxWaitTime values
        if (!TryExtractBatchingStrategyBatchSize(objectCreation, semanticModel, out var batchSize) ||
            !TryExtractMaxWaitTime(objectCreation, semanticModel, out var maxWaitTimeMs))
            return;

        // Analyze the configuration for mismatches
        var mismatch = AnalyzeBatchingConfiguration(batchSize, maxWaitTimeMs);

        if (mismatch != null)
        {
            var diagnostic = Diagnostic.Create(
                Rule,
                objectCreation.GetLocation(),
                mismatch.Description,
                mismatch.Impact);

            context.ReportDiagnostic(diagnostic);
        }
    }

    private static BatchingMismatch? AnalyzeBatchingConfiguration(int batchSize, double timeoutMs)
    {
        // Large batch size with short timeout
        if (batchSize > 100 && timeoutMs < 1000)
        {
            return new BatchingMismatch(
                $"large batch size ({batchSize}) with short timeout ({timeoutMs}ms)",
                "frequent timeouts before batch fills, reducing throughput");
        }

        // Small batch size with long timeout
        if (batchSize < 10 && timeoutMs > 10000)
        {
            return new BatchingMismatch(
                $"small batch size ({batchSize}) with long timeout ({timeoutMs}ms)",
                "unnecessary latency waiting for batches that fill quickly");
        }

        // Medium batch size with disproportionate timeout
        if (batchSize is >= 10 and <= 100)
        {
            var expectedMinTimeout = batchSize * 10; // 10ms per item minimum
            var expectedMaxTimeout = batchSize * 100; // 100ms per item maximum

            if (timeoutMs < expectedMinTimeout)
            {
                return new BatchingMismatch(
                    $"medium batch size ({batchSize}) with too short timeout ({timeoutMs}ms)",
                    "frequent processing of partially filled batches");
            }

            if (timeoutMs > expectedMaxTimeout)
            {
                return new BatchingMismatch(
                    $"medium batch size ({batchSize}) with excessive timeout ({timeoutMs}ms)",
                    "unnecessary latency for batch completion");
            }
        }

        return null;
    }

    private static bool IsWithBatchingMethod(InvocationExpressionSyntax invocation)
    {
        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return false;

        var methodName = memberAccess.Name.Identifier.Text;
        return methodName.IndexOf("Batching", StringComparison.OrdinalIgnoreCase) >= 0;
    }

    private static bool TryGetBatchingOptionsArgument(InvocationExpressionSyntax invocation, out ExpressionSyntax batchingOptionsArg)
    {
        batchingOptionsArg = null!;

        if (invocation.ArgumentList == null || invocation.ArgumentList.Arguments.Count == 0)
            return false;

        // Look for BatchingOptions argument (could be positional or named)
        foreach (var argument in invocation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text.IndexOf("Batching", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                batchingOptionsArg = argument.Expression;
                return true;
            }

            // If it's the first argument and not named, assume it's the batching options
            if (argument.NameColon == null && invocation.ArgumentList.Arguments.IndexOf(argument) == 0)
            {
                batchingOptionsArg = argument.Expression;
                return true;
            }
        }

        return false;
    }

    private static bool TryExtractBatchSize(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        out int batchSize)
    {
        batchSize = 0;

        if (objectCreation.ArgumentList == null)
            return false;

        foreach (var argument in objectCreation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text == "BatchSize")
                return TryExtractIntValue(argument.Expression, semanticModel, out batchSize);

            // If it's the first argument and not named, assume it's BatchSize
            if (argument.NameColon == null && objectCreation.ArgumentList.Arguments.IndexOf(argument) == 0)
                return TryExtractIntValue(argument.Expression, semanticModel, out batchSize);
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
        SemanticModel semanticModel,
        out int batchSize)
    {
        batchSize = 0;

        if (objectCreation.ArgumentList == null)
            return false;

        foreach (var argument in objectCreation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text.Equals("BatchSize", StringComparison.OrdinalIgnoreCase) == true ||
                argument.NameColon?.Name.Identifier.Text.Equals("batchSize", StringComparison.OrdinalIgnoreCase) == true)
                return TryExtractIntValue(argument.Expression, semanticModel, out batchSize);

            // If it's the first argument and not named, assume it's BatchSize
            if (argument.NameColon == null && objectCreation.ArgumentList.Arguments.IndexOf(argument) == 0)
                return TryExtractIntValue(argument.Expression, semanticModel, out batchSize);
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
            if (argument.NameColon?.Name.Identifier.Text.Equals("MaxWaitTime", StringComparison.OrdinalIgnoreCase) == true ||
                argument.NameColon?.Name.Identifier.Text.Equals("maxWaitTime", StringComparison.OrdinalIgnoreCase) == true)
                return TryExtractTimeSpanValue(argument.Expression, semanticModel, out maxWaitTimeMs);

            // If it's the second argument and not named, assume it's MaxWaitTime
            if (argument.NameColon == null && objectCreation.ArgumentList.Arguments.IndexOf(argument) == 1)
                return TryExtractTimeSpanValue(argument.Expression, semanticModel, out maxWaitTimeMs);
        }

        return false;
    }

    private static bool TryExtractIntValue(ExpressionSyntax expression, SemanticModel semanticModel, out int value)
    {
        value = 0;

        if (expression == null)
            return false;

        var constant = semanticModel.GetConstantValue(expression);

        if (constant.HasValue)
        {
            switch (constant.Value)
            {
                case int i:
                    value = i;
                    return true;
                case long l when l is >= int.MinValue and <= int.MaxValue:
                    value = (int)l;
                    return true;
                case short s:
                    value = s;
                    return true;
                case byte b:
                    value = b;
                    return true;
                case sbyte sb:
                    value = sb;
                    return true;
                case ushort us:
                    value = us;
                    return true;
            }
        }

        if (expression is LiteralExpressionSyntax literal && int.TryParse(literal.Token.ValueText, out var parsedValue))
        {
            value = parsedValue;
            return true;
        }

        // Try to evaluate simple expressions
        if (expression is BinaryExpressionSyntax binary)
        {
            if (TryExtractIntValue(binary.Left, semanticModel, out var leftValue) &&
                TryExtractIntValue(binary.Right, semanticModel, out var rightValue))
            {
                value = binary.OperatorToken.Kind() switch
                {
                    SyntaxKind.AsteriskToken => leftValue * rightValue,
                    SyntaxKind.PlusToken => leftValue + rightValue,
                    SyntaxKind.MinusToken => leftValue - rightValue,
                    SyntaxKind.SlashToken => rightValue != 0
                        ? leftValue / rightValue
                        : 0,
                    _ => 0,
                };

                return true;
            }
        }

        return false;
    }

    private static bool TryExtractTimeSpanValue(ExpressionSyntax expression, SemanticModel semanticModel, out double milliseconds)
    {
        milliseconds = 0;

        if (expression == null)
            return false;

        var constantValue = semanticModel.GetConstantValue(expression);

        if (constantValue.HasValue && constantValue.Value is TimeSpan timeSpan)
        {
            milliseconds = timeSpan.TotalMilliseconds;
            return true;
        }

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
                        TryExtractDoubleValue(invocation.ArgumentList.Arguments[0].Expression, semanticModel, out var value))
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

        // Handle TimeSpan constructor with parameters
        if (expression is ObjectCreationExpressionSyntax timeSpanCreation)
        {
            var typeSymbol = semanticModel.GetTypeInfo(timeSpanCreation).Type;

            if (typeSymbol?.Name == "TimeSpan")
            {
                // Try to extract from constructor arguments (hours, minutes, seconds, milliseconds)
                if (timeSpanCreation.ArgumentList?.Arguments.Count >= 3)
                {
                    var args = timeSpanCreation.ArgumentList.Arguments;

                    // Try to get hours, minutes, seconds, milliseconds
                    var hours = args.Count > 0 && TryExtractIntValue(args[0].Expression, semanticModel, out var h)
                        ? h
                        : 0;

                    var minutes = args.Count > 1 && TryExtractIntValue(args[1].Expression, semanticModel, out var m)
                        ? m
                        : 0;

                    var seconds = args.Count > 2 && TryExtractIntValue(args[2].Expression, semanticModel, out var s)
                        ? s
                        : 0;

                    var ms = args.Count > 3 && TryExtractIntValue(args[3].Expression, semanticModel, out var millis)
                        ? millis
                        : 0;

                    milliseconds = ((hours * 60 + minutes) * 60 + seconds) * 1000 + ms;
                    return true;
                }
            }
        }

        return false;
    }

    private static bool TryExtractDoubleValue(ExpressionSyntax expression, SemanticModel semanticModel, out double value)
    {
        value = 0;

        if (expression == null)
            return false;

        var constant = semanticModel.GetConstantValue(expression);

        if (constant.HasValue)
        {
            switch (constant.Value)
            {
                case double d:
                    value = d;
                    return true;
                case float f:
                    value = f;
                    return true;
                case int i:
                    value = i;
                    return true;
                case long l:
                    value = l;
                    return true;
                case uint ui:
                    value = ui;
                    return true;
                case ulong ul:
                    value = ul;
                    return true;
            }
        }

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

    private static bool TryExtractBatchSizeFromInitializer(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        out int batchSize)
    {
        batchSize = 0;

        if (objectCreation.Initializer == null)
            return false;

        foreach (var initializer in objectCreation.Initializer.Expressions)
        {
            if (initializer is AssignmentExpressionSyntax assignment)
            {
                if (assignment.Left is IdentifierNameSyntax identifier &&
                    identifier.Identifier.Text == "BatchSize")
                    return TryExtractIntValue(assignment.Right, semanticModel, out batchSize);
            }
        }

        return false;
    }

    private static bool TryExtractTimeoutFromInitializer(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        out double timeoutMs)
    {
        timeoutMs = 0;

        if (objectCreation.Initializer == null)
            return false;

        foreach (var initializer in objectCreation.Initializer.Expressions)
        {
            if (initializer is AssignmentExpressionSyntax assignment)
            {
                if (assignment.Left is IdentifierNameSyntax identifier &&
                    identifier.Identifier.Text == "Timeout")
                    return TryExtractTimeSpanValue(assignment.Right, semanticModel, out timeoutMs);
            }
        }

        return false;
    }

    private class BatchingMismatch
    {
        public BatchingMismatch(string description, string impact)
        {
            Description = description;
            Impact = impact;
        }

        public string Description { get; }
        public string Impact { get; }
    }
}
