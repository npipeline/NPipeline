using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects inappropriate timeout configurations that can cause resource leaks,
///     hanging operations, or inefficient resource utilization in NPipeline pipelines.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class TimeoutConfigurationAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for timeout configuration issues.
    /// </summary>
    public const string TimeoutConfigurationId = "NP9504";

    private static readonly DiagnosticDescriptor Rule = new(
        TimeoutConfigurationId,
        "Inappropriate timeout configuration detected",
        "Timeout configuration '{0}' is inappropriate for {1} operations: {2}",
        "Configuration",
        DiagnosticSeverity.Warning,
        true,
        "Inappropriate timeout configurations can cause resource leaks, hanging operations, "
        + "or inefficient resource utilization. For I/O-bound operations, use timeouts >= 500ms. "
        + "For CPU-bound operations, use timeouts <= 5 minutes. Avoid zero or negative timeouts. "
        + "Keep retry timeouts reasonable (< 30 minutes). "
        + "https://npipeline.dev/docs/configuration/timeouts.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze object creation expressions for ResilientExecutionStrategy and PipelineRetryOptions
        context.RegisterSyntaxNodeAction(AnalyzeObjectCreation, SyntaxKind.ObjectCreationExpression);

        // Register to analyze invocation expressions for WithTimeout and WithRetryOptions methods
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

        // Analyze ResilientExecutionStrategy constructor calls
        if (typeName == "ResilientExecutionStrategy")
            AnalyzeResilientExecutionStrategy(objectCreation, semanticModel, context);

        // Analyze PipelineRetryOptions constructor calls
        else if (typeName == "PipelineRetryOptions")
            AnalyzePipelineRetryOptions(objectCreation, semanticModel, context);
    }

    private static void AnalyzeInvocationExpression(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not InvocationExpressionSyntax invocation)
            return;

        var semanticModel = context.SemanticModel;

        // Check if this is a WithTimeout or WithRetryOptions method call
        if (!IsTimeoutRelatedMethod(invocation))
            return;

        // Get the timeout value
        if (!TryGetTimeoutValue(invocation, semanticModel, out var timeoutValue))
            return;

        // Determine workload type from the transform type argument
        var workloadType = DetermineWorkloadTypeFromInvocation(invocation);
        var recommendation = GetTimeoutRecommendation(timeoutValue, workloadType);

        if (!string.IsNullOrEmpty(recommendation))
        {
            var diagnostic = Diagnostic.Create(
                Rule,
                invocation.GetLocation(),
                GetConfigurationDescription(timeoutValue),
                $"{workloadType.ToString().ToLowerInvariant()}",
                recommendation);

            context.ReportDiagnostic(diagnostic);
        }
    }

    private static void AnalyzeResilientExecutionStrategy(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        SyntaxNodeAnalysisContext context)
    {
        // Try to get the timeout parameter value
        if (!TryGetTimeoutFromObjectCreation(objectCreation, semanticModel, out var timeoutValue))
            return;

        // Determine workload type from the containing class
        var workloadType = DetermineWorkloadType(objectCreation);
        var recommendation = GetTimeoutRecommendation(timeoutValue, workloadType);

        if (!string.IsNullOrEmpty(recommendation))
        {
            var diagnostic = Diagnostic.Create(
                Rule,
                objectCreation.GetLocation(),
                $"ResilientExecutionStrategy(timeout: {FormatTimeout(timeoutValue ?? TimeSpan.Zero)})",
                $"{workloadType.ToString().ToLowerInvariant()}",
                recommendation);

            context.ReportDiagnostic(diagnostic);
        }
    }

    private static void AnalyzePipelineRetryOptions(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        SyntaxNodeAnalysisContext context)
    {
        // Try to get the timeout parameter value
        if (!TryGetTimeoutFromObjectCreation(objectCreation, semanticModel, out var timeoutValue))
            return;

        // For retry options, we have different timeout thresholds
        var recommendation = GetRetryTimeoutRecommendation(timeoutValue);

        if (!string.IsNullOrEmpty(recommendation))
        {
            var diagnostic = Diagnostic.Create(
                Rule,
                objectCreation.GetLocation(),
                $"PipelineRetryOptions(timeout: {FormatTimeout(timeoutValue ?? TimeSpan.Zero)})",
                "retry",
                recommendation);

            context.ReportDiagnostic(diagnostic);
        }
    }

    private static bool IsTimeoutRelatedMethod(InvocationExpressionSyntax invocation)
    {
        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return false;

        var methodName = memberAccess.Name.Identifier.Text;

        return methodName.IndexOf("Timeout", StringComparison.OrdinalIgnoreCase) >= 0 ||
               methodName.IndexOf("Retry", StringComparison.OrdinalIgnoreCase) >= 0;
    }

    private static bool TryGetTimeoutValue(InvocationExpressionSyntax invocation, SemanticModel semanticModel, out TimeSpan? timeoutValue)
    {
        timeoutValue = null;

        if (invocation.ArgumentList == null || invocation.ArgumentList.Arguments.Count == 0)
            return false;

        // Look for timeout parameter (could be positional or named)
        foreach (var argument in invocation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text is "timeout" or "Timeout")
                return TryExtractTimeSpanValue(argument.Expression, semanticModel, out timeoutValue);

            // If it's the first argument and not named, assume it's timeout
            if (argument.NameColon == null && invocation.ArgumentList.Arguments.IndexOf(argument) == 0)
                return TryExtractTimeSpanValue(argument.Expression, semanticModel, out timeoutValue);
        }

        return false;
    }

    private static bool TryGetTimeoutFromObjectCreation(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        out TimeSpan? timeoutValue)
    {
        timeoutValue = null;

        if (objectCreation.ArgumentList == null)
            return false;

        // Look for timeout parameter (could be positional or named)
        foreach (var argument in objectCreation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text is "timeout" or "Timeout")
                return TryExtractTimeSpanValue(argument.Expression, semanticModel, out timeoutValue);

            // If it's the first argument and not named, assume it's timeout
            if (argument.NameColon == null && objectCreation.ArgumentList.Arguments.IndexOf(argument) == 0)
                return TryExtractTimeSpanValue(argument.Expression, semanticModel, out timeoutValue);
        }

        return false;
    }

    private static bool TryExtractTimeSpanValue(ExpressionSyntax expression, SemanticModel semanticModel, out TimeSpan? timeoutValue)
    {
        timeoutValue = null;

        if (expression == null)
            return false;

        var constant = semanticModel.GetConstantValue(expression);

        if (constant.HasValue)
        {
            switch (constant.Value)
            {
                case TimeSpan ts:
                    timeoutValue = ts;
                    return true;
                case double milliseconds:
                    timeoutValue = TimeSpan.FromMilliseconds(milliseconds);
                    return true;
                case float millisecondsFloat:
                    timeoutValue = TimeSpan.FromMilliseconds(millisecondsFloat);
                    return true;
                case int millisecondsInt:
                    timeoutValue = TimeSpan.FromMilliseconds(millisecondsInt);
                    return true;
                case long millisecondsLong:
                    timeoutValue = TimeSpan.FromMilliseconds(millisecondsLong);
                    return true;
            }
        }

        // Handle TimeSpan.Zero, TimeSpan.MaxValue, etc.
        if (expression is MemberAccessExpressionSyntax memberAccess)
        {
            var symbol = semanticModel.GetSymbolInfo(memberAccess).Symbol;

            if (symbol?.ContainingType?.Name == "TimeSpan")
            {
                if (symbol.Name == "Zero")
                {
                    timeoutValue = TimeSpan.Zero;
                    return true;
                }

                if (symbol.Name == "MaxValue")
                {
                    timeoutValue = TimeSpan.MaxValue;
                    return true;
                }
            }
        }

        // Handle TimeSpan.FromXxx methods
        if (expression is InvocationExpressionSyntax invocation)
        {
            if (invocation.Expression is MemberAccessExpressionSyntax timeSpanMethod)
            {
                var methodName = timeSpanMethod.Name?.Identifier.Text;
                var symbol = semanticModel.GetSymbolInfo(timeSpanMethod).Symbol;

                if (symbol?.ContainingType?.Name == "TimeSpan")
                {
                    if (invocation.ArgumentList?.Arguments.Count > 0)
                    {
                        var firstArg = invocation.ArgumentList.Arguments[0];

                        if (TryExtractDoubleValue(firstArg.Expression, semanticModel, out var value))
                        {
                            timeoutValue = methodName switch
                            {
                                "FromMilliseconds" => TimeSpan.FromMilliseconds(value),
                                "FromSeconds" => TimeSpan.FromSeconds(value),
                                "FromMinutes" => TimeSpan.FromMinutes(value),
                                "FromHours" => TimeSpan.FromHours(value),
                                "FromDays" => TimeSpan.FromDays(value),
                                _ => null,
                            };

                            return timeoutValue.HasValue;
                        }
                    }
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

        if (expression is LiteralExpressionSyntax literal && double.TryParse(literal.Token.ValueText, out var parsedValue))
        {
            value = parsedValue;
            return true;
        }

        var constantValue = semanticModel.GetConstantValue(expression);

        if (constantValue.HasValue)
        {
            switch (constantValue.Value)
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
                case short s:
                    value = s;
                    return true;
                case ushort us:
                    value = us;
                    return true;
                case byte b:
                    value = b;
                    return true;
                case sbyte sb:
                    value = sb;
                    return true;
            }
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

        if (ioIndicators.Any(indicator => className.IndexOf(indicator, StringComparison.OrdinalIgnoreCase) >= 0))
            return WorkloadType.IoBound;

        // Check for CPU-bound indicators
        var cpuIndicators = new[] { "Cpu", "Compute", "Process", "Transform" };

        if (cpuIndicators.Any(indicator => className.IndexOf(indicator, StringComparison.OrdinalIgnoreCase) >= 0))
            return WorkloadType.CpuBound;

        // Default to CPU-bound for transforms
        return className.IndexOf("Transform", StringComparison.OrdinalIgnoreCase) >= 0
            ? WorkloadType.CpuBound
            : WorkloadType.Unknown;
    }

    private static WorkloadType DetermineWorkloadTypeFromInvocation(InvocationExpressionSyntax invocation)
    {
        // Try to get the transform type from the AddTransform method call
        // This is typically the previous method call in the chain
        if (invocation.Expression is MemberAccessExpressionSyntax memberAccess &&
            memberAccess.Expression is InvocationExpressionSyntax previousInvocation)
        {
            // Check if this is an AddTransform call
            if (previousInvocation.Expression is MemberAccessExpressionSyntax previousMemberAccess &&
                previousMemberAccess.Name.Identifier.Text == "AddTransform")
            {
                // Get the type arguments from the AddTransform call
                if (previousMemberAccess.Expression is GenericNameSyntax genericName &&
                    genericName.TypeArgumentList?.Arguments.Count > 0)
                {
                    // The first type argument is the transform type
                    var transformType = genericName.TypeArgumentList.Arguments[0].ToString();
                    return DetermineWorkloadTypeFromTypeName(transformType);
                }
            }
        }

        // Alternative approach: look for AddTransform in the same expression chain
        if (invocation.Expression is MemberAccessExpressionSyntax currentMemberAccess &&
            currentMemberAccess.Expression is MemberAccessExpressionSyntax parentMemberAccess &&
            parentMemberAccess.Name.Identifier.Text == "AddTransform" &&
            parentMemberAccess.Expression is GenericNameSyntax parentGenericName &&
            parentGenericName.TypeArgumentList?.Arguments.Count > 0)
        {
            var transformType = parentGenericName.TypeArgumentList.Arguments[0].ToString();
            return DetermineWorkloadTypeFromTypeName(transformType);
        }

        // Additional approach: look for AddTransform in a more complex chain
        var currentExpression = invocation.Expression;

        while (currentExpression is MemberAccessExpressionSyntax memberAccessExpr)
        {
            if (memberAccessExpr.Expression is InvocationExpressionSyntax invocationExpr &&
                invocationExpr.Expression is MemberAccessExpressionSyntax innerMemberAccess &&
                innerMemberAccess.Name.Identifier.Text == "AddTransform" &&
                innerMemberAccess.Expression is GenericNameSyntax innerGenericName &&
                innerGenericName.TypeArgumentList?.Arguments.Count > 0)
            {
                var transformType = innerGenericName.TypeArgumentList.Arguments[0].ToString();
                return DetermineWorkloadTypeFromTypeName(transformType);
            }

            currentExpression = memberAccessExpr.Expression as MemberAccessExpressionSyntax;
        }

        // Fallback to determining from the containing class
        return DetermineWorkloadType(invocation);
    }

    private static WorkloadType DetermineWorkloadTypeFromTypeName(string typeName)
    {
        // Check for I/O-bound indicators
        var ioIndicators = new[] { "Io", "Database", "File", "Network", "Http", "Api", "Stream" };

        if (ioIndicators.Any(indicator => typeName.IndexOf(indicator, StringComparison.OrdinalIgnoreCase) >= 0))
            return WorkloadType.IoBound;

        // Check for CPU-bound indicators
        var cpuIndicators = new[] { "Cpu", "Compute", "Process", "Transform" };

        if (cpuIndicators.Any(indicator => typeName.IndexOf(indicator, StringComparison.OrdinalIgnoreCase) >= 0))
            return WorkloadType.CpuBound;

        // Default to CPU-bound for transforms
        return typeName.IndexOf("Transform", StringComparison.OrdinalIgnoreCase) >= 0
            ? WorkloadType.CpuBound
            : WorkloadType.Unknown;
    }

    private static string GetTimeoutRecommendation(TimeSpan? timeoutValue, WorkloadType workloadType)
    {
        if (!timeoutValue.HasValue)
            return string.Empty;

        var timeout = timeoutValue.Value;

        // Check for zero or negative timeouts
        if (timeout <= TimeSpan.Zero)
            return "Zero or negative timeouts cause immediate failures. Use a positive timeout value.";

        return workloadType switch
        {
            WorkloadType.IoBound when timeout < TimeSpan.FromMilliseconds(500) =>
                "For I/O-bound operations, timeouts should be at least 500ms to account for network/database latency.",
            WorkloadType.CpuBound when timeout < TimeSpan.FromMilliseconds(500) =>
                "For CPU-bound operations, timeouts should be at least 500ms to allow compute-intensive steps to complete.",
            WorkloadType.CpuBound when timeout > TimeSpan.FromMinutes(5) =>
                "For CPU-bound operations, timeouts should not exceed 5 minutes to prevent resource leaks.",
            WorkloadType.Unknown when timeout < TimeSpan.FromMilliseconds(500) =>
                "Timeouts shorter than 500ms may cause premature failures. Consider using at least 500ms.",
            WorkloadType.Unknown when timeout > TimeSpan.FromMinutes(5) =>
                "Very long timeouts (> 5 minutes) may cause resource leaks. Consider using shorter timeouts.",
            _ => string.Empty,
        };
    }

    private static string GetRetryTimeoutRecommendation(TimeSpan? timeoutValue)
    {
        if (!timeoutValue.HasValue)
            return string.Empty;

        var timeout = timeoutValue.Value;

        // Check for zero or negative timeouts
        if (timeout <= TimeSpan.Zero)
            return "Zero or negative retry timeouts cause immediate failures. Use a positive timeout value.";

        // Check for excessive retry timeouts
        if (timeout > TimeSpan.FromMinutes(30))
            return "Retry timeouts should not exceed 30 minutes to prevent excessive resource consumption.";

        return string.Empty;
    }

    private static string GetConfigurationDescription(TimeSpan? timeoutValue)
    {
        if (!timeoutValue.HasValue)
            return "unknown configuration";

        return $"timeout: {FormatTimeout(timeoutValue.Value)}";
    }

    private static string FormatTimeout(TimeSpan timeout)
    {
        if (timeout == TimeSpan.Zero)
            return "TimeSpan.Zero";

        if (timeout == TimeSpan.MaxValue)
            return "TimeSpan.MaxValue";

        if (timeout.TotalMilliseconds < 1000)
            return $"TimeSpan.FromMilliseconds({timeout.TotalMilliseconds})";

        if (timeout.TotalSeconds < 60)
            return $"TimeSpan.FromSeconds({timeout.TotalSeconds})";

        if (timeout.TotalMinutes < 60)
            return $"TimeSpan.FromMinutes({timeout.TotalMinutes})";

        return $"TimeSpan.FromHours({timeout.TotalHours})";
    }

    private enum WorkloadType
    {
        Unknown,
        CpuBound,
        IoBound,
    }
}
