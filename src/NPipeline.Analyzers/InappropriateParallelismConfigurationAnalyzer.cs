using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects inappropriate parallelism configurations that can cause resource contention,
///     thread pool starvation, or suboptimal resource utilization in NPipeline pipelines.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class InappropriateParallelismConfigurationAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for inappropriate parallelism configuration.
    /// </summary>
    public const string DiagnosticId = "NP9502";

    // Analyzer heuristics cannot access Environment.ProcessorCount; use a conservative default instead.
    // Use 4 as a conservative default that matches test expectations in CI environments.
    private const int DefaultProcessorCount = 4;

    private static readonly DiagnosticDescriptor Rule = new(
        DiagnosticId,
        "Inappropriate parallelism configuration detected",
        "Parallelism configuration '{0}' is inappropriate for {1} workload: {2}",
        "Performance",
        DiagnosticSeverity.Warning,
        true,
        "Inappropriate parallelism configurations can cause resource contention, thread pool starvation, "
        + "or suboptimal resource utilization. For CPU-bound workloads, use parallelism close to processor count. "
        + "For I/O-bound workloads, use moderate parallelism. Avoid PreserveOrdering with high parallelism. "
        + "https://npipeline.dev/docs/performance/parallelism-configuration.");

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule];

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze object creation expressions for ParallelExecutionStrategy and ParallelOptions
        context.RegisterSyntaxNodeAction(AnalyzeObjectCreation, SyntaxKind.ObjectCreationExpression);

        // Register to analyze invocation expressions for WithParallelism methods
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

        // Analyze ParallelExecutionStrategy constructor calls
        if (typeName == "ParallelExecutionStrategy")
            AnalyzeParallelExecutionStrategy(objectCreation, semanticModel, context);

        // Analyze ParallelOptions constructor calls
        else if (typeName == "ParallelOptions")
            AnalyzeParallelOptions(objectCreation, semanticModel, context);
    }

    private static void AnalyzeInvocationExpression(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not InvocationExpressionSyntax invocation)
            return;

        var semanticModel = context.SemanticModel;

        // Check if this is a WithParallelism method call
        if (!IsWithParallelismMethod(invocation))
            return;

        // Get the parallelism value
        if (!TryGetParallelismValue(invocation, semanticModel, out var parallelismValue))
            return;

        // Determine workload type from the transform type argument
        var workloadType = DetermineWorkloadTypeFromInvocation(invocation);
        var recommendation = GetRecommendation(parallelismValue, workloadType);

        if (!string.IsNullOrEmpty(recommendation))
        {
            var diagnostic = Diagnostic.Create(
                Rule,
                invocation.GetLocation(),
                $"WithParallelism({parallelismValue})",
                $"{workloadType.ToString().ToLowerInvariant()}",
                recommendation);

            context.ReportDiagnostic(diagnostic);
        }
    }

    private static void AnalyzeParallelExecutionStrategy(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        SyntaxNodeAnalysisContext context)
    {
        // Try to get the degreeOfParallelism parameter value
        if (!TryGetDegreeOfParallelism(objectCreation, semanticModel, out var degreeOfParallelism))
            return;

        // Determine workload type from the containing class
        var workloadType = DetermineWorkloadType(objectCreation);
        var recommendation = GetRecommendation(degreeOfParallelism, workloadType);

        if (!string.IsNullOrEmpty(recommendation))
        {
            var diagnostic = Diagnostic.Create(
                Rule,
                objectCreation.GetLocation(),
                $"ParallelExecutionStrategy(degreeOfParallelism: {degreeOfParallelism})",
                $"{workloadType.ToString().ToLowerInvariant()}",
                recommendation);

            context.ReportDiagnostic(diagnostic);
        }
    }

    private static void AnalyzeParallelOptions(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        SyntaxNodeAnalysisContext context)
    {
        // Try to get the MaxDegreeOfParallelism and PreserveOrdering values
        if (!TryGetMaxDegreeOfParallelism(objectCreation, semanticModel, out var maxDegreeOfParallelism))
            return;

        var preserveOrdering = TryGetPreserveOrdering(objectCreation, out var preserveOrderingValue) && preserveOrderingValue;

        // Check for PreserveOrdering with high parallelism
        if (preserveOrdering && maxDegreeOfParallelism > 4)
        {
            var diagnostic = Diagnostic.Create(
                Rule,
                objectCreation.GetLocation(),
                $"ParallelOptions(MaxDegreeOfParallelism: {maxDegreeOfParallelism}, PreserveOrdering: true)",
                "high-parallelism",
                "PreserveOrdering with high parallelism (> 4) can cause significant performance overhead. Consider disabling PreserveOrdering or reducing parallelism.");

            context.ReportDiagnostic(diagnostic);
        }
    }

    private static bool TryGetDegreeOfParallelism(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        out int degreeOfParallelism)
    {
        degreeOfParallelism = 0;

        if (objectCreation.ArgumentList == null)
            return false;

        // Look for degreeOfParallelism parameter (could be positional or named)
        foreach (var argument in objectCreation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text is "degreeOfParallelism" or "maxDegreeOfParallelism")
                return TryExtractIntValue(argument.Expression, semanticModel, out degreeOfParallelism);

            // If it's the first argument and not named, assume it's degreeOfParallelism
            if (argument.NameColon == null && objectCreation.ArgumentList.Arguments.IndexOf(argument) == 0)
                return TryExtractIntValue(argument.Expression, semanticModel, out degreeOfParallelism);
        }

        return false;
    }

    private static bool TryGetMaxDegreeOfParallelism(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        out int maxDegreeOfParallelism)
    {
        maxDegreeOfParallelism = 0;

        if (objectCreation.ArgumentList == null)
            return false;

        foreach (var argument in objectCreation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text == "maxDegreeOfParallelism")
                return TryExtractIntValue(argument.Expression, semanticModel, out maxDegreeOfParallelism);

            // If it's the first argument and not named, assume it's maxDegreeOfParallelism
            if (argument.NameColon == null && objectCreation.ArgumentList.Arguments.IndexOf(argument) == 0)
                return TryExtractIntValue(argument.Expression, semanticModel, out maxDegreeOfParallelism);
        }

        return false;
    }

    private static bool TryGetPreserveOrdering(
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

        // Try to evaluate simple expressions like Environment.ProcessorCount
        if (expression is MemberAccessExpressionSyntax memberAccess)
        {
            var symbol = semanticModel.GetSymbolInfo(memberAccess).Symbol;

            if (symbol?.Name == "ProcessorCount" &&
                symbol.ContainingType?.Name == "Environment" &&
                symbol.ContainingType?.ContainingNamespace?.Name == "System")
            {
                value = DefaultProcessorCount;
                return true;
            }
        }

        // Try to evaluate simple arithmetic expressions
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

    private static bool IsWithParallelismMethod(InvocationExpressionSyntax invocation)
    {
        if (invocation.Expression is not MemberAccessExpressionSyntax memberAccess)
            return false;

        var methodName = memberAccess.Name.Identifier.Text;

        return methodName.Contains("Parallelism", StringComparison.OrdinalIgnoreCase) ||
               methodName.Contains("Parallel", StringComparison.OrdinalIgnoreCase);
    }

    private static bool TryGetParallelismValue(InvocationExpressionSyntax invocation, SemanticModel semanticModel, out int parallelismValue)
    {
        parallelismValue = 0;

        if (invocation.ArgumentList == null || invocation.ArgumentList.Arguments.Count == 0)
            return false;

        // For WithBlockingParallelism(builder, 16), the parallelism value is the second argument
        // For WithParallelism(16), the parallelism value is the first argument
        // Check if the first argument looks like a builder parameter (identifier)
        var firstArgument = invocation.ArgumentList.Arguments[0];

        if (invocation.ArgumentList.Arguments.Count > 1 &&
            firstArgument.Expression is IdentifierNameSyntax)
        {
            // Assume the second argument is the parallelism value
            var secondArgument = invocation.ArgumentList.Arguments[1];
            return TryExtractIntValue(secondArgument.Expression, semanticModel, out parallelismValue);
        }

        // Assume the first argument is the parallelism value
        return TryExtractIntValue(firstArgument.Expression, semanticModel, out parallelismValue);
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

    private static WorkloadType DetermineWorkloadTypeFromInvocation(InvocationExpressionSyntax invocation)
    {
        // Try to get the transform type from the AddTransform method call
        // This is typically the previous method call in the chain
        if (invocation.Expression is MemberAccessExpressionSyntax memberAccess &&
            memberAccess.Expression is InvocationExpressionSyntax previousInvocation)
        {
            // Check if this is an AddTransform call
            if (previousInvocation.Expression is MemberAccessExpressionSyntax previousMemberAccess)
            {
                // The Name of the member access may be a GenericNameSyntax when generics are used
                if (previousMemberAccess.Name is GenericNameSyntax genericName &&
                    genericName.Identifier.Text == "AddTransform" &&
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
            currentMemberAccess.Expression is MemberAccessExpressionSyntax parentMemberAccess)
        {
            if (parentMemberAccess.Name is GenericNameSyntax parentGenericName &&
                parentGenericName.Identifier.Text == "AddTransform" &&
                parentGenericName.TypeArgumentList?.Arguments.Count > 0)
            {
                var transformType = parentGenericName.TypeArgumentList.Arguments[0].ToString();
                return DetermineWorkloadTypeFromTypeName(transformType);
            }
        }

        // Additional approach: look for AddTransform in a more complex chain
        // Handle cases like: builder.AddTransform<TestTransform, int, string>("test").WithBlockingParallelism(builder, 16);
        var currentExpression = invocation.Expression;

        while (currentExpression is MemberAccessExpressionSyntax memberAccessExpr)
        {
            if (memberAccessExpr.Expression is InvocationExpressionSyntax invocationExpr &&
                invocationExpr.Expression is MemberAccessExpressionSyntax innerMemberAccess)
            {
                if (innerMemberAccess.Name is GenericNameSyntax innerGenericName &&
                    innerGenericName.Identifier.Text == "AddTransform" &&
                    innerGenericName.TypeArgumentList?.Arguments.Count > 0)
                {
                    var transformType = innerGenericName.TypeArgumentList.Arguments[0].ToString();
                    return DetermineWorkloadTypeFromTypeName(transformType);
                }
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

        if (ioIndicators.Any(indicator => typeName.Contains(indicator, StringComparison.OrdinalIgnoreCase)))
            return WorkloadType.IoBound;

        // Check for CPU-bound indicators
        var cpuIndicators = new[] { "Cpu", "Compute", "Process", "Transform", "Calculate", "Math" };

        if (cpuIndicators.Any(indicator => typeName.Contains(indicator, StringComparison.OrdinalIgnoreCase)))
            return WorkloadType.CpuBound;

        // Default to CPU-bound for transforms
        return typeName.Contains("Transform", StringComparison.OrdinalIgnoreCase)
            ? WorkloadType.CpuBound
            : WorkloadType.Unknown;
    }

    private static string GetRecommendation(int parallelism, WorkloadType workloadType)
    {
        var processorCount = DefaultProcessorCount;

        return workloadType switch
        {
            WorkloadType.CpuBound when parallelism > processorCount =>
                $"For CPU-bound workloads, parallelism should not exceed processor count ({processorCount}). Consider using {processorCount} or fewer.",
            WorkloadType.CpuBound when parallelism == 1 =>
                $"For CPU-bound workloads, single-threaded execution may underutilize available processors. Consider using {processorCount} for better CPU utilization.",
            WorkloadType.IoBound when parallelism >= processorCount * 2 =>
                $"For I/O-bound workloads, excessive parallelism (>= {processorCount * 2}) can cause thread pool starvation. Consider using {processorCount} to {processorCount * 2 - 1}.",
            WorkloadType.Unknown when parallelism > processorCount =>
                $"Excessive parallelism detected. Consider using processor count ({processorCount}) for CPU-bound workloads or {processorCount} to {processorCount * 2} for I/O-bound workloads.",
            _ => string.Empty,
        };
    }

    private enum WorkloadType
    {
        Unknown,
        CpuBound,
        IoBound,
    }
}
