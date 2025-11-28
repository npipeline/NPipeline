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
///     Code fix provider that suggests optimal timeout configurations.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(TimeoutConfigurationCodeFixProvider))]
[Shared]
public sealed class TimeoutConfigurationCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [TimeoutConfigurationAnalyzer.TimeoutConfigurationId];

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

        if (typeSymbol.Name == "ResilientExecutionStrategy")
            await RegisterResilientExecutionStrategyFixes(context, objectCreation, semanticModel, diagnostic);
        else if (typeSymbol.Name == "PipelineRetryOptions")
            await RegisterPipelineRetryOptionsFixes(context, objectCreation, semanticModel, diagnostic);
    }

    private static Task RegisterResilientExecutionStrategyFixes(
        CodeFixContext context,
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        Diagnostic diagnostic)
    {
        // Try to extract current timeout value
        if (!TryExtractCurrentTimeout(objectCreation, semanticModel, out var currentTimeout))
            return Task.CompletedTask;

        // Determine workload type
        var workloadType = DetermineWorkloadType(objectCreation);

        // Generate fixes based on workload type
        var fixes = GenerateResilientExecutionStrategyFixes(workloadType, currentTimeout);

        foreach (var (title, newTimeout) in fixes)
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    title,
                    cancellationToken => UpdateResilientExecutionStrategyTimeoutAsync(context.Document, objectCreation, newTimeout, cancellationToken),
                    nameof(TimeoutConfigurationCodeFixProvider)),
                diagnostic);
        }

        return Task.CompletedTask;
    }

    private static Task RegisterPipelineRetryOptionsFixes(
        CodeFixContext context,
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        Diagnostic diagnostic)
    {
        // Try to extract current timeout value
        if (!TryExtractCurrentTimeout(objectCreation, semanticModel, out var currentTimeout))
            return Task.CompletedTask;

        // Generate fixes for retry timeout issues
        var fixes = GeneratePipelineRetryOptionsFixes(currentTimeout);

        foreach (var (title, newTimeout) in fixes)
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    title,
                    cancellationToken => UpdatePipelineRetryOptionsTimeoutAsync(context.Document, objectCreation, newTimeout, cancellationToken),
                    nameof(TimeoutConfigurationCodeFixProvider)),
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

        // Try to extract current timeout value
        if (!TryExtractTimeoutFromInvocation(invocation, semanticModel, out var currentTimeout))
            return;

        // Determine workload type
        var workloadType = DetermineWorkloadType(invocation);

        // Generate fixes based on workload type
        var fixes = GenerateInvocationFixes(workloadType, currentTimeout);

        foreach (var (title, newTimeout) in fixes)
        {
            context.RegisterCodeFix(
                CodeAction.Create(
                    title,
                    cancellationToken => UpdateInvocationTimeoutAsync(context.Document, invocation, newTimeout, cancellationToken),
                    nameof(TimeoutConfigurationCodeFixProvider)),
                diagnostic);
        }
    }

    private static async Task<Document> UpdateResilientExecutionStrategyTimeoutAsync(
        Document document,
        ObjectCreationExpressionSyntax objectCreation,
        TimeSpan newTimeout,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create new timeout argument
        var newArgument = SyntaxFactory.Argument(
            SyntaxFactory.NameColon("timeout"),
            SyntaxFactory.Token(SyntaxKind.ColonToken),
            CreateTimeSpanExpression(newTimeout));

        // Replace or add timeout argument
        ObjectCreationExpressionSyntax newObjectCreation;

        if (objectCreation.ArgumentList?.Arguments.Count > 0)
        {
            // Replace first argument or add timeout if none exists
            var hasTimeoutArg = objectCreation.ArgumentList.Arguments.Any(arg =>
                arg.NameColon?.Name.Identifier.Text is "timeout" or "Timeout");

            if (hasTimeoutArg)
            {
                var timeoutArg = objectCreation.ArgumentList.Arguments.First(arg =>
                    arg.NameColon?.Name.Identifier.Text is "timeout" or "Timeout");

                newObjectCreation = objectCreation.WithArgumentList(
                    objectCreation.ArgumentList.WithArguments(
                        objectCreation.ArgumentList.Arguments.Replace(timeoutArg, newArgument)));
            }
            else
            {
                newObjectCreation = objectCreation.WithArgumentList(
                    objectCreation.ArgumentList.WithArguments(
                        objectCreation.ArgumentList.Arguments.Add(newArgument)));
            }
        }
        else
        {
            newObjectCreation = objectCreation.WithArgumentList(
                SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(newArgument)));
        }

        var newRoot = root.ReplaceNode(objectCreation, newObjectCreation);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> UpdatePipelineRetryOptionsTimeoutAsync(
        Document document,
        ObjectCreationExpressionSyntax objectCreation,
        TimeSpan newTimeout,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create new timeout argument
        var newArgument = SyntaxFactory.Argument(
            SyntaxFactory.NameColon("timeout"),
            SyntaxFactory.Token(SyntaxKind.ColonToken),
            CreateTimeSpanExpression(newTimeout));

        // Replace or add timeout argument
        ObjectCreationExpressionSyntax newObjectCreation;

        if (objectCreation.ArgumentList?.Arguments.Count > 0)
        {
            // Replace first argument or add timeout if none exists
            var hasTimeoutArg = objectCreation.ArgumentList.Arguments.Any(arg =>
                arg.NameColon?.Name.Identifier.Text is "timeout" or "Timeout");

            if (hasTimeoutArg)
            {
                var timeoutArg = objectCreation.ArgumentList.Arguments.First(arg =>
                    arg.NameColon?.Name.Identifier.Text is "timeout" or "Timeout");

                newObjectCreation = objectCreation.WithArgumentList(
                    objectCreation.ArgumentList.WithArguments(
                        objectCreation.ArgumentList.Arguments.Replace(timeoutArg, newArgument)));
            }
            else
            {
                newObjectCreation = objectCreation.WithArgumentList(
                    objectCreation.ArgumentList.WithArguments(
                        objectCreation.ArgumentList.Arguments.Add(newArgument)));
            }
        }
        else
        {
            newObjectCreation = objectCreation.WithArgumentList(
                SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(newArgument)));
        }

        var newRoot = root.ReplaceNode(objectCreation, newObjectCreation);
        return document.WithSyntaxRoot(newRoot);
    }

    private static async Task<Document> UpdateInvocationTimeoutAsync(
        Document document,
        InvocationExpressionSyntax invocation,
        TimeSpan newTimeout,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Create new timeout argument
        var newArgument = SyntaxFactory.Argument(CreateTimeSpanExpression(newTimeout));

        // Replace first argument
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

    private static ExpressionSyntax CreateTimeSpanExpression(TimeSpan timeout)
    {
        if (timeout == TimeSpan.Zero)
        {
            return SyntaxFactory.MemberAccessExpression(
                SyntaxKind.SimpleMemberAccessExpression,
                SyntaxFactory.IdentifierName("TimeSpan"),
                SyntaxFactory.IdentifierName("Zero"));
        }

        if (timeout.TotalMilliseconds < 1000)
        {
            return SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName("TimeSpan"),
                    SyntaxFactory.IdentifierName("FromMilliseconds")),
                SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(
                    SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(
                        SyntaxKind.NumericLiteralExpression, SyntaxFactory.Literal(timeout.TotalMilliseconds))))));
        }

        if (timeout.TotalSeconds < 60)
        {
            return SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName("TimeSpan"),
                    SyntaxFactory.IdentifierName("FromSeconds")),
                SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(
                    SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(
                        SyntaxKind.NumericLiteralExpression, SyntaxFactory.Literal(timeout.TotalSeconds))))));
        }

        if (timeout.TotalMinutes < 60)
        {
            return SyntaxFactory.InvocationExpression(
                SyntaxFactory.MemberAccessExpression(
                    SyntaxKind.SimpleMemberAccessExpression,
                    SyntaxFactory.IdentifierName("TimeSpan"),
                    SyntaxFactory.IdentifierName("FromMinutes")),
                SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(
                    SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(
                        SyntaxKind.NumericLiteralExpression, SyntaxFactory.Literal(timeout.TotalMinutes))))));
        }

        return SyntaxFactory.InvocationExpression(
            SyntaxFactory.MemberAccessExpression(
                SyntaxKind.SimpleMemberAccessExpression,
                SyntaxFactory.IdentifierName("TimeSpan"),
                SyntaxFactory.IdentifierName("FromHours")),
            SyntaxFactory.ArgumentList(SyntaxFactory.SingletonSeparatedList(
                SyntaxFactory.Argument(SyntaxFactory.LiteralExpression(
                    SyntaxKind.NumericLiteralExpression, SyntaxFactory.Literal(timeout.TotalHours))))));
    }

    private static bool TryExtractCurrentTimeout(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel,
        out TimeSpan? currentTimeout)
    {
        currentTimeout = null;

        if (objectCreation.ArgumentList == null)
            return false;

        foreach (var argument in objectCreation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text is "timeout" or "Timeout")
                return TryExtractTimeSpanValue(argument.Expression, semanticModel, out currentTimeout);

            // If it's the first argument and not named, assume it's timeout
            if (argument.NameColon == null && objectCreation.ArgumentList.Arguments.IndexOf(argument) == 0)
                return TryExtractTimeSpanValue(argument.Expression, semanticModel, out currentTimeout);
        }

        return false;
    }

    private static bool TryExtractTimeoutFromInvocation(
        InvocationExpressionSyntax invocation,
        SemanticModel semanticModel,
        out TimeSpan? currentTimeout)
    {
        currentTimeout = null;

        if (invocation.ArgumentList == null || invocation.ArgumentList.Arguments.Count == 0)
            return false;

        // Look for timeout parameter (could be positional or named)
        foreach (var argument in invocation.ArgumentList.Arguments)
        {
            if (argument.NameColon?.Name.Identifier.Text is "timeout" or "Timeout")
                return TryExtractTimeSpanValue(argument.Expression, semanticModel, out currentTimeout);

            // If it's the first argument and not named, assume it's timeout
            if (argument.NameColon == null && invocation.ArgumentList.Arguments.IndexOf(argument) == 0)
                return TryExtractTimeSpanValue(argument.Expression, semanticModel, out currentTimeout);
        }

        return false;
    }

    private static bool TryExtractTimeSpanValue(ExpressionSyntax expression, SemanticModel semanticModel, out TimeSpan? timeoutValue)
    {
        timeoutValue = null;

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

                        if (TryExtractDoubleValue(firstArg.Expression, out var value))
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

    private static List<(string title, TimeSpan timeout)> GenerateResilientExecutionStrategyFixes(
        WorkloadType workloadType,
        TimeSpan? currentTimeout)
    {
        var fixes = new List<(string, TimeSpan)>();

        switch (workloadType)
        {
            case WorkloadType.IoBound:
                if (currentTimeout.HasValue && currentTimeout.Value < TimeSpan.FromMilliseconds(500))
                {
                    fixes.Add(("Use 500ms for I/O operations", TimeSpan.FromMilliseconds(500)));
                    fixes.Add(("Use 1 second for I/O operations", TimeSpan.FromSeconds(1)));
                    fixes.Add(("Use 5 seconds for I/O operations", TimeSpan.FromSeconds(5)));
                }

                break;
            case WorkloadType.CpuBound:
                if (currentTimeout.HasValue && currentTimeout.Value > TimeSpan.FromMinutes(5))
                {
                    fixes.Add(("Use 1 minute for CPU operations", TimeSpan.FromMinutes(1)));
                    fixes.Add(("Use 2 minutes for CPU operations", TimeSpan.FromMinutes(2)));
                    fixes.Add(("Use 5 minutes for CPU operations", TimeSpan.FromMinutes(5)));
                }
                else if (currentTimeout.HasValue && currentTimeout.Value <= TimeSpan.Zero)
                {
                    fixes.Add(("Use 1 minute for CPU operations", TimeSpan.FromMinutes(1)));
                    fixes.Add(("Use 5 minutes for CPU operations", TimeSpan.FromMinutes(5)));
                }

                break;
            case WorkloadType.Unknown:
                if (currentTimeout.HasValue && currentTimeout.Value <= TimeSpan.Zero)
                {
                    fixes.Add(("Use 1 minute for operations", TimeSpan.FromMinutes(1)));
                    fixes.Add(("Use 5 minutes for operations", TimeSpan.FromMinutes(5)));
                }
                else if (currentTimeout.HasValue && currentTimeout.Value < TimeSpan.FromMilliseconds(500))
                {
                    fixes.Add(("Use 500ms for operations", TimeSpan.FromMilliseconds(500)));
                    fixes.Add(("Use 1 second for operations", TimeSpan.FromSeconds(1)));
                }
                else if (currentTimeout.HasValue && currentTimeout.Value > TimeSpan.FromMinutes(5))
                {
                    fixes.Add(("Use 1 minute for operations", TimeSpan.FromMinutes(1)));
                    fixes.Add(("Use 5 minutes for operations", TimeSpan.FromMinutes(5)));
                }

                break;
        }

        return fixes;
    }

    private static List<(string title, TimeSpan timeout)> GeneratePipelineRetryOptionsFixes(TimeSpan? currentTimeout)
    {
        var fixes = new List<(string, TimeSpan)>();

        if (currentTimeout.HasValue && currentTimeout.Value <= TimeSpan.Zero)
        {
            fixes.Add(("Use 1 minute for retry timeout", TimeSpan.FromMinutes(1)));
            fixes.Add(("Use 5 minutes for retry timeout", TimeSpan.FromMinutes(5)));
            fixes.Add(("Use 10 minutes for retry timeout", TimeSpan.FromMinutes(10)));
        }
        else if (currentTimeout.HasValue && currentTimeout.Value > TimeSpan.FromMinutes(30))
        {
            fixes.Add(("Use 5 minutes for retry timeout", TimeSpan.FromMinutes(5)));
            fixes.Add(("Use 10 minutes for retry timeout", TimeSpan.FromMinutes(10)));
            fixes.Add(("Use 30 minutes for retry timeout", TimeSpan.FromMinutes(30)));
        }

        return fixes;
    }

    private static List<(string title, TimeSpan timeout)> GenerateInvocationFixes(
        WorkloadType workloadType,
        TimeSpan? currentTimeout)
    {
        var fixes = new List<(string, TimeSpan)>();

        switch (workloadType)
        {
            case WorkloadType.IoBound:
                if (currentTimeout.HasValue && currentTimeout.Value < TimeSpan.FromMilliseconds(500))
                {
                    fixes.Add(("Use 500ms for I/O operations", TimeSpan.FromMilliseconds(500)));
                    fixes.Add(("Use 1 second for I/O operations", TimeSpan.FromSeconds(1)));
                    fixes.Add(("Use 5 seconds for I/O operations", TimeSpan.FromSeconds(5)));
                }

                break;
            case WorkloadType.CpuBound:
                if (currentTimeout.HasValue && currentTimeout.Value > TimeSpan.FromMinutes(5))
                {
                    fixes.Add(("Use 1 minute for CPU operations", TimeSpan.FromMinutes(1)));
                    fixes.Add(("Use 2 minutes for CPU operations", TimeSpan.FromMinutes(2)));
                    fixes.Add(("Use 5 minutes for CPU operations", TimeSpan.FromMinutes(5)));
                }
                else if (currentTimeout.HasValue && currentTimeout.Value <= TimeSpan.Zero)
                {
                    fixes.Add(("Use 1 minute for CPU operations", TimeSpan.FromMinutes(1)));
                    fixes.Add(("Use 5 minutes for CPU operations", TimeSpan.FromMinutes(5)));
                }

                break;
            case WorkloadType.Unknown:
                if (currentTimeout.HasValue && currentTimeout.Value <= TimeSpan.Zero)
                {
                    fixes.Add(("Use 1 minute for operations", TimeSpan.FromMinutes(1)));
                    fixes.Add(("Use 5 minutes for operations", TimeSpan.FromMinutes(5)));
                }
                else if (currentTimeout.HasValue && currentTimeout.Value < TimeSpan.FromMilliseconds(500))
                {
                    fixes.Add(("Use 500ms for operations", TimeSpan.FromMilliseconds(500)));
                    fixes.Add(("Use 1 second for operations", TimeSpan.FromSeconds(1)));
                }
                else if (currentTimeout.HasValue && currentTimeout.Value > TimeSpan.FromMinutes(5))
                {
                    fixes.Add(("Use 1 minute for operations", TimeSpan.FromMinutes(1)));
                    fixes.Add(("Use 5 minutes for operations", TimeSpan.FromMinutes(5)));
                }

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
