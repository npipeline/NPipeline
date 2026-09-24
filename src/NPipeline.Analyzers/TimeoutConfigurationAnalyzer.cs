using System.Collections.Immutable;
using System.Globalization;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Detects circuit breaker timings that cannot work, in <c>CircuitBreakerOptions</c> initializers
///     (<c>new CircuitBreakerOptions { ... }</c> or <c>options with { ... }</c>):
///     <list type="bullet">
///         <item><c>Window</c>, <c>OpenDuration</c>, or <c>MaxPause</c> of zero or less, which fails validation when the pipeline is built;</item>
///         <item>
///             <c>WhenOpen = Pause</c> with a <c>MaxPause</c> shorter than <c>OpenDuration</c>, so every paused attempt
///             gives up before the breaker lets a probe through.
///         </item>
///     </list>
///     Only constant values are checked.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class TimeoutConfigurationAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for timeout configuration issues.
    /// </summary>
    public const string TimeoutConfigurationId = "NP9005";

    private const string OptionsTypeName = "NPipeline.Reliability.CircuitBreakerOptions";

    // CircuitBreakerOptions defaults, used when an initializer on a fresh instance leaves a value out.
    private static readonly TimeSpan DefaultOpenDuration = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan DefaultMaxPause = TimeSpan.FromMinutes(5);

    private static readonly DiagnosticDescriptor Rule = new(
        TimeoutConfigurationId,
        "Circuit breaker timing cannot work",
        "{0}",
        "Configuration & Setup",
        DiagnosticSeverity.Warning,
        true,
        "Window, OpenDuration, and MaxPause must be positive. With WhenOpen = Pause, MaxPause must be at least "
        + "OpenDuration, or an attempt stops waiting before the breaker lets a probe through. "
        + "https://docs.npipeline.net/analyzers/configuration#np9005-timeout-configuration-issues.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterCompilationStartAction(start =>
        {
            var optionsType = start.Compilation.GetTypeByMetadataName(OptionsTypeName);

            if (optionsType is null)
                return;

            start.RegisterSyntaxNodeAction(
                nodeContext => AnalyzeInitializer(nodeContext, optionsType),
                SyntaxKind.ObjectInitializerExpression,
                SyntaxKind.WithInitializerExpression);
        });
    }

    private static void AnalyzeInitializer(SyntaxNodeAnalysisContext context, INamedTypeSymbol optionsType)
    {
        var initializer = (InitializerExpressionSyntax)context.Node;
        var semanticModel = context.SemanticModel;

        if (initializer.Parent is not ExpressionSyntax owner
            || !SymbolEqualityComparer.Default.Equals(semanticModel.GetTypeInfo(owner, context.CancellationToken).Type, optionsType))
            return;

        TimeSpan? openDuration = null;
        TimeSpan? maxPause = null;
        Location? maxPauseLocation = null;
        var pauses = false;

        foreach (var expression in initializer.Expressions)
        {
            if (expression is not AssignmentExpressionSyntax { Left: IdentifierNameSyntax target } assignment)
                continue;

            switch (target.Identifier.Text)
            {
                case "Window" or "OpenDuration" or "MaxPause":
                    if (!TryGetTimeSpan(assignment.Right, semanticModel, context.CancellationToken, out var value))
                        break;

                    if (value <= TimeSpan.Zero)
                    {
                        context.ReportDiagnostic(Diagnostic.Create(Rule, assignment.GetLocation(),
                            $"CircuitBreakerOptions.{target.Identifier.Text} is {value}, but it must be positive; building the pipeline will fail"));

                        break;
                    }

                    if (target.Identifier.Text == "OpenDuration")
                        openDuration = value;
                    else if (target.Identifier.Text == "MaxPause")
                    {
                        maxPause = value;
                        maxPauseLocation = assignment.GetLocation();
                    }

                    break;

                case "WhenOpen":
                    pauses = semanticModel.GetSymbolInfo(assignment.Right, context.CancellationToken).Symbol is IFieldSymbol { Name: "Pause" } field
                             && field.ContainingType?.Name == "BreakerOpenBehavior";

                    break;
            }
        }

        if (!pauses)
            return;

        // A `with` on an unknown instance may inherit either value, so only compare values this initializer sets, or,
        // for a new instance, the defaults it leaves in place.
        if (owner is BaseObjectCreationExpressionSyntax)
        {
            openDuration ??= DefaultOpenDuration;
            maxPause ??= DefaultMaxPause;
        }

        if (openDuration is { } open && maxPause is { } pause && pause < open)
        {
            context.ReportDiagnostic(Diagnostic.Create(Rule, maxPauseLocation ?? initializer.GetLocation(),
                $"WhenOpen = Pause waits at most MaxPause ({pause}), which is shorter than OpenDuration ({open}), so every paused attempt "
                + "fails before the breaker lets a probe through. Make MaxPause at least OpenDuration, or use WhenOpen = Fail."));
        }
    }

    private static bool TryGetTimeSpan(ExpressionSyntax expression, SemanticModel semanticModel, CancellationToken cancellationToken, out TimeSpan value)
    {
        value = default;

        switch (expression)
        {
            case PrefixUnaryExpressionSyntax { RawKind: (int)SyntaxKind.UnaryMinusExpression } negation
                when TryGetTimeSpan(negation.Operand, semanticModel, cancellationToken, out var positive):
                value = positive.Negate();
                return true;

            case MemberAccessExpressionSyntax member
                when semanticModel.GetSymbolInfo(member, cancellationToken).Symbol is IFieldSymbol { ContainingType.Name: "TimeSpan" } field:
                switch (field.Name)
                {
                    case "Zero":
                        value = TimeSpan.Zero;
                        return true;
                    case "MaxValue":
                        value = TimeSpan.MaxValue;
                        return true;
                    case "MinValue":
                        value = TimeSpan.MinValue;
                        return true;
                }

                return false;

            case InvocationExpressionSyntax { Expression: MemberAccessExpressionSyntax method } invocation
                when invocation.ArgumentList.Arguments.Count == 1
                     && semanticModel.GetSymbolInfo(method, cancellationToken).Symbol is IMethodSymbol { ContainingType.Name: "TimeSpan" } symbol:
                var constant = semanticModel.GetConstantValue(invocation.ArgumentList.Arguments[0].Expression, cancellationToken);

                if (!constant.HasValue || constant.Value is null)
                    return false;

                double amount;

                try
                {
                    amount = Convert.ToDouble(constant.Value, CultureInfo.InvariantCulture);
                }
                catch (Exception ex) when (ex is FormatException or InvalidCastException)
                {
                    return false;
                }

                TimeSpan? parsed = symbol.Name switch
                {
                    "FromMilliseconds" => TimeSpan.FromMilliseconds(amount),
                    "FromSeconds" => TimeSpan.FromSeconds(amount),
                    "FromMinutes" => TimeSpan.FromMinutes(amount),
                    "FromHours" => TimeSpan.FromHours(amount),
                    "FromDays" => TimeSpan.FromDays(amount),
                    _ => null,
                };

                if (parsed is null)
                    return false;

                value = parsed.Value;
                return true;

            default:
                return false;
        }
    }
}
