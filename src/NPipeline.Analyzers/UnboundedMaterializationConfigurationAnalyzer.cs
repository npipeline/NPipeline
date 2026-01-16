using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects when PipelineRetryOptions.MaxMaterializedItems is null or missing,
///     which causes unbounded memory growth in ResilientExecutionStrategy and silently disables restart functionality.
///     This is a critical configuration error that can lead to OutOfMemoryException in production.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class UnboundedMaterializationConfigurationAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for unbounded materialization configuration.
    /// </summary>
    public const string UnboundedMaterializationConfigurationId = "NP9002";

    private static readonly DiagnosticDescriptor Rule = new(
        UnboundedMaterializationConfigurationId,
        "Unbounded materialization configuration detected",
        "PipelineRetryOptions configuration has unbounded materialization (MaxMaterializedItems is null or missing). "
        + "This can cause unbounded memory growth and silently disable restart functionality in ResilientExecutionStrategy. "
        + "Set MaxMaterializedItems to a reasonable value (e.g., 1000) to bound memory usage and enable restart.",
        "Configuration & Setup",
        DiagnosticSeverity.Error,
        true,
        "Unbounded materialization can cause OutOfMemoryException in high-throughput scenarios. "
        + "Always set MaxMaterializedItems to bound memory usage and enable backpressure. "
        + "Recommended values: 100-10000 depending on memory constraints and item size.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule];

    /// <inheritdoc />
    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        // Register to analyze object creation expressions
        context.RegisterSyntaxNodeAction(AnalyzeObjectCreation, SyntaxKind.ObjectCreationExpression);
    }

    private static void AnalyzeObjectCreation(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not ObjectCreationExpressionSyntax objectCreation)
            return;

        var semanticModel = context.SemanticModel;
        var typeInfo = semanticModel.GetTypeInfo(objectCreation);

        // Check if this is PipelineRetryOptions
        if (!IsPipelineRetryOptions(typeInfo.Type))
            return;

        // Analyze the constructor arguments
        var analyzer = new PipelineRetryOptionsAnalyzer(objectCreation);
        analyzer.Analyze();

        // Report any diagnostics found
        foreach (var diagnostic in analyzer.Diagnostics)
        {
            context.ReportDiagnostic(diagnostic);
        }
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
    ///     Analyzes PipelineRetryOptions constructor calls for unbounded materialization issues.
    /// </summary>
    private sealed class PipelineRetryOptionsAnalyzer
    {
        private readonly List<Diagnostic> _diagnostics = [];
        private readonly ObjectCreationExpressionSyntax _objectCreation;

        public PipelineRetryOptionsAnalyzer(ObjectCreationExpressionSyntax objectCreation)
        {
            _objectCreation = objectCreation;
        }

        public IReadOnlyList<Diagnostic> Diagnostics => _diagnostics;

        public void Analyze()
        {
            var arguments = _objectCreation.ArgumentList?.Arguments ?? [];

            CheckForNullMaxMaterializedItems(arguments);
            CheckForMissingMaxMaterializedItems(arguments);
        }

        /// <summary>
        ///     Checks if MaxMaterializedItems is explicitly set to null.
        /// </summary>
        private void CheckForNullMaxMaterializedItems(SeparatedSyntaxList<ArgumentSyntax> arguments)
        {
            foreach (var argument in arguments)
            {
                if (!IsMaxMaterializedItemsArgument(argument))
                    continue;

                // Check if the argument value is null
                if (IsNullLiteral(argument.Expression))
                {
                    var diagnostic = Diagnostic.Create(
                        Rule,
                        argument.GetLocation(),
                        "MaxMaterializedItems explicitly set to null");

                    _diagnostics.Add(diagnostic);
                }
            }
        }

        /// <summary>
        ///     Checks if MaxMaterializedItems parameter is missing (defaults to null).
        /// </summary>
        private void CheckForMissingMaxMaterializedItems(SeparatedSyntaxList<ArgumentSyntax> arguments)
        {
            var hasMaxMaterializedItems = arguments.Any(IsMaxMaterializedItemsArgument);

            if (!hasMaxMaterializedItems)
            {
                var diagnostic = Diagnostic.Create(
                    Rule,
                    _objectCreation.GetLocation(),
                    "Missing MaxMaterializedItems parameter (defaults to null)");

                _diagnostics.Add(diagnostic);
            }
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
            // MaxMaterializedItems is the 2nd parameter (index 1) in PipelineRetryOptions constructor
            return GetParameterPosition(argument) == 1;
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
        ///     Determines if an expression represents a null literal.
        /// </summary>
        private static bool IsNullLiteral(ExpressionSyntax? expression)
        {
            return expression is LiteralExpressionSyntax literal &&
                   literal.IsKind(SyntaxKind.NullLiteralExpression);
        }
    }
}
