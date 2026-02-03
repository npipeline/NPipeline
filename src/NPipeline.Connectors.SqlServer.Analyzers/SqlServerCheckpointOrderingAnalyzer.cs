using System.Collections.Immutable;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Connectors.SqlServer.Analyzers;

/// <summary>
///     Analyzer that detects SQL Server source nodes with checkpointing enabled but missing ORDER BY clause.
///     Checkpointing requires an ORDER BY clause on a unique, monotonically increasing column to work correctly.
///     Without it, checkpointing can cause data loss or duplication.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SqlServerCheckpointOrderingAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for analyzer.
    /// </summary>
    public const string SqlServerCheckpointOrderingId = "NP9502";

    private static readonly DiagnosticDescriptor SqlServerCheckpointOrderingRule = new(
        SqlServerCheckpointOrderingId,
        "SQL Server source with checkpointing requires ORDER BY clause",
        "Checkpointing requires an ORDER BY clause on a unique, monotonically increasing column to track processed rows correctly. Without ORDER BY, checkpointing may cause data loss or duplication.",
        "Reliability",
        DiagnosticSeverity.Warning,
        true,
        "When using checkpointing with SQL Server source nodes, the SQL query must include an ORDER BY clause on a unique, monotonically increasing column (e.g., id, created_at, updated_at). This ensures consistent row ordering across checkpoint restarts. Without proper ordering, checkpointing may skip rows or process duplicates.");

    /// <inheritdoc />
    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [SqlServerCheckpointOrderingRule];

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

        // Check if this is a SqlServerSourceNode<T> instantiation
        var typeSymbol = context.SemanticModel.GetTypeInfo(objectCreation).Type;

        if (typeSymbol == null)
            return;

        // Check if the type is SqlServerSourceNode<T>
        if (!IsSqlServerSourceNode(typeSymbol))
            return;

        // Extract the SQL query from the constructor arguments
        var query = ExtractQueryFromConstructor(objectCreation, context.SemanticModel);

        if (query == null)
            return;

        // Check if checkpointing is enabled in the configuration
        var checkpointStrategy = ExtractCheckpointStrategy(objectCreation, context.SemanticModel);

        // If checkpointing is None or InMemory, no diagnostic needed
        if (checkpointStrategy == "None" || checkpointStrategy == "InMemory")
            return;

        // Parse the query to check for ORDER BY clause
        if (!HasOrderByClause(query))
        {
            var diagnostic = Diagnostic.Create(
                SqlServerCheckpointOrderingRule,
                objectCreation.GetLocation());

            context.ReportDiagnostic(diagnostic);
        }
    }

    /// <summary>
    ///     Determines if a type symbol represents SqlServerSourceNode&lt;T&gt;.
    /// </summary>
    private static bool IsSqlServerSourceNode(ITypeSymbol typeSymbol)
    {
        // Check the type name
        if (typeSymbol.Name != "SqlServerSourceNode")
            return false;

        // Check the containing namespace
        var containingNamespace = typeSymbol.ContainingNamespace?.ToString();

        if (containingNamespace == null)
            return false;

        // Check if it's from the SQL Server connectors namespace
        return containingNamespace.Contains("SqlServer") &&
               containingNamespace.Contains("Nodes");
    }

    /// <summary>
    ///     Extracts the SQL query string from a SqlServerSourceNode constructor.
    /// </summary>
    private static string? ExtractQueryFromConstructor(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel)
    {
        // The query is typically the second argument (after connectionString or connectionPool)
        var arguments = objectCreation.ArgumentList?.Arguments;

        if (arguments == null || arguments.Value.Count < 2)
            return null;

        // Try to get the second argument (index 1)
        var secondArg = arguments.Value[1];

        // Check if it's a string literal
        if (secondArg.Expression is LiteralExpressionSyntax literal &&
            literal.Kind() == SyntaxKind.StringLiteralExpression)
            return literal.Token.Value as string;

        // Check if it's an interpolated string
        if (secondArg.Expression is InterpolatedStringExpressionSyntax interpolated)
        {
            // For interpolated strings, we can't reliably analyze the query
            // Skip analysis for dynamic queries
            return null;
        }

        // Check if it's a variable or property (try to resolve constant value)
        var constantValue = semanticModel.GetConstantValue(secondArg.Expression);

        if (constantValue.HasValue && constantValue.Value is string stringValue)
            return stringValue;

        // Can't extract the query - skip analysis
        return null;
    }

    /// <summary>
    ///     Extracts the checkpoint strategy from a SqlServerSourceNode constructor.
    /// </summary>
    private static string ExtractCheckpointStrategy(
        ObjectCreationExpressionSyntax objectCreation,
        SemanticModel semanticModel)
    {
        // Look for the configuration parameter (typically named "configuration")
        var arguments = objectCreation.ArgumentList?.Arguments;

        if (arguments == null)
            return "None";

        // Try to find the configuration argument by name
        foreach (var arg in arguments)
        {
            if (arg.NameColon != null && arg.NameColon.Name.Identifier.Text == "configuration")
            {
                // Found named configuration parameter
                return GetCheckpointStrategyFromExpression(arg.Expression, semanticModel) ?? "None";
            }
        }

        // If no named parameter, try to infer by position
        // SqlServerSourceNode constructor signature:
        // (connectionString, query, mapper?, configuration?, parameters?, continueOnError?)
        // (connectionPool, query, mapper?, configuration?, parameters?, continueOnError?, connectionName?)

        // Find the checkpoint strategy argument (3rd argument)
        if (arguments != null && arguments.Value.Count >= 3)
        {
            var thirdArg = arguments.Value[2];
            var checkpointStrategy = GetCheckpointStrategyFromExpression(thirdArg.Expression, semanticModel) ?? "None";
            return checkpointStrategy;
        }

        // If no checkpoint strategy argument, look for SqlServerConfiguration in 2nd argument
        if (arguments != null && arguments.Value.Count >= 2)
        {
            var secondArg = arguments.Value[1];
            var checkpointStrategy = GetCheckpointStrategyFromExpression(secondArg.Expression, semanticModel) ?? "None";
            return checkpointStrategy;
        }

        // Default: assume no checkpointing (safer than assuming it's enabled)
        return "None";
    }

    /// <summary>
    ///     Gets the checkpoint strategy enum value from an expression.
    /// </summary>
    private static string? GetCheckpointStrategyFromExpression(
        ExpressionSyntax expression,
        SemanticModel semanticModel)
    {
        // If it's null, assume no checkpointing
        if (expression is null || expression.Kind() == SyntaxKind.NullLiteralExpression)
            return "None";

        // If it's a member access (e.g., CheckpointStrategy.Offset), extract the member name
        if (expression is MemberAccessExpressionSyntax memberAccess)
            return memberAccess.Name.Identifier.Text;

        // If it's an identifier (e.g., just "Offset" when imported)
        if (expression is IdentifierNameSyntax identifier)
            return identifier.Identifier.Text;

        // If it's an object creation (new SqlServerConfiguration()), try to analyze it
        if (expression is ObjectCreationExpressionSyntax configCreation)
        {
            // Look for CheckpointStrategy property initialization
            var initializer = configCreation.Initializer;

            if (initializer != null)
            {
                foreach (var expr in initializer.Expressions)
                {
                    if (expr is AssignmentExpressionSyntax assignment)
                    {
                        // Check if the left side is the CheckpointStrategy property
                        string? propertyName = null;

                        if (assignment.Left is IdentifierNameSyntax identifierName)
                            propertyName = identifierName.Identifier.Text;
                        else if (assignment.Left is MemberAccessExpressionSyntax propMemberAccess)
                            propertyName = propMemberAccess.Name.Identifier.Text;

                        if (propertyName == "CheckpointStrategy")
                        {
                            // Found CheckpointStrategy assignment
                            var strategyValue = GetCheckpointStrategyValue(assignment.Right, semanticModel);

                            if (strategyValue != null)
                                return strategyValue;
                        }
                    }
                }
            }

            // Check for default configuration (likely has checkpointing disabled)
            return "None"; // Default assumption - matches SqlServerConfiguration default
        }

        // Try to get constant value
        var constantValue = semanticModel.GetConstantValue(expression);

        if (constantValue.HasValue)
        {
            // For enum values, try to get the name from the constant value
            var typeInfo = semanticModel.GetTypeInfo(expression);

            if (typeInfo.Type != null && typeInfo.Type.TypeKind == TypeKind.Enum)
            {
                // Use the constant value's ToString() to get the enum member name
                var enumValue = constantValue.Value;

                if (enumValue != null)
                {
                    // Try to find the enum member with this value
                    var enumType = typeInfo.Type;

                    foreach (var member in enumType.GetMembers())
                    {
                        if (member is IFieldSymbol field)
                        {
                            var fieldConstant = field.ConstantValue;

                            if (fieldConstant != null && fieldConstant.Equals(enumValue))
                                return member.Name;
                        }
                    }
                }
            }
        }

        // Default: assume no checkpointing (safer than assuming it's enabled)
        return "None";
    }

    /// <summary>
    ///     Gets the checkpoint strategy enum value from an expression.
    /// </summary>
    private static string? GetCheckpointStrategyValue(
        ExpressionSyntax expression,
        SemanticModel semanticModel)
    {
        // Check for member access (CheckpointStrategy.Offset)
        if (expression is MemberAccessExpressionSyntax memberAccess)
        {
            // Simply return the member name - this handles CheckpointStrategy.Offset, CheckpointStrategy.KeyBased, etc.
            return memberAccess.Name.Identifier.Text;
        }

        // Check for simple identifier (Offset when using static import)
        if (expression is IdentifierNameSyntax simpleIdentifier)
        {
            var symbol = semanticModel.GetSymbolInfo(simpleIdentifier).Symbol;

            if (symbol?.ContainingType?.Name == "CheckpointStrategy")
                return simpleIdentifier.Identifier.Text;
        }

        // Try to get constant value
        var constantValue = semanticModel.GetConstantValue(expression);

        if (constantValue.HasValue)
        {
            // For enum values, try to get the name from the constant value
            var typeInfo = semanticModel.GetTypeInfo(expression);

            if (typeInfo.Type?.TypeKind == TypeKind.Enum)
            {
                // Use the constant value's ToString() to get the enum member name
                var enumValue = constantValue.Value;

                if (enumValue != null)
                    return enumValue.ToString();
            }
        }

        return null;
    }

    /// <summary>
    ///     Checks if a SQL query contains an ORDER BY clause.
    /// </summary>
    private static bool HasOrderByClause(string query)
    {
        // Normalize the query for easier parsing
        var normalizedQuery = query.Trim();

        // Use regex to find ORDER BY clause (case-insensitive)
        // This pattern looks for ORDER BY that's not inside a string literal
        var orderByPattern = @"\bORDER\s+BY\b";

        var regex = new Regex(orderByPattern, RegexOptions.IgnoreCase | RegexOptions.Multiline);
        return regex.IsMatch(normalizedQuery);
    }
}
