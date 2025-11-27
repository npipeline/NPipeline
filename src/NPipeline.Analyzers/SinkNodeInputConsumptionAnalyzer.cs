using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace NPipeline.Analyzers;

/// <summary>
///     Analyzer that detects when a SinkNode-derived class overrides ExecuteAsync but doesn't consume input parameter.
///     This helps identify potential bugs where sink nodes ignore their input data.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SinkNodeInputConsumptionAnalyzer : DiagnosticAnalyzer
{
    /// <summary>
    ///     Diagnostic ID for when SinkNode doesn't consume input parameter.
    /// </summary>
    public const string SinkNodeInputNotConsumedId = "NP9302";

    private static readonly DiagnosticDescriptor SinkNodeInputNotConsumedRule = new(
        SinkNodeInputNotConsumedId,
        "SinkNode should consume input parameter",
        "SinkNode '{0}' overrides ExecuteAsync but doesn't consume input parameter. Sink nodes should process all items from input data pipe.",
        "Correctness",
        DiagnosticSeverity.Error,
        true,
        "SinkNode implementations should consume all items from their input data pipe. Failing to consume input may result in data loss and unexpected behavior. Use await foreach to iterate through input items. https://npipeline.dev/docs/nodes/sink-nodes/best-practices.");

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
        [SinkNodeInputNotConsumedRule];

    public override void Initialize(AnalysisContext context)
    {
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();

        context.RegisterSymbolAction(AnalyzeSymbol, SymbolKind.NamedType);
    }

    private static void AnalyzeSymbol(SymbolAnalysisContext context)
    {
        var namedTypeSymbol = (INamedTypeSymbol)context.Symbol;

        // Check if this type implements ISinkNode<T>
        if (!IsSinkNodeImplementation(namedTypeSymbol))
            return;

        // Find ExecuteAsync method override
        var executeAsyncMethod = FindExecuteAsyncOverride(namedTypeSymbol);

        if (executeAsyncMethod == null)
            return;

        // Get the syntax for the method and check if input is consumed
        var methodSyntax = executeAsyncMethod.DeclaringSyntaxReferences.FirstOrDefault()?.GetSyntax();

        if (methodSyntax is not MethodDeclarationSyntax methodDeclaration)
            return;

        // Check if first parameter (input) is consumed
        var inputParameter = methodDeclaration.ParameterList.Parameters.FirstOrDefault();

        if (inputParameter == null)
            return;

        InputParameterUsageWalker walker = new(inputParameter.Identifier.Text);
        walker.Visit(methodDeclaration);

        if (!walker.IsInputConsumed)
        {
            var diagnostic = Diagnostic.Create(
                SinkNodeInputNotConsumedRule,
                methodDeclaration.Identifier.GetLocation(),
                namedTypeSymbol.Name);

            context.ReportDiagnostic(diagnostic);
        }
    }

    /// <summary>
    ///     Checks if the type implements ISinkNode&lt;T&gt;.
    /// </summary>
    private static bool IsSinkNodeImplementation(INamedTypeSymbol typeSymbol)
    {
        var currentType = typeSymbol;

        while (currentType != null)
        {
            foreach (var interfaceImpl in currentType.AllInterfaces)
            {
                if (interfaceImpl.Name == "ISinkNode" && interfaceImpl.IsGenericType)
                    return true;
            }

            currentType = currentType.BaseType;
        }

        return false;
    }

    /// <summary>
    ///     Finds the ExecuteAsync method override in the type.
    /// </summary>
    private static IMethodSymbol? FindExecuteAsyncOverride(INamedTypeSymbol typeSymbol)
    {
        return typeSymbol.GetMembers("ExecuteAsync")
            .OfType<IMethodSymbol>()
            .FirstOrDefault(m => m.IsOverride || IsInterfaceImplementation(m));
    }

    /// <summary>
    ///     Checks if method is an implementation of an interface method.
    /// </summary>
    private static bool IsInterfaceImplementation(IMethodSymbol method)
    {
        return method.ExplicitInterfaceImplementations.Any() ||
               method.ContainingType.AllInterfaces
                   .SelectMany(i => i.GetMembers())
                   .OfType<IMethodSymbol>()
                   .Any(i => SymbolEqualityComparer.Default.Equals(method, method.ContainingType.FindImplementationForInterfaceMember(i)));
    }

    /// <summary>
    ///     AST walker that detects if input parameter is consumed.
    /// </summary>
    private sealed class InputParameterUsageWalker(string inputParamName) : CSharpSyntaxWalker
    {
        public bool IsInputConsumed { get; private set; }

        public override void VisitForEachStatement(ForEachStatementSyntax node)
        {
            // Check if this is a foreach over input parameter
            if (IsForEachOverInput(node))
                IsInputConsumed = true;

            base.VisitForEachStatement(node);
        }

        public override void VisitAwaitExpression(AwaitExpressionSyntax node)
        {
            // Check for await foreach over input
            if (node.Parent is ForEachStatementSyntax forEach && IsForEachOverInput(forEach))
                IsInputConsumed = true;

            // Check for other async operations on input
            if (IsAsyncOperationOnInput(node))
                IsInputConsumed = true;

            base.VisitAwaitExpression(node);
        }

        public override void VisitInvocationExpression(InvocationExpressionSyntax node)
        {
            // Check for method calls on input parameter
            if (IsMethodCallOnInput(node))
                IsInputConsumed = true;

            base.VisitInvocationExpression(node);
        }

        public override void VisitMemberAccessExpression(MemberAccessExpressionSyntax node)
        {
            // Check for member access on input parameter
            if (IsMemberAccessOnInput(node))
                IsInputConsumed = true;

            base.VisitMemberAccessExpression(node);
        }

        /// <summary>
        ///     Checks if a foreach statement is iterating over input parameter.
        /// </summary>
        private bool IsForEachOverInput(ForEachStatementSyntax forEach)
        {
            if (forEach.Expression is IdentifierNameSyntax identifier)
                return identifier.Identifier.Text == inputParamName;

            // Check for member access like input.WithCancellation(cancellationToken)
            if (forEach.Expression is MemberAccessExpressionSyntax memberAccess)
                return IsMemberAccessOnInput(memberAccess);

            return false;
        }

        /// <summary>
        ///     Checks if an await expression is performing an async operation on input.
        /// </summary>
        private bool IsAsyncOperationOnInput(AwaitExpressionSyntax awaitExpression)
        {
            return awaitExpression.Expression switch
            {
                InvocationExpressionSyntax invocation => IsMethodCallOnInput(invocation),
                MemberAccessExpressionSyntax memberAccess => IsMemberAccessOnInput(memberAccess),
                _ => false,
            };
        }

        /// <summary>
        ///     Checks if an invocation is a method call on input parameter.
        /// </summary>
        private bool IsMethodCallOnInput(InvocationExpressionSyntax invocation)
        {
            if (invocation.Expression is MemberAccessExpressionSyntax memberAccess)
                return IsMemberAccessOnInput(memberAccess);

            return false;
        }

        /// <summary>
        ///     Checks if a member access expression is accessing the input parameter.
        /// </summary>
        private bool IsMemberAccessOnInput(MemberAccessExpressionSyntax memberAccess)
        {
            if (memberAccess.Expression is IdentifierNameSyntax identifier)
                return identifier.Identifier.Text == inputParamName;

            return false;
        }
    }
}
