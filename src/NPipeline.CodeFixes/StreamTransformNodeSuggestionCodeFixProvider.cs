using System.Collections.Immutable;
using System.Composition;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CodeActions;
using Microsoft.CodeAnalysis.CodeFixes;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using NPipeline.Analyzers;

namespace NPipeline.CodeFixes;

/// <summary>
///     Code fix provider that helps convert ITransformNode implementations with IAsyncEnumerable return types to IStreamTransformNode.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(StreamTransformNodeSuggestionCodeFixProvider))]
[Shared]
public sealed class StreamTransformNodeSuggestionCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
        [StreamTransformNodeSuggestionAnalyzer.StreamTransformNodeSuggestionId];

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

        // Find the class declaration identified by diagnostic
        var node = root.FindNode(diagnosticSpan);

        if (node is not ClassDeclarationSyntax classDeclaration)
            return;

        var semanticModel = await context.Document.GetSemanticModelAsync(context.CancellationToken);
        var classSymbol = semanticModel.GetDeclaredSymbol(classDeclaration);

        if (classSymbol == null)
            return;

        // Get the input and output types from the diagnostic message
        if (!TryExtractTypesFromDiagnostic(diagnostic, out var inputType, out var outputType))
            return;

        // Register code fix to convert to IStreamTransformNode
        context.RegisterCodeFix(
            CodeAction.Create(
                $"Convert to IStreamTransformNode<{inputType}, {outputType}>",
                ct => ConvertToIStreamTransformNodeAsync(context.Document, classDeclaration, inputType, outputType, ct),
                nameof(StreamTransformNodeSuggestionCodeFixProvider) + "_ConvertToIStreamTransformNode"),
            diagnostic);
    }

    /// <summary>
    ///     Converts a class implementing ITransformNode to IStreamTransformNode.
    /// </summary>
    private static async Task<Document> ConvertToIStreamTransformNodeAsync(
        Document document,
        ClassDeclarationSyntax classDeclaration,
        string inputType,
        string outputType,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        var semanticModel = await document.GetSemanticModelAsync(cancellationToken);

        // 1. Update the base interface list
        var baseList = classDeclaration.BaseList;

        if (baseList != null)
        {
            // Find and replace ITransformNode<TIn, TOut> with IStreamTransformNode<TIn, TOut>
            var newBaseTypes = new List<BaseTypeSyntax>();
            var interfaceReplaced = false;

            foreach (var baseType in baseList.Types)
            {
                if (baseType.Type is GenericNameSyntax genericName &&
                    genericName.Identifier.Text == "ITransformNode" &&
                    genericName.TypeArgumentList.Arguments.Count == 2)
                {
                    // Replace with IStreamTransformNode
                    var newGenericName = SyntaxFactory.GenericName(
                        SyntaxFactory.Identifier("IStreamTransformNode"),
                        genericName.TypeArgumentList);

                    newBaseTypes.Add(SyntaxFactory.SimpleBaseType(newGenericName));
                    interfaceReplaced = true;
                }
                else
                    newBaseTypes.Add(baseType);
            }

            if (interfaceReplaced)
            {
                var newBaseList = SyntaxFactory.BaseList(SyntaxFactory.SeparatedList(newBaseTypes));
                classDeclaration = classDeclaration.WithBaseList(newBaseList);
            }
        }

        // 2. Find and update the ExecuteAsync method
        var executeAsyncMethod = classDeclaration.Members
            .OfType<MethodDeclarationSyntax>()
            .FirstOrDefault(m => m.Identifier.Text == "ExecuteAsync");

        if (executeAsyncMethod != null)
        {
            // Update the return type from Task<TOut> to IAsyncEnumerable<TOut>
            var newReturnType = SyntaxFactory.ParseTypeName($"IAsyncEnumerable<{outputType}>");

            // Update the first parameter from TIn item to IAsyncEnumerable<TIn> items
            var newParameters = new List<ParameterSyntax>();
            var parametersUpdated = false;

            foreach (var parameter in executeAsyncMethod.ParameterList.Parameters)
            {
                if (parameter.Type is PredefinedTypeSyntax predefinedType &&
                    predefinedType.Keyword.Text == inputType &&
                    parameter.Identifier.Text == "item")
                {
                    // Replace with IAsyncEnumerable<TIn> items
                    var newParameter = SyntaxFactory.Parameter(
                        parameter.AttributeLists,
                        parameter.Modifiers,
                        SyntaxFactory.ParseTypeName($"IAsyncEnumerable<{inputType}>"),
                        SyntaxFactory.Identifier("items"),
                        parameter.Default);

                    newParameters.Add(newParameter);
                    parametersUpdated = true;
                }
                else
                    newParameters.Add(parameter);
            }

            if (parametersUpdated)
            {
                var newParameterList = SyntaxFactory.ParameterList(
                    SyntaxFactory.SeparatedList(newParameters));

                executeAsyncMethod = executeAsyncMethod
                    .WithReturnType(newReturnType)
                    .WithParameterList(newParameterList);

                // Replace the method in the class
                var newMembers = classDeclaration.Members.Replace(executeAsyncMethod, executeAsyncMethod);
                classDeclaration = classDeclaration.WithMembers(newMembers);
            }
        }

        // 3. Add using statement for IAsyncEnumerable if not present
        var compilationUnit = root as CompilationUnitSyntax ?? classDeclaration.Parent as CompilationUnitSyntax;

        if (compilationUnit != null)
        {
            var hasSystemCollectionsGeneric = compilationUnit.Usings
                .Any(u => u.Name?.ToString() == "System.Collections.Generic");

            if (!hasSystemCollectionsGeneric)
            {
                var newUsing = SyntaxFactory.UsingDirective(
                    SyntaxFactory.ParseName("System.Collections.Generic"));

                compilationUnit = compilationUnit.AddUsings(newUsing);
                root = compilationUnit;
            }
        }

        // Replace the class declaration in the root
        var newRoot = root.ReplaceNode(
            root.FindNode(classDeclaration.Span),
            classDeclaration);

        return document.WithSyntaxRoot(newRoot);
    }

    /// <summary>
    ///     Extracts input and output types from the diagnostic message.
    /// </summary>
    private static bool TryExtractTypesFromDiagnostic(Diagnostic diagnostic, out string inputType, out string outputType)
    {
        inputType = string.Empty;
        outputType = string.Empty;

        // The diagnostic message format is:
        // "Class '{0}' implements ITransformNode but ExecuteAsync returns IAsyncEnumerable<{1}>. 
        //  Consider implementing IStreamTransformNode<{2}, {1}> instead for better interface segregation."
        // where {0}=className, {1}=outputType, {2}=inputType

        // Extract types from the diagnostic message using regex
        var message = diagnostic.GetMessage();

        // Pattern to match: IAsyncEnumerable<OutputType> and IStreamTransformNode<InputType, OutputType>
        var asyncEnumerableMatch = Regex.Match(
            message, @"IAsyncEnumerable<([^>]+)>");

        var streamTransformMatch = Regex.Match(
            message, @"IStreamTransformNode<([^,]+),\s*([^>]+)>");

        if (asyncEnumerableMatch.Success && streamTransformMatch.Success)
        {
            outputType = asyncEnumerableMatch.Groups[1].Value.Trim();
            inputType = streamTransformMatch.Groups[1].Value.Trim();
            return true;
        }

        return false;
    }
}
