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
///     Code fix provider that adds a public parameterless constructor to node implementations.
/// </summary>
[ExportCodeFixProvider(LanguageNames.CSharp, Name = nameof(NodeParameterlessConstructorCodeFixProvider))]
[Shared]
public sealed class NodeParameterlessConstructorCodeFixProvider : CodeFixProvider
{
    /// <inheritdoc />
    public override ImmutableArray<string> FixableDiagnosticIds =>
    [
        NodeParameterlessConstructorAnalyzer.MissingParameterlessConstructorId,
        NodeParameterlessConstructorAnalyzer.PerformanceSuggestionId,
    ];

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
        var classDeclaration = root.FindToken(diagnosticSpan.Start)
            .Parent?
            .AncestorsAndSelf()
            .OfType<ClassDeclarationSyntax>()
            .FirstOrDefault();

        if (classDeclaration == null)
            return;

        // Register code fix to add parameterless constructor
        context.RegisterCodeFix(
            CodeAction.Create(
                "Add public parameterless constructor",
                ct => AddParameterlessConstructorAsync(context.Document, classDeclaration, ct),
                nameof(NodeParameterlessConstructorCodeFixProvider)),
            diagnostic);
    }

    /// <summary>
    ///     Adds a public parameterless constructor to the class.
    /// </summary>
    private static async Task<Document> AddParameterlessConstructorAsync(
        Document document,
        ClassDeclarationSyntax classDeclaration,
        CancellationToken cancellationToken)
    {
        var root = await document.GetSyntaxRootAsync(cancellationToken);

        if (root == null)
            return document;

        // Get the class name
        var className = classDeclaration.Identifier.Text;

        // Create the parameterless constructor
        var constructor = SyntaxFactory.ConstructorDeclaration(className)
            .AddModifiers(SyntaxFactory.Token(SyntaxKind.PublicKeyword))
            .WithBody(SyntaxFactory.Block())
            .WithLeadingTrivia(
                SyntaxFactory.TriviaList(
                    SyntaxFactory.Comment("/// <summary>"),
                    SyntaxFactory.CarriageReturnLineFeed,
                    SyntaxFactory.Comment($"///     Initializes a new instance of the <see cref=\"{className}\" /> class."),
                    SyntaxFactory.CarriageReturnLineFeed,
                    SyntaxFactory.Comment("/// </summary>"),
                    SyntaxFactory.CarriageReturnLineFeed))
            .NormalizeWhitespace();

        // Find the best location to insert the constructor
        var members = classDeclaration.Members;
        var insertIndex = 0;

        // Try to insert after fields but before methods
        for (var i = 0; i < members.Count; i++)
        {
            if (members[i] is FieldDeclarationSyntax or PropertyDeclarationSyntax)
                insertIndex = i + 1;
            else if (members[i] is ConstructorDeclarationSyntax)
            {
                // Insert before existing constructors if they have parameters
                if (members[i] is ConstructorDeclarationSyntax existingCtor && existingCtor.ParameterList.Parameters.Count > 0)
                {
                    insertIndex = i;
                    break;
                }

                insertIndex = i + 1;
            }
            else if (members[i] is MethodDeclarationSyntax)
            {
                // Stop at first method if we haven't found a constructor
                break;
            }
        }

        // Insert the constructor
        var newMembers = members.Insert(insertIndex, constructor);
        var newClass = classDeclaration.WithMembers(newMembers);

        // Replace the old class with the new one
        var newRoot = root.ReplaceNode(classDeclaration, newClass);

        return document.WithSyntaxRoot(newRoot);
    }
}
