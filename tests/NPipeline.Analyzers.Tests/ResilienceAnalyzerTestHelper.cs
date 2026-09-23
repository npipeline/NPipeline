using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Pipeline;

namespace NPipeline.Analyzers.Tests;

/// <summary>
///     Runs an analyzer over source compiled against the real NPipeline assembly. The source must compile, so the
///     tests cannot drift from the public resilience API.
/// </summary>
internal static class ResilienceAnalyzerTestHelper
{
    private const string Usings = """
                                  using System;
                                  using System.Collections.Generic;
                                  using System.Threading;
                                  using System.Threading.Tasks;
                                  using NPipeline.Graph;
                                  using NPipeline.Nodes;
                                  using NPipeline.Pipeline;
                                  using NPipeline.Reliability;

                                  """;

    public static async Task<ImmutableArray<Diagnostic>> GetDiagnosticsAsync<TAnalyzer>(string source)
        where TAnalyzer : DiagnosticAnalyzer, new()
    {
        var syntaxTree = CSharpSyntaxTree.ParseText(Usings + source, new CSharpParseOptions(LanguageVersion.Latest));

        var references = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .Select(a => MetadataReference.CreateFromFile(a.Location))
            .Append(MetadataReference.CreateFromFile(typeof(PipelineBuilder).Assembly.Location));

        var compilation = CSharpCompilation.Create(
            "ResilienceAnalyzerTest",
            [syntaxTree],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));

        var errors = compilation.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error).ToList();
        Assert.True(errors.Count == 0, "Test source does not compile:\n" + string.Join("\n", errors));

        var diagnostics = await compilation
            .WithAnalyzers([new TAnalyzer()])
            .GetAnalyzerDiagnosticsAsync();

        return diagnostics;
    }
}
