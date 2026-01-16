using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using NPipeline.Pipeline;

namespace NPipeline.Analyzers.Tests;

public class SinkNodeInputConsumptionAnalyzerTests
{
    [Fact]
    public async Task ShouldDetectSinkNodeWithoutInputConsumption()
    {
        var testCode = """
                       using System;
                       using System.Threading;
                       using System.Threading.Tasks;
                       using NPipeline;
                       using NPipeline.DataFlow;
                       using NPipeline.Nodes;
                       using NPipeline.Pipeline;

                       namespace Test
                       {
                           public class TestSink : ISinkNode<int>
                           {
                               public Task ExecuteAsync(IDataPipe<int> input, PipelineContext context, CancellationToken cancellationToken)
                               {
                                   // Input parameter not consumed - this should trigger NP9301
                                   return Task.CompletedTask;
                               }
                               
                               public ValueTask DisposeAsync()
                               {
                                   return default;
                               }
                           }
                       }
                       """;

        var diagnostics = await GetDiagnostics(testCode);

        Assert.Single(diagnostics);
        Assert.Equal("NP9301", diagnostics[0].Id);
    }

    [Fact]
    public async Task ShouldNotDetectSinkNodeWithInputConsumption()
    {
        var testCode = """
                       using System;
                       using System.Threading;
                       using System.Threading.Tasks;
                       using NPipeline;
                       using NPipeline.DataFlow;
                       using NPipeline.Nodes;
                       using NPipeline.Pipeline;

                       namespace Test
                       {
                           public class TestSink : ISinkNode<int>
                           {
                               public async Task ExecuteAsync(IDataPipe<int> input, PipelineContext context, CancellationToken cancellationToken)
                               {
                                   await foreach (var item in input.WithCancellation(cancellationToken))
                                   {
                                       // Process item
                                   }
                               }
                               
                               public ValueTask DisposeAsync()
                               {
                                   return default;
                               }
                           }
                       }
                       """;

        var diagnostics = await GetDiagnostics(testCode);

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task ShouldNotDetectNonSinkNodeClass()
    {
        var testCode = """
                       using System;
                       using System.Threading;
                       using System.Threading.Tasks;
                       using NPipeline;
                       using NPipeline.DataFlow;
                       using NPipeline.Nodes;
                       using NPipeline.Pipeline;

                       namespace Test
                       {
                           public class NotASink : IAsyncDisposable
                           {
                               public Task ExecuteAsync(IDataPipe<int> input, PipelineContext context, CancellationToken cancellationToken)
                               {
                                   // This class doesn't implement ISinkNode, so no diagnostic should be reported
                                   return Task.CompletedTask;
                               }
                               
                               public ValueTask DisposeAsync()
                               {
                                   return default;
                               }
                           }
                       }
                       """;

        var diagnostics = await GetDiagnostics(testCode);
        Assert.Empty(diagnostics);
    }

    private async Task<Diagnostic[]> GetDiagnostics(string source)
    {
        // Get the path to the NPipeline assembly
        var nPipelineAssemblyPath = typeof(PipelineContext).Assembly.Location;
        var nPipelineAnalyzersAssemblyPath = typeof(SinkNodeInputConsumptionAnalyzer).Assembly.Location;

        var references = AppDomain.CurrentDomain.GetAssemblies()
            .Where(a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
            .Select(a => MetadataReference.CreateFromFile(a.Location))
            .ToList();

        // Add explicit references to NPipeline assemblies
        references.Add(MetadataReference.CreateFromFile(nPipelineAssemblyPath));
        references.Add(MetadataReference.CreateFromFile(nPipelineAnalyzersAssemblyPath));

        var compilation = CSharpCompilation.Create(
            "Test",
            new[] { CSharpSyntaxTree.ParseText(source) },
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var analyzer = new SinkNodeInputConsumptionAnalyzer();
        var analyzers = ImmutableArray.Create<DiagnosticAnalyzer>(analyzer);
        var compilationWithAnalyzers = compilation.WithAnalyzers(analyzers);

        var diagnostics = await compilationWithAnalyzers.GetAllDiagnosticsAsync();

        // Filter to only include our analyzer's diagnostics
        var filteredDiagnostics = diagnostics.Where(d => d.Id == "NP9301").ToArray();

        return filteredDiagnostics;
    }
}
