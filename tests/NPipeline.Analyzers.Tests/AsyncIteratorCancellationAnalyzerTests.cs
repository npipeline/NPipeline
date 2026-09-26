using Microsoft.CodeAnalysis;

namespace NPipeline.Analyzers.Tests;

public sealed class AsyncIteratorCancellationAnalyzerTests
{
    private const string Usings = """
                                  using System.Runtime.CompilerServices;
                                  using NPipeline.DataFlow;
                                  using NPipeline.DataFlow.DataStreams;

                                  """;

    private static async Task<IReadOnlyList<Diagnostic>> AnalyzeAsync(string source)
    {
        var diagnostics = await ResilienceAnalyzerTestHelper.GetDiagnosticsAsync<AsyncIteratorCancellationAnalyzer>(Usings + source);
        return diagnostics.Where(d => d.Id == AsyncIteratorCancellationAnalyzer.AsyncIteratorWithoutCancellationId).ToList();
    }

    [Fact]
    public async Task Reports_SourceIterator_WithoutAToken()
    {
        var diagnostics = await AnalyzeAsync("""
                                             public sealed class Ticks : SourceNode<int>
                                             {
                                                 public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
                                                     new DataStream<int>(Produce(), "ticks");

                                                 private static async IAsyncEnumerable<int> Produce()
                                                 {
                                                     for (var i = 0; ; i++)
                                                     {
                                                         await Task.Delay(1000);
                                                         yield return i;
                                                     }
                                                 }
                                             }
                                             """);

        var diagnostic = Assert.Single(diagnostics);
        Assert.Contains("Produce", diagnostic.GetMessage());
    }

    [Fact]
    public async Task Reports_LocalFunctionIterator_WithoutAToken()
    {
        var diagnostics = await AnalyzeAsync("""
                                             public sealed class Ticks : SourceNode<int>
                                             {
                                                 public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
                                                 {
                                                     return new DataStream<int>(Produce(), "ticks");

                                                     async IAsyncEnumerable<int> Produce()
                                                     {
                                                         await Task.Yield();
                                                         yield return 1;
                                                     }
                                                 }
                                             }
                                             """);

        Assert.Single(diagnostics);
    }

    [Fact]
    public async Task DoesNotReport_IteratorWithEnumeratorCancellation()
    {
        var diagnostics = await AnalyzeAsync("""
                                             public sealed class Ticks : SourceNode<int>
                                             {
                                                 public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
                                                     new DataStream<int>(Produce(cancellationToken), "ticks");

                                                 private static async IAsyncEnumerable<int> Produce([EnumeratorCancellation] CancellationToken cancellationToken = default)
                                                 {
                                                     await Task.Delay(1000, cancellationToken);
                                                     yield return 1;
                                                 }
                                             }
                                             """);

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task DoesNotReport_IteratorOutsideANode()
    {
        var diagnostics = await AnalyzeAsync("""
                                             public static class Helpers
                                             {
                                                 public static async IAsyncEnumerable<int> Produce()
                                                 {
                                                     await Task.Yield();
                                                     yield return 1;
                                                 }
                                             }
                                             """);

        Assert.Empty(diagnostics);
    }

    [Fact]
    public async Task DoesNotReport_NonIteratorReturningAnAsyncEnumerable()
    {
        var diagnostics = await AnalyzeAsync("""
                                             public sealed class PassThrough : TransformNode<int, int>
                                             {
                                                 public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken) =>
                                                     ValueTask.FromResult(item);

                                                 private static async Task<IAsyncEnumerable<int>> Wrap(IAsyncEnumerable<int> source)
                                                 {
                                                     await Task.Yield();
                                                     return source;
                                                 }
                                             }
                                             """);

        Assert.Empty(diagnostics);
    }
}
