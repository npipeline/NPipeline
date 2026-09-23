namespace NPipeline.Analyzers.Tests;

public sealed class NodeKindResilienceMisuseAnalyzerTests
{
    private const string Nodes = """

                                 public sealed class Numbers : SourceNode<int>
                                 {
                                     public override NPipeline.DataFlow.IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken)
                                         => throw new NotImplementedException();
                                 }

                                 public sealed class Double : TransformNode<int, int>
                                 {
                                     public override ValueTask<int> TransformAsync(int item, PipelineContext context, CancellationToken cancellationToken)
                                         => new(item * 2);
                                 }

                                 public sealed class Store : SinkNode<int>
                                 {
                                     public override Task ConsumeAsync(NPipeline.DataFlow.IDataStream<int> input, PipelineContext context, CancellationToken cancellationToken)
                                         => Task.CompletedTask;
                                 }

                                 """;

    private static Task<IReadOnlyList<Microsoft.CodeAnalysis.Diagnostic>> AnalyzeAsync(string body)
    {
        var source = Nodes + $$"""
                               public static class Setup
                               {
                                   public static void Configure(PipelineBuilder builder)
                                   {
                                       var source = builder.AddSource<Numbers, int>("source");
                                       var transform = builder.AddTransform<Double, int, int>("transform");
                                       var sink = builder.AddSink<Store, int>("sink");
                               {{body}}
                                   }
                               }
                               """;

        return AnalyzeCoreAsync(source);
    }

    private static async Task<IReadOnlyList<Microsoft.CodeAnalysis.Diagnostic>> AnalyzeCoreAsync(string source)
    {
        var diagnostics = await ResilienceAnalyzerTestHelper.GetDiagnosticsAsync<NodeKindResilienceMisuseAnalyzer>(source);
        return diagnostics.Where(d => d.Id == NodeKindResilienceMisuseAnalyzer.NodeKindResilienceMisuseId).ToList();
    }

    [Theory]
    [InlineData("builder.WithResilience(sink, o => o with { ItemRetry = ItemRetryOptions.Default });", "ItemRetry")]
    [InlineData("builder.WithResilience(source, o => o with { NodeRestart = new NodeRestartOptions { MaxRestarts = 3 } });", "NodeRestart")]
    [InlineData("builder.WithResilience(sink, o => o with { CircuitBreaker = CircuitBreakerOptions.Default });", "CircuitBreaker")]
    [InlineData("builder.WithResilience(sink, o => o with { ItemRetry = o.ItemRetry with { MaxRetries = 5 } });", "ItemRetry")]
    [InlineData("builder.WithResilience(sink, _ => new PipelineResilienceOptions { ItemRetry = ItemRetryOptions.Default });", "ItemRetry")]
    public async Task Reports_TransformOnlySetting_OnNonTransformHandle(string call, string setting)
    {
        var diagnostics = await AnalyzeAsync(call);

        var diagnostic = Assert.Single(diagnostics);
        Assert.Equal(Microsoft.CodeAnalysis.DiagnosticSeverity.Error, diagnostic.Severity);
        Assert.Contains(setting, diagnostic.GetMessage());
    }

    [Fact]
    public async Task Reports_EverySettingInOneInitializer()
    {
        var diagnostics = await AnalyzeAsync(
                "builder.WithResilience(sink, o => o with { ItemRetry = ItemRetryOptions.Default, CircuitBreaker = CircuitBreakerOptions.Default });");

        Assert.Equal(2, diagnostics.Count);
    }

    [Theory]
    [InlineData("builder.WithResilience(transform, o => o with { ItemRetry = ItemRetryOptions.Default, CircuitBreaker = CircuitBreakerOptions.Default });")]
    [InlineData("builder.WithResilience(sink, o => o with { NodeRetry = new NodeRetryOptions { MaxRetries = 2 } });")]
    [InlineData("builder.WithResilience(sink, o => o with { ItemRetry = o.ItemRetry, OnItemFailure = ItemFailureAction.Skip });")]
    [InlineData("builder.WithResilience(o => o with { ItemRetry = ItemRetryOptions.Default });")]
    public async Task DoesNotReport_SettingsTheBuildAccepts(string call)
    {
        Assert.Empty(await AnalyzeAsync(call));
    }
}
