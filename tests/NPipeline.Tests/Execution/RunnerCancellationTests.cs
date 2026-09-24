using AwesomeAssertions;
using NPipeline.Configuration;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.Tests.Reliability.Behavior;

namespace NPipeline.Tests.Execution;

/// <summary>
///     The token passed to <see cref="PipelineRunner.RunAsync(IPipelineDefinition, PipelineContext, CancellationToken)" />
///     used to reach only the setup stage: node execution observed the context's token, so cancelling the runner's
///     token never stopped a running pipeline.
/// </summary>
public sealed class RunnerCancellationTests
{
    private static readonly TimeSpan WaitLimit = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task CancellingTheRunnerToken_StopsARunningPipeline()
    {
        var sink = new CollectingSink<int>();
        using var cts = new CancellationTokenSource();
        await using var context = new PipelineContext();

        var run = PipelineRunner.Create().RunAsync(Unbounded(sink), context, cts.Token);
        await sink.FirstItemReceived.WaitAsync(WaitLimit);

        await cts.CancelAsync();

        var act = () => run.WaitAsync(WaitLimit);
        _ = await act.Should().ThrowAsync<OperationCanceledException>("the caller's token must stop the pipeline");
    }

    [Fact]
    public async Task CancellingTheContextToken_StillStopsARunningPipeline()
    {
        var sink = new CollectingSink<int>();
        using var contextCts = new CancellationTokenSource();
        using var runnerCts = new CancellationTokenSource();
        await using var context = new PipelineContext(PipelineContextConfiguration.WithCancellation(contextCts.Token));

        var run = PipelineRunner.Create().RunAsync(Unbounded(sink), context, runnerCts.Token);
        await sink.FirstItemReceived.WaitAsync(WaitLimit);

        await contextCts.CancelAsync();

        var act = () => run.WaitAsync(WaitLimit);
        _ = await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task NodesReadingTheContextToken_SeeTheRunnerToken()
    {
        using var cts = new CancellationTokenSource();
        var probe = new ContextTokenProbe();
        await using var context = new PipelineContext();

        var run = PipelineRunner.Create().RunAsync(new BehaviorPipeline(b =>
        {
            var s = b.AddSource<ContextTokenProbe, int>("source");
            var k = b.AddSink<CollectingSink<int>, int>("sink");

            _ = b.AddPreconfiguredNodeInstance(s.Id, probe)
                .AddPreconfiguredNodeInstance(k.Id, new CollectingSink<int>())
                .Connect(s, k);
        }), context, cts.Token);

        var contextToken = await probe.ContextToken.WaitAsync(WaitLimit);
        await cts.CancelAsync();

        contextToken.IsCancellationRequested.Should().BeTrue("a node that watches context.CancellationToken must see the caller's cancellation");

        var act = () => run.WaitAsync(WaitLimit);
        _ = await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task AfterTheRun_TheContextTokenIsTheOneItWasCreatedWith()
    {
        using var cts = new CancellationTokenSource();
        await using var context = new PipelineContext();

        await PipelineRunner.Create().RunAsync(new BehaviorPipeline(b =>
        {
            var s = b.AddSource<StreamingSource<int>, int>("source");
            var k = b.AddSink<CollectingSink<int>, int>("sink");

            _ = b.AddPreconfiguredNodeInstance(s.Id, StreamingSource<int>.Of([1]))
                .AddPreconfiguredNodeInstance(k.Id, new CollectingSink<int>())
                .Connect(s, k);
        }), context, cts.Token);

        // The run's link is gone, so cancelling that run's token no longer affects the context.
        await cts.CancelAsync();
        context.CancellationToken.IsCancellationRequested.Should().BeFalse();
        context.CancellationToken.Should().Be(CancellationToken.None);
    }

    private static BehaviorPipeline Unbounded(CollectingSink<int> sink)
    {
        return new BehaviorPipeline(b =>
        {
            var s = b.AddSource<StreamingSource<int>, int>("source");
            var k = b.AddSink<CollectingSink<int>, int>("sink");

            _ = b.AddPreconfiguredNodeInstance(s.Id, StreamingSource<int>.Unbounded([1, 2, 3]))
                .AddPreconfiguredNodeInstance(k.Id, sink)
                .Connect(s, k);
        });
    }

    /// <summary>
    ///     A source that reports the context's token and then waits on it, never on the token it is handed.
    /// </summary>
    private sealed class ContextTokenProbe : SourceNode<int>
    {
        private readonly TaskCompletionSource<CancellationToken> _contextToken = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<CancellationToken> ContextToken => _contextToken.Task;

        public override IDataStream<int> OpenStream(PipelineContext context, CancellationToken cancellationToken) =>
            new DataStream<int>(Stream(context), "probe");

        private async IAsyncEnumerable<int> Stream(PipelineContext context)
        {
            _ = _contextToken.TrySetResult(context.CancellationToken);
            await Task.Delay(Timeout.Infinite, context.CancellationToken).ConfigureAwait(false);
            yield break;
        }
    }
}
