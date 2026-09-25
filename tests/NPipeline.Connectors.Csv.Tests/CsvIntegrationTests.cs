using AwesomeAssertions;
using NPipeline.Connectors.Csv;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataStreams;
using NPipeline.Execution;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.Csv.Tests;

public sealed class CsvIntegrationTests
{
    [Fact]
    public async Task CsvTap_WritesHeaderAndEveryRow_WithOneConsumeCall()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);
            var resolver = StorageProviderFactory.CreateResolver();
            var csvSink = new CsvSinkNode<Row>(uri, resolver);

            await using var context = new PipelineContext();

            await PipelineRunner.Create().RunAsync(new CsvTapPipeline(csvSink), context, CancellationToken.None);

            var lines = await File.ReadAllLinesAsync(tempFile);

            lines.Should().HaveCount(4);
            lines[0].Should().Be("id,name");
            lines.Skip(1).Should().Equal("1,alpha", "2,beta", "3,gamma");
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Csv_RoundTrip_WithFileSystemProvider_WritesAndReads()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            // No headers for simple scalar round-trip
            var cfg = new CsvConfiguration
            {
                BufferSize = 1024,
            };

            cfg.HelperConfiguration.HasHeaderRecord = false;

            // Write: CsvSinkNode<int>
            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new CsvSinkNode<int>(uri, resolver, cfg);
            IDataStream<int> input = new DataStream<int>(Enumerable.Range(1, 5).ToAsyncEnumerable());
            await sink.ConsumeAsync(input, PipelineContext.CreateDefault(), CancellationToken.None);

            // Read: CsvSourceNode<int>
            var src = new CsvSourceNode<int>(uri, MapIntRow, resolver, cfg);
            var outPipe = src.OpenStream(PipelineContext.CreateDefault(), CancellationToken.None);

            var result = new List<int>();

            await foreach (var i in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(i);
            }

            // Assert
            result.Should().Equal(1, 2, 3, 4, 5);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    private static int MapIntRow(CsvRow row) => row.GetByIndex(0, 0);

    private sealed record Row(int Id, string Name);

    private sealed class CsvTapPipeline(CsvSinkNode<Row> csvSink) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var source = builder.AddSource(() => new[]
            {
                new Row(1, "alpha"),
                new Row(2, "beta"),
                new Row(3, "gamma"),
            }, "source");

            var tap = builder.AddTap<Row>(csvSink, "tap");
            var sink = builder.AddSink<CountingSink, Row>("sink");
            _ = builder.AddPreconfiguredNodeInstance(sink.Id, new CountingSink()).Connect(source, tap).Connect(tap, sink);
        }
    }

    private sealed class CountingSink : SinkNode<Row>
    {
        public int Count { get; private set; }

        public override async Task ConsumeAsync(IDataStream<Row> input, PipelineContext context, CancellationToken cancellationToken)
        {
            await foreach (var _ in input.WithCancellation(cancellationToken))
                Count++;
        }
    }
}
