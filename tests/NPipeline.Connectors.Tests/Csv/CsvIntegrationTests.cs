using AwesomeAssertions;
using NPipeline.Connectors.Csv;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Tests.Csv;

public sealed class CsvIntegrationTests
{
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
            var resolver = StorageProviderFactory.CreateResolver().Resolver;
            var sink = new CsvSinkNode<int>(uri, resolver, cfg);
            IDataPipe<int> input = new StreamingDataPipe<int>(Enumerable.Range(1, 5).ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            // Read: CsvSourceNode<int>
            var src = new CsvSourceNode<int>(uri, resolver, cfg);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

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
}
