using AwesomeAssertions;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Pipeline;

namespace NPipeline.Connectors.Csv.Tests;

public sealed class CsvNodeBehaviorTests
{
    [Fact]
    public async Task Sink_ShouldNotWriteHeader_ForPrimitive_WhenHeadersEnabled()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration
            {
                HasHeaderRecord = true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var sink = new CsvSinkNode<int>(uri, resolver, config);
            IDataPipe<int> input = new StreamingDataPipe<int>(new[] { 1, 2 }.ToAsyncEnumerable());
            await sink.ExecuteAsync(input, PipelineContext.Default, CancellationToken.None);

            var firstLine = File.ReadLines(tempFile).FirstOrDefault();

            firstLine.Should().Be("1");
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    [Fact]
    public async Task Source_ShouldSkipRow_WhenRowErrorHandlerReturnsTrue()
    {
        var tempFile = Path.Combine(Path.GetTempPath(), $"np_{Guid.NewGuid():N}.csv");

        try
        {
            await File.WriteAllTextAsync(tempFile, "Id,Name\n1,Alice\nbad,Bob\n2,Charlie\n");

            var uri = StorageUri.FromFilePath(tempFile);

            var config = new CsvConfiguration
            {
                HasHeaderRecord = true,
                RowErrorHandler = (_, _) => true,
            };

            var resolver = StorageProviderFactory.CreateResolver();
            var src = new CsvSourceNode<int>(uri, MapIntRow, resolver, config);
            var outPipe = src.Initialize(PipelineContext.Default, CancellationToken.None);

            var result = new List<int>();

            await foreach (var i in outPipe.WithCancellation(CancellationToken.None))
            {
                result.Add(i);
            }

            result.Should().Equal(1, 2);
        }
        finally
        {
            if (File.Exists(tempFile))
                File.Delete(tempFile);
        }
    }

    private static int MapIntRow(CsvRow row)
    {
        var raw = row.Get("Id", string.Empty) ?? string.Empty;
        return int.Parse(raw);
    }
}
