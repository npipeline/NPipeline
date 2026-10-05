using System.Text;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Files;
using NPipeline.Connectors.Json;
using NPipeline.Connectors.RoundTrip.Tests.Harnesses;
using NPipeline.Connectors.RoundTrip.Tests.Infrastructure;
using NPipeline.Connectors.RoundTrip.Tests.Models;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.RoundTrip.Tests;

/// <summary>The same round trips for both JSON formats; the known bugs are shared.</summary>
public abstract class JsonRoundTripTests(JsonFormat format)
{
    private readonly JsonHarness _harness = new(format);

    [Fact]
    public Task Scalars() => RoundTripScenarios.Scalars(_harness);

    [Fact]
    public Task SmallAndUnsignedIntegers() => RoundTripScenarios.SmallAndUnsignedIntegers(_harness);

    [Fact]
    public Task Nullables() => RoundTripScenarios.Nullables(_harness);

    [Fact]
    public Task Enums() => RoundTripScenarios.Enums(_harness);

    [Fact]
    public Task DateTimeOffsets() => RoundTripScenarios.DateTimeOffsets(_harness);

    [Fact]
    public Task DateOnlys() => RoundTripScenarios.DateOnlys(_harness);

    [Fact]
    public Task Text() => RoundTripScenarios.Text(_harness);

    [Fact]
    public Task ControlCharacters() => RoundTripScenarios.ControlCharacters(_harness);

    [Fact]
    public Task Binary() => RoundTripScenarios.Binary(_harness);

    [Fact]
    public Task Lists() => RoundTripScenarios.Lists(_harness);

    [Fact]
    public Task Nested() => RoundTripScenarios.Nested(_harness);

    [Fact]
    public Task PositionalRecords() => RoundTripScenarios.PositionalRecords(_harness);

    [Fact]
    public Task Volume() => RoundTripScenarios.Volume(_harness);

    [Fact]
    public Task Empty() => RoundTripScenarios.Empty(_harness);

    [Fact]
    public async Task Row_error_handler_skip_drops_value_type_rows()
    {
        var content = format == JsonFormat.Array
            ? """[{"v":1},{"v":"x"},{"v":3}]"""
            : "{\"v\":1}\n{\"v\":\"x\"}\n{\"v\":3}\n";

        _harness.Provider.Put(_harness.Uri, Encoding.UTF8.GetBytes(content));
        var source = JsonConnector.Source(
            _harness.Uri,
            row => row.Get<int>("v"),
            o => o with { Provider = _harness.Provider, Format = format, RowErrorHandler = _ => RowErrorAction.Skip });

        var values = await NodeRunner.ReadAsync(source);

        values.Should().Equal(1, 3);
    }
}

public sealed class JsonArrayRoundTripTests() : JsonRoundTripTests(JsonFormat.Array)
{
    private static long PendingBytes(string path) =>
        Directory.GetFiles(Path.GetDirectoryName(path)!, $".{Path.GetFileName(path)}.*.tmp").Sum(file => new FileInfo(file).Length);

    [Fact]
    public async Task Sink_streams_output_before_the_input_completes()
    {
        // Written to a real file so the test can watch bytes reach the stream while the sink is still consuming.
        var path = Path.Combine(Path.GetTempPath(), $"np_roundtrip_{Guid.NewGuid():N}.json");
        long bytesBeforeLastItem = -1;

        async IAsyncEnumerable<ScalarRecord> Items()
        {
            for (var i = 0; i < 50_000; i++)
            {
                yield return ScalarRecord.Create(i);
            }

            await Task.Yield();
            bytesBeforeLastItem = PendingBytes(path);
            yield return ScalarRecord.Create(50_000);
        }

        try
        {
            // The file system provider writes a hidden temporary sibling until the commit, so the test watches that grow.
            var sink = JsonConnector.Sink<ScalarRecord>(StorageUri.FromFilePath(path));
            await sink.ConsumeAsync(new NPipeline.DataFlow.DataStreams.DataStream<ScalarRecord>(Items()), PipelineContext.CreateDefault(), CancellationToken.None);

            bytesBeforeLastItem.Should().BeGreaterThan(1_000_000, "a 50,000-item array must not be held in memory until the end");
        }
        finally
        {
            File.Delete(path);
        }
    }
}

public sealed class NdjsonRoundTripTests() : JsonRoundTripTests(JsonFormat.NewlineDelimited);
