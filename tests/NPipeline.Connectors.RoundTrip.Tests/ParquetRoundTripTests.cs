using NPipeline.Connectors.Files;
using NPipeline.Connectors.Parquet;
using NPipeline.Connectors.RoundTrip.Tests.Harnesses;
using NPipeline.Connectors.RoundTrip.Tests.Infrastructure;
using NPipeline.Connectors.RoundTrip.Tests.Models;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common;

namespace NPipeline.Connectors.RoundTrip.Tests;

public sealed class ParquetRoundTripTests
{
    private readonly ParquetHarness _harness = new();

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
    public Task PositionalRecords() => RoundTripScenarios.PositionalRecords(_harness);

    [Fact]
    public Task Volume() => RoundTripScenarios.Volume(_harness);

    [Fact]
    public Task Empty() => RoundTripScenarios.Empty(_harness);

    [Fact]
    public Task Reads_from_non_seekable_streams() =>
        RoundTripScenarios.Scalars(new ParquetHarness { Provider = new InMemoryStorageProvider { NonSeekableReads = true } });

    [Fact]
    public Task Writes_to_non_seekable_streams() =>
        RoundTripScenarios.Scalars(new ParquetHarness { Provider = new InMemoryStorageProvider { NonSeekableWrites = true } });

    [Fact]
    public async Task Atomic_write_leaves_no_temporary_objects_behind()
    {
        await _harness.WriteAsync([ScalarRecord.Create(1)]);

        _harness.Provider.Keys.Should().ContainSingle().Which.Should().EndWith("/data.parquet");
    }

    [Fact]
    public async Task Write_keeps_uri_parameters_on_the_written_file()
    {
        var uri = StorageUri.Parse("mem://test/data.parquet?region=ap-southeast-2");
        var sink = ParquetConnector.Sink<ScalarRecord>(uri, o => o with { Provider = _harness.Provider });

        await NodeRunner.WriteAsync(sink, [ScalarRecord.Create(1)]);

        _harness.Provider.WriteRequests.Should().ContainSingle()
            .Which.Parameters.Should().Contain("region", "ap-southeast-2");
    }

    [Fact]
    public async Task Parallel_directory_read_does_not_deadlock()
    {
        // The deadlock depends on thread-pool scheduling, so read several times; one pass often gets lucky.
        const int files = 40;
        const int rowsPerFile = 200;
        var provider = new InMemoryStorageProvider();
        for (var file = 0; file < files; file++)
        {
            var sink = ParquetConnector.Sink<ScalarRecord>(InMemoryStorageProvider.Uri($"parts/f{file:D2}.parquet"), o => o with { Provider = provider, RowGroupSize = 10 });
            await NodeRunner.WriteAsync(sink, Enumerable.Range(file * rowsPerFile, rowsPerFile).Select(ScalarRecord.Create));
        }

        for (var attempt = 0; attempt < 5; attempt++)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            var source = ParquetConnector.Source<ScalarRecord>(InMemoryStorageProvider.Uri("parts/"), o => o with { Provider = provider, FileReadParallelism = 4 });

            var rows = await NodeRunner.ReadAsync(source, timeout.Token);

            rows.Select(r => r.Id).Should().Equal(Enumerable.Range(0, files * rowsPerFile), "files are read in parallel but emitted in file order");
        }
    }

    [Fact]
    public async Task Stopping_a_parallel_read_early_releases_the_workers()
    {
        var provider = new InMemoryStorageProvider();
        for (var file = 0; file < 8; file++)
        {
            var sink = ParquetConnector.Sink<ScalarRecord>(InMemoryStorageProvider.Uri($"parts/f{file}.parquet"), o => o with { Provider = provider, RowGroupSize = 10 });
            await NodeRunner.WriteAsync(sink, Enumerable.Range(file * 200, 200).Select(ScalarRecord.Create));
        }

        var source = ParquetConnector.Source<ScalarRecord>(InMemoryStorageProvider.Uri("parts/"), o => o with { Provider = provider, FileReadParallelism = 4 });
        var stream = source.OpenStream(NPipeline.Pipeline.PipelineContext.CreateDefault(), CancellationToken.None);
        var read = 0;

        // Disposing the enumerator after an early break must cancel and await the workers blocked on full channels.
        var consume = async () =>
        {
            await foreach (var _ in stream)
            {
                if (++read == 5)
                    break;
            }
        };

        await consume.Should().CompleteWithinAsync(TimeSpan.FromSeconds(10));
        read.Should().Be(5);
    }
}
