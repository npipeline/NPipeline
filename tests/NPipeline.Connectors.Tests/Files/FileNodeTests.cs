using System.Diagnostics.Metrics;
using System.IO.Compression;
using System.Text;
using AwesomeAssertions;
using NPipeline.Connectors.Diagnostics;
using NPipeline.Connectors.Errors;
using NPipeline.Connectors.Files;
using NPipeline.DataFlow.DataStreams;
using NPipeline.ErrorHandling;
using NPipeline.Execution;
using NPipeline.Extensions.Testing;
using NPipeline.Pipeline;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common;
using Xunit;

namespace NPipeline.Connectors.Tests.Files;

[Collection(MetricCaptureFixture.Name)]
public sealed class FileSourceNodeTests
{
    private readonly InMemoryStorageProvider _provider = new();

    [Fact]
    public async Task Reads_a_single_file()
    {
        Put("data.txt", "a\nb\n");

        (await Read("data.txt")).Should().Equal("a", "b");
    }

    [Fact]
    public async Task Reads_a_directory_in_path_order_filtered_by_extension()
    {
        Put("dir/b.txt", "b");
        Put("dir/a.txt", "a");
        Put("dir/skip.csv", "x");
        Put("dir/sub/c.txt", "c");

        (await Read("dir/")).Should().Equal("a", "b");
        (await Read("dir/", recursive: true)).Should().Equal("a", "b", "c");
    }

    [Theory]
    [InlineData("logs/2026-*/*.txt", new[] { "jan", "feb" })]
    [InlineData("logs/**/*.txt", new[] { "old", "jan", "feb" })]
    [InlineData("logs/2026-*/day1.txt", new[] { "jan" })]
    public async Task Reads_files_matching_a_glob(string glob, string[] expected)
    {
        Put("logs/2025.txt", "old");
        Put("logs/2026-01/day1.txt", "jan");
        Put("logs/2026-02/day2.txt", "feb");
        Put("logs/2026-02/notes.md", "no");

        (await Read(glob)).Should().BeEquivalentTo(expected);
    }

    [Fact]
    public async Task Keeps_the_uri_parameters_on_listed_files()
    {
        var provider = new MoveableProvider(_provider);
        Put("dir/a.txt", "a");
        var source = new LineSource(new LineSourceOptions { Uri = StorageUri.Parse("mem://test/dir/?region=ap-southeast-2"), Provider = provider });

        _ = await NodeRunner(source);

        provider.Reads.Should().ContainSingle().Which.Parameters.Should().Contain("region", "ap-southeast-2");
    }

    [Theory]
    [InlineData("data.txt.gz")]
    [InlineData("data.txt.br")]
    [InlineData("data.txt.zz")]
    public async Task Decompresses_by_suffix(string path)
    {
        _provider.Put(InMemoryStorageProvider.Uri(path), Compress(path, "x\ny\n"));

        (await Read(path)).Should().Equal("x", "y");
    }

    [Fact]
    public async Task Formats_with_their_own_compression_ignore_the_suffix_and_reject_explicit_compression()
    {
        Put("data.txt.gz", "plain");

        var auto = new LineSource(new LineSourceOptions { Uri = InMemoryStorageProvider.Uri("data.txt.gz"), Provider = _provider }) { Compressible = false };
        var explicitGzip = new LineSource(new LineSourceOptions { Uri = InMemoryStorageProvider.Uri("data.txt.gz"), Provider = _provider, Compression = FileCompression.Gzip }) { Compressible = false };

        (await NodeRunner(auto)).Should().Equal("plain");
        await FluentActions.Awaiting(() => NodeRunner(explicitGzip)).Should().ThrowAsync<NotSupportedException>();
    }

    [Fact]
    public async Task Spools_non_seekable_streams_for_formats_that_need_to_seek()
    {
        var provider = new InMemoryStorageProvider { NonSeekableReads = true };
        provider.Put(InMemoryStorageProvider.Uri("data.txt"), Encoding.UTF8.GetBytes("a\nb"));
        var source = new LineSource(new LineSourceOptions { Uri = InMemoryStorageProvider.Uri("data.txt"), Provider = provider }) { NeedsSeekableStream = true };

        (await NodeRunner(source)).Should().Equal("a", "b");
        source.SeekableStreamsSeen.Should().Equal(true);
    }

    [Fact]
    public async Task Fails_on_a_bad_record_without_a_handler()
    {
        _provider.Put(StorageUri.Parse("mem://test/data.txt?token=secret"), Encoding.UTF8.GetBytes("a\nbad\nc"));
        var source = new LineSource(new LineSourceOptions { Uri = StorageUri.Parse("mem://test/data.txt?token=secret"), Provider = _provider });

        var failure = (await FluentActions.Awaiting(() => NodeRunner(source)).Should().ThrowAsync<RecordMappingException>()).Which;

        failure.RecordSource.Should().Be("mem://test/data.txt", "the query string can carry credentials");
        failure.RecordNumber.Should().Be(2);
        failure.RawExcerpt.Should().Be("bad");
        failure.InnerException.Should().BeOfType<FormatException>();
    }

    [Fact]
    public async Task Skips_bad_records_when_the_handler_says_so()
    {
        Put("data.txt", "a\nbad\nc");
        var errors = new List<RowError>();

        var rows = await Read("data.txt", error =>
        {
            errors.Add(error);
            return RowErrorAction.Skip;
        });

        rows.Should().Equal("a", "c");
        errors.Should().ContainSingle().Which.RecordNumber.Should().Be(2);
    }

    [Fact]
    public async Task Truncates_or_omits_the_raw_excerpt()
    {
        Put("data.txt", "bad");
        RowError? truncated = null;
        RowError? omitted = null;

        _ = await NodeRunner(new LineSource(new LineSourceOptions
        {
            Uri = InMemoryStorageProvider.Uri("data.txt"), Provider = _provider, RawExcerptLength = 2, RowErrorHandler = e => Capture(e, out truncated),
        }));

        _ = await NodeRunner(new LineSource(new LineSourceOptions
        {
            Uri = InMemoryStorageProvider.Uri("data.txt"), Provider = _provider, RawExcerptLength = 0, RowErrorHandler = e => Capture(e, out omitted),
        }));

        truncated!.RawExcerpt.Should().Be("ba…");
        omitted!.RawExcerpt.Should().BeNull();
    }

    [Fact]
    public async Task Dead_letters_bad_records_attributed_to_the_source_node()
    {
        Put("data.txt", "a\nbad\nc");
        var deadLetters = new CapturingDeadLetterSink();
        var context = new PipelineContext();

        var source = new LineSource(new LineSourceOptions
        {
            Uri = InMemoryStorageProvider.Uri("data.txt"), Provider = _provider, RowErrorHandler = _ => RowErrorAction.DeadLetter,
        });

        await PipelineRunner.Create().RunAsync(new SingleSourcePipeline(source, deadLetters), context);

        var envelope = deadLetters.Captured.Should().ContainSingle().Subject;
        envelope.Item.Should().Be(new ConnectorRecordFailure("mem://test/data.txt", 2, null, "bad"));
        envelope.Attribution.DecisionNodeId.Should().Be("lines");
        context.GetSink<InMemorySinkNode<string>>().Items.Should().BeEquivalentTo(["a", "c"]);
    }

    [Fact]
    public async Task Dead_lettering_without_a_sink_fails()
    {
        Put("data.txt", "bad");

        var rows = () => Read("data.txt", _ => RowErrorAction.DeadLetter);

        await rows.Should().ThrowAsync<DeadLetterSinkNotConfiguredException>();
    }

    [Fact]
    public async Task Reads_files_ahead_in_parallel_and_keeps_file_order()
    {
        for (var i = 0; i < 20; i++)
        {
            Put($"in/{i:D2}.txt", string.Join('\n', Enumerable.Range(0, 50).Select(j => $"{i}-{j}")));
        }

        var rows = await NodeRunner(new LineSource(new LineSourceOptions { Uri = InMemoryStorageProvider.Uri("in/"), Provider = _provider, FileReadParallelism = 4 }));

        rows.Should().Equal(Enumerable.Range(0, 20).SelectMany(i => Enumerable.Range(0, 50).Select(j => $"{i}-{j}")));
    }

    [Fact]
    public async Task Stopping_a_parallel_read_early_releases_the_readers()
    {
        for (var i = 0; i < 40; i++)
        {
            Put($"in/{i:D2}.txt", string.Join('\n', Enumerable.Range(0, 5_000).Select(j => $"{j}")));
        }

        var source = new LineSource(new LineSourceOptions { Uri = InMemoryStorageProvider.Uri("in/"), Provider = _provider, FileReadParallelism = 8 });
        var read = 0;

        await foreach (var _ in source.OpenStream(new PipelineContext(), CancellationToken.None))
        {
            if (++read == 10)
                break;
        }

        read.Should().Be(10, "disposing the stream stops and awaits the readers instead of hanging");
    }

    [Fact]
    public async Task Reports_rows_bytes_and_files_with_bounded_tags()
    {
        Put("data.txt", "a\nb\nc\n");
        using var metrics = new MetricCapture();

        _ = await Read("data.txt");

        metrics.Total("npipeline.connector.rows_read").Should().Be(3);
        metrics.Total("npipeline.connector.bytes_read").Should().Be(6);
        metrics.Total("npipeline.connector.files_read").Should().Be(1);
        metrics.TagKeys("npipeline.connector.rows_read").Should().BeEquivalentTo(["connector", "storage.scheme"]);
    }

    [Fact]
    public void Validates_options()
    {
        var bufferSize = () => new LineSource(new LineSourceOptions { Uri = InMemoryStorageProvider.Uri("a.txt"), BufferSize = 0 });
        var excerpt = () => new LineSource(new LineSourceOptions { Uri = InMemoryStorageProvider.Uri("a.txt"), RawExcerptLength = -1 });

        bufferSize.Should().Throw<ArgumentOutOfRangeException>();
        excerpt.Should().Throw<ArgumentOutOfRangeException>();
    }

    internal static byte[] Compress(string path, string text)
    {
        using var buffer = new MemoryStream();

        using (Stream compressor = path switch
               {
                   _ when path.EndsWith(".gz", StringComparison.Ordinal) => new GZipStream(buffer, CompressionLevel.Fastest, true),
                   _ when path.EndsWith(".br", StringComparison.Ordinal) => new BrotliStream(buffer, CompressionLevel.Fastest, true),
                   _ => new ZLibStream(buffer, CompressionLevel.Fastest, true),
               })
        {
            compressor.Write(Encoding.UTF8.GetBytes(text));
        }

        return buffer.ToArray();
    }

    private static RowErrorAction Capture(RowError error, out RowError? captured)
    {
        captured = error;
        return RowErrorAction.Skip;
    }

    private void Put(string path, string content) => _provider.Put(InMemoryStorageProvider.Uri(path), Encoding.UTF8.GetBytes(content));

    private Task<List<string>> Read(string path, RowErrorHandler? handler = null, bool recursive = false) =>
        NodeRunner(new LineSource(new LineSourceOptions { Uri = InMemoryStorageProvider.Uri(path), Provider = _provider, RowErrorHandler = handler, Recursive = recursive }));

    private static async Task<List<string>> NodeRunner(LineSource source)
    {
        var rows = new List<string>();

        await foreach (var row in source.OpenStream(new PipelineContext(), CancellationToken.None))
        {
            rows.Add(row);
        }

        return rows;
    }

    private sealed class SingleSourcePipeline(LineSource source, IDeadLetterSink deadLetters) : IPipelineDefinition
    {
        public void Define(PipelineBuilder builder, PipelineContext context)
        {
            var handle = builder.AddSource(source, "lines");
            builder.Connect(handle, builder.AddInMemorySink<string>(context));
            builder.AddDeadLetterSink(deadLetters);
        }
    }

    private sealed class CapturingDeadLetterSink : IDeadLetterSink
    {
        public List<DeadLetterEnvelope> Captured { get; } = [];

        public Task HandleAsync(DeadLetterEnvelope envelope, PipelineContext context, CancellationToken cancellationToken)
        {
            Captured.Add(envelope);
            return Task.CompletedTask;
        }
    }
}

[Collection(MetricCaptureFixture.Name)]
public sealed class FileSinkNodeTests
{
    private static readonly StorageUri Target = InMemoryStorageProvider.Uri("out/data.txt");

    private readonly InMemoryStorageProvider _provider = new();

    [Fact]
    public async Task Writes_directly_to_a_provider_that_cannot_move()
    {
        await Write(new LineSink(new LineSinkOptions { Uri = Target, Provider = _provider }), "a", "b");

        Text(Target).Should().Be("a\nb\n");
        _provider.WriteRequests.Should().Equal(Target);
    }

    [Fact]
    public async Task Writes_through_a_temporary_object_when_the_provider_can_move()
    {
        var provider = new MoveableProvider(_provider);

        await Write(new LineSink(new LineSinkOptions { Uri = Target, Provider = provider }), "a");

        Text(Target).Should().Be("a\n");
        provider.Moves.Should().ContainSingle().Which.To.Should().Be(Target);
        _provider.Keys.Should().ContainSingle();
    }

    [Fact]
    public async Task Always_copies_a_temporary_object_into_place_when_the_provider_cannot_move()
    {
        await Write(new LineSink(new LineSinkOptions { Uri = Target, Provider = _provider, AtomicWrite = AtomicWrite.Always }), "a");

        Text(Target).Should().Be("a\n");
        _provider.WriteRequests.Should().HaveCount(2);
        _provider.Keys.Should().ContainSingle("the temporary object is deleted");
    }

    [Fact]
    public async Task Never_writes_directly_even_when_the_provider_can_move()
    {
        var provider = new MoveableProvider(_provider);

        await Write(new LineSink(new LineSinkOptions { Uri = Target, Provider = provider, AtomicWrite = AtomicWrite.Never }), "a");

        provider.Moves.Should().BeEmpty();
        _provider.WriteRequests.Should().Equal(Target);
    }

    [Fact]
    public async Task The_temporary_object_keeps_the_uri_parameters()
    {
        var target = StorageUri.Parse("mem://test/out/data.txt?region=ap-southeast-2");

        await Write(new LineSink(new LineSinkOptions { Uri = target, Provider = _provider, AtomicWrite = AtomicWrite.Always }), "a");

        _provider.WriteRequests.Should().AllSatisfy(uri => uri.Parameters.Should().Contain("region", "ap-southeast-2"));
    }

    [Theory]
    [InlineData(AtomicWrite.Never)]
    [InlineData(AtomicWrite.Always)]
    public async Task A_failed_write_leaves_nothing_behind(AtomicWrite atomicWrite)
    {
        var write = () => Write(new LineSink(new LineSinkOptions { Uri = Target, Provider = _provider, AtomicWrite = atomicWrite }), "a", "explode");

        await write.Should().ThrowAsync<InvalidOperationException>();
        _provider.Keys.Should().BeEmpty();
    }

    [Fact]
    public async Task A_failed_direct_write_can_keep_its_partial_output()
    {
        var write = () => Write(new LineSink(new LineSinkOptions { Uri = Target, Provider = _provider, AtomicWrite = AtomicWrite.Never, DeletePartialOnFailure = false }), "a", "explode");

        await write.Should().ThrowAsync<InvalidOperationException>();
        _provider.Keys.Should().ContainSingle();
    }

    [Fact]
    public async Task Compresses_by_suffix()
    {
        var target = InMemoryStorageProvider.Uri("out/data.txt.gz");

        await Write(new LineSink(new LineSinkOptions { Uri = target, Provider = _provider }), "a", "b");

        using var gzip = new GZipStream(new MemoryStream(_provider.Get(target)), CompressionMode.Decompress);
        new StreamReader(gzip).ReadToEnd().Should().Be("a\nb\n");
    }

    [Fact]
    public async Task Gives_formats_that_need_one_a_seekable_stream_over_a_non_seekable_target()
    {
        var provider = new InMemoryStorageProvider { NonSeekableWrites = true };
        var sink = new LineSink(new LineSinkOptions { Uri = Target, Provider = provider }) { NeedsSeekableStream = true };

        await Write(sink, "a");

        sink.SawSeekableStream.Should().BeTrue();
        Encoding.UTF8.GetString(provider.Get(Target)).Should().Be("a\n");
    }

    [Fact]
    public async Task Null_items_fail_by_default_and_can_be_skipped_or_written()
    {
        var fail = () => Write(new LineSink(new LineSinkOptions { Uri = Target, Provider = _provider }), "a", null);
        var unsupported = () => Write(new LineSink(new LineSinkOptions { Uri = Target, Provider = _provider, NullItems = NullItemHandling.Write }), "a");

        await fail.Should().ThrowAsync<InvalidOperationException>().WithMessage("*null item at position 2*");
        await unsupported.Should().ThrowAsync<NotSupportedException>();

        await Write(new LineSink(new LineSinkOptions { Uri = Target, Provider = _provider, NullItems = NullItemHandling.Skip }), "a", null, "b");
        Text(Target).Should().Be("a\nb\n");

        await Write(new LineSink(new LineSinkOptions { Uri = Target, Provider = _provider, NullItems = NullItemHandling.Write }) { CanWriteNulls = true }, "a", null);
        Text(Target).Should().Be("a\n<null>\n");
    }

    [Fact]
    public async Task Reports_rows_bytes_and_files()
    {
        using var metrics = new MetricCapture();

        await Write(new LineSink(new LineSinkOptions { Uri = Target, Provider = _provider }), "a", "b");

        metrics.Total("npipeline.connector.rows_written").Should().Be(2);
        metrics.Total("npipeline.connector.bytes_written").Should().Be(4);
        metrics.Total("npipeline.connector.files_written").Should().Be(1);
    }

    private static async Task Write(LineSink sink, params string?[] items)
    {
        await using var input = new DataStream<string?>(items.ToAsyncEnumerableCompat(), "items");
        await sink.ConsumeAsync(input, new PipelineContext(), CancellationToken.None);
    }

    private string Text(StorageUri uri) => Encoding.UTF8.GetString(_provider.Get(uri));
}

/// <summary>
/// Meter listeners see measurements from the whole process, so tests that assert on metrics run with
/// no other test in flight.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class MetricCaptureFixture
{
    public const string Name = "Connector metrics";
}

/// <summary>Collects connector measurements published while it is alive.</summary>
public sealed class MetricCapture : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly List<(string Instrument, long Value, string[] Tags)> _measurements = [];

    public MetricCapture()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == ConnectorDiagnostics.Name)
                listener.EnableMeasurementEvents(instrument);
        };

        _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            lock (_measurements)
            {
                _measurements.Add((instrument.Name, value, [.. tags.ToArray().Select(t => t.Key)]));
            }
        });

        _listener.Start();
    }

    public long Total(string instrument)
    {
        lock (_measurements)
        {
            return _measurements.Where(m => m.Instrument == instrument).Sum(m => m.Value);
        }
    }

    public string[] TagKeys(string instrument)
    {
        lock (_measurements)
        {
            return _measurements.First(m => m.Instrument == instrument).Tags;
        }
    }

    public void Dispose() => _listener.Dispose();
}

internal static class AsyncEnumerableCompat
{
    /// <summary>The items as an async sequence, without System.Linq.Async (not available on every target).</summary>
    public static async IAsyncEnumerable<T> ToAsyncEnumerableCompat<T>(this IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            await Task.Yield();
            yield return item;
        }
    }
}
