using System.Text;
using System.Text.Json;
using NPipeline.Connectors.DataLake.Manifest;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common;

namespace NPipeline.Connectors.DataLake.Tests.Reliability.Behavior;

/// <summary>
///     On a provider that declares <see cref="StorageCapabilities.ConditionalWrite" />, the main manifest is replaced only
///     if its ETag is still the one that was read, so two racing appends both land in it. These tests make a writer read
///     a stale manifest, as it would when another writer commits between its read and its write.
/// </summary>
public sealed class ManifestConditionalWriteTests
{
    private static readonly StorageUri TableUri = InMemoryStorageProvider.Uri("table");

    private static readonly StorageUri MainManifest = InMemoryStorageProvider.Uri("table/_manifest/manifest.ndjson");

    private readonly InMemoryStorageProvider _store = new();

    [Fact]
    public async Task RacingAppends_BothLandInTheMainManifest()
    {
        var provider = new StaleOnceProvider(_store);
        await FlushAsync(provider, "seed.parquet");

        // The next writer to read the manifest sees it as it is now; the writer after it replaces it first.
        provider.ServeStaleManifestOnce();
        await FlushAsync(provider, "a.parquet");
        await FlushAsync(provider, "b.parquet");

        MainManifestPaths().Should().BeEquivalentTo("seed.parquet", "a.parquet", "b.parquet");
        provider.PreconditionFailures.Should().Be(0, "the first writer read the manifest that was current");
    }

    [Fact]
    public async Task AWriterThatReadAStaleManifest_RereadsAndKeepsTheOtherWritersEntries()
    {
        var provider = new StaleOnceProvider(_store);
        await FlushAsync(provider, "seed.parquet");

        // B commits after A has read, so A's conditional write is refused and A starts again from B's manifest.
        provider.ServeStaleManifestOnce();
        var writerA = new ManifestWriter(provider, TableUri, ManifestWriter.GenerateSnapshotId());
        await using (writerA.ConfigureAwait(false))
        {
            writerA.Append(Entry("a.parquet", writerA.SnapshotId));
            provider.BeforeNextCommit = () => _store.Put(MainManifest, Combine(_store.Get(MainManifest), "b.parquet"));
            await writerA.FlushAsync();
        }

        provider.PreconditionFailures.Should().Be(1);
        MainManifestPaths().Should().Equal("seed.parquet", "b.parquet", "a.parquet");
    }

    [Fact]
    public async Task TwoWritersCreatingTheManifest_BothLandInIt()
    {
        var provider = new StaleOnceProvider(_store);

        // A thinks the manifest is missing, but B creates it first: A's create-only write is refused.
        var writerA = new ManifestWriter(provider, TableUri, ManifestWriter.GenerateSnapshotId());
        await using (writerA.ConfigureAwait(false))
        {
            writerA.Append(Entry("a.parquet", writerA.SnapshotId));
            provider.BeforeNextCommit = () => _store.Put(MainManifest, Combine([], "b.parquet"));
            await writerA.FlushAsync();
        }

        provider.PreconditionFailures.Should().Be(1);
        MainManifestPaths().Should().Equal("b.parquet", "a.parquet");
    }

    private async Task FlushAsync(IStorageProvider provider, string path)
    {
        var writer = new ManifestWriter(provider, TableUri, ManifestWriter.GenerateSnapshotId());

        await using (writer.ConfigureAwait(false))
        {
            writer.Append(Entry(path, writer.SnapshotId));
            await writer.FlushAsync();
        }
    }

    private static ManifestEntry Entry(string path, string snapshotId) => new()
    {
        Path = path,
        RowCount = 1,
        WrittenAt = DateTimeOffset.UtcNow,
        FileSizeBytes = 1,
        SnapshotId = snapshotId,
    };

    private static byte[] Combine(byte[] existing, string path)
    {
        var line = JsonSerializer.Serialize(new { path, row_count = 1, written_at = DateTimeOffset.UtcNow, file_size_bytes = 1, snapshot_id = "other" });
        var text = Encoding.UTF8.GetString(existing);
        return Encoding.UTF8.GetBytes(text.Length == 0 ? line : text.TrimEnd('\n') + "\n" + line);
    }

    private IReadOnlyList<string> MainManifestPaths() =>
        Encoding.UTF8.GetString(_store.Get(MainManifest))
            .Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(line => JsonDocument.Parse(line).RootElement.GetProperty("path").GetString()!)
            .ToList();

    /// <summary>
    ///     Wraps the in-memory store. After <see cref="ServeStaleManifestOnce" />, the next metadata and read of the main
    ///     manifest return what it held then, whatever has been written since. <see cref="BeforeNextCommit" /> runs once, when
    ///     a stream for the main manifest commits, to let "another writer" get in first.
    /// </summary>
    private sealed class StaleOnceProvider(InMemoryStorageProvider inner) : StorageProvider
    {
        private byte[]? _staleContent;
        private StorageMetadata? _staleMetadata;
        private bool _staleMetadataPending;
        private bool _staleContentPending;

        public int PreconditionFailures { get; private set; }

        public Action? BeforeNextCommit { get; set; }

        public override string Name => "Stale once";

        public override IReadOnlyList<StorageScheme> Schemes => inner.Schemes;

        public override StorageCapabilities Capabilities => inner.Capabilities;

        public void ServeStaleManifestOnce()
        {
            _staleContent = inner.Get(MainManifest);
            _staleMetadata = inner.GetMetadataAsync(MainManifest).GetAwaiter().GetResult();
            _staleMetadataPending = _staleContentPending = true;
        }

        protected override Task<Stream> OpenReadCoreAsync(StorageUri uri, CancellationToken cancellationToken)
        {
            if (_staleContentPending && uri.Equals(MainManifest))
            {
                _staleContentPending = false;
                return Task.FromResult<Stream>(new MemoryStream(_staleContent!, false));
            }

            return inner.OpenReadAsync(uri, cancellationToken);
        }

        protected override Task<StorageMetadata?> GetMetadataCoreAsync(StorageUri uri, CancellationToken cancellationToken)
        {
            if (_staleMetadataPending && uri.Equals(MainManifest))
            {
                _staleMetadataPending = false;
                return Task.FromResult(_staleMetadata);
            }

            // A manifest that does not exist yet reads as missing to a writer that looked before another created it.
            if (BeforeNextCommit is not null && uri.Equals(MainManifest) && !inner.Keys.Contains($"{uri.Host}{uri.Path}"))
                return Task.FromResult<StorageMetadata?>(null);

            return inner.GetMetadataAsync(uri, cancellationToken);
        }

        protected override async Task<StorageWriteStream> OpenWriteCoreAsync(StorageUri uri, StorageWriteOptions? options, CancellationToken cancellationToken)
        {
            var stream = await inner.OpenWriteAsync(uri, options, cancellationToken).ConfigureAwait(false);

            return uri.Equals(MainManifest) ? new Hooked(stream, this) : stream;
        }

        private sealed class Hooked(StorageWriteStream stream, StaleOnceProvider owner) : StorageWriteStream
        {
            public override bool CanRead => false;

            public override bool CanSeek => false;

            public override bool CanWrite => stream.CanWrite;

            public override long Length => stream.Length;

            public override long Position
            {
                get => stream.Position;
                set => stream.Position = value;
            }

            public override async Task CommitAsync(CancellationToken cancellationToken = default)
            {
                var hook = owner.BeforeNextCommit;
                owner.BeforeNextCommit = null;
                hook?.Invoke();

                try
                {
                    await stream.CommitAsync(cancellationToken).ConfigureAwait(false);
                    ETag = stream.ETag;
                }
                catch (NPipeline.StorageProviders.Exceptions.StoragePreconditionFailedException)
                {
                    owner.PreconditionFailures++;
                    throw;
                }
            }

            public override void Flush() => stream.Flush();

            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();

            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

            public override void SetLength(long value) => throw new NotSupportedException();

            public override void Write(byte[] buffer, int offset, int count) => stream.Write(buffer, offset, count);

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                    stream.Dispose();

                base.Dispose(disposing);
            }
        }
    }
}
