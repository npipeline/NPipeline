using System.Text.Json;
using NPipeline.Connectors.DataLake.Manifest;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;

namespace NPipeline.Connectors.DataLake.Tests.Reliability.Behavior;

/// <summary>
///     The main manifest is last-writer-wins. These tests reproduce the lost update two racing appends cause - both
///     writers read the same manifest before either replaces it - and assert that <see cref="ManifestReader" /> still
///     returns every entry, recovered from the per-snapshot manifests.
/// </summary>
public sealed class ManifestConcurrencyRecoveryTests : IDisposable
{
    private readonly string _tempDir;
    private readonly StorageUri _tableUri;

    public ManifestConcurrencyRecoveryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"datalake_race_{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(_tempDir);
        _tableUri = StorageUri.FromFilePath(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Theory]
    [InlineData(true)] // temp file + atomic rename (FileSystem, ADLS)
    [InlineData(false)] // overwrite in place (S3, Azure Blob, GCS, Sftp)
    public async Task RacingAppends_LoseAnEntryFromMainManifest_ReaderRecoversIt(bool moveable)
    {
        var provider = StaleReadProvider.Create(moveable, MainManifestFile);
        await FlushAsync(provider, "seed.parquet");

        // Both writers read the manifest as it is now, before either has replaced it
        provider.FreezeMainManifestReads();
        await FlushAsync(provider, "a.parquet");
        await FlushAsync(provider, "b.parquet");
        provider.UnfreezeMainManifestReads();

        // The race really lost writer A's append from the main manifest...
        ReadMainManifestPaths().Should().Equal("seed.parquet", "b.parquet");

        // ...and the reader recovers it from A's snapshot file
        var entries = await new ManifestReader(provider, _tableUri).ReadAllAsync();
        entries.Select(e => e.Path).Should().BeEquivalentTo("seed.parquet", "a.parquet", "b.parquet");
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task WriterThatFlushesTwice_EntryLostFromFirstFlush_ReaderRecoversIt(bool moveable)
    {
        var provider = StaleReadProvider.Create(moveable, MainManifestFile);
        await FlushAsync(provider, "seed.parquet");

        await using var writerA = new ManifestWriter(provider, _tableUri, ManifestWriter.GenerateSnapshotId());

        // A's first flush races with B and loses
        provider.FreezeMainManifestReads();
        writerA.Append(Entry("a1.parquet", writerA.SnapshotId));
        await writerA.FlushAsync();
        await FlushAsync(provider, "b.parquet");
        provider.UnfreezeMainManifestReads();

        // A's second flush must not drop a1 from A's snapshot file
        writerA.Append(Entry("a2.parquet", writerA.SnapshotId));
        await writerA.FlushAsync();

        ReadMainManifestPaths().Should().Equal("seed.parquet", "b.parquet", "a2.parquet");

        var entries = await new ManifestReader(provider, _tableUri).ReadAllAsync();
        entries.Select(e => e.Path).Should().BeEquivalentTo("seed.parquet", "a1.parquet", "b.parquet", "a2.parquet");

        var snapshot = await new ManifestReader(provider, _tableUri).ReadBySnapshotAsync(writerA.SnapshotId);
        snapshot.Select(e => e.Path).Should().Equal("a1.parquet", "a2.parquet");
    }

    private async Task FlushAsync(IStorageProvider provider, string path)
    {
        await using var writer = new ManifestWriter(provider, _tableUri, ManifestWriter.GenerateSnapshotId());
        writer.Append(Entry(path, writer.SnapshotId));
        await writer.FlushAsync();
    }

    private static ManifestEntry Entry(string path, string snapshotId) => new()
    {
        Path = path,
        RowCount = 1,
        WrittenAt = DateTimeOffset.UtcNow,
        FileSizeBytes = 1,
        SnapshotId = snapshotId,
    };

    private string MainManifestFile => Path.Combine(_tempDir, "_manifest", "manifest.ndjson");

    private IReadOnlyList<string> ReadMainManifestPaths()
    {
        var content = File.ReadAllText(MainManifestFile);

        return content.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(line => JsonDocument.Parse(line).RootElement.GetProperty("path").GetString()!)
            .ToList();
    }

    /// <summary>
    ///     A file-system provider that, while frozen, serves every read of the main manifest from the content it had when
    ///     it was frozen. Writers run one after another but each sees the same stale manifest, which is exactly the
    ///     interleaving of two concurrent read-modify-write appends.
    /// </summary>
    private class StaleReadProvider : IStorageProvider
    {
        private byte[]? _frozen;
        private string _mainManifestFile = string.Empty;

        protected FileSystemStorageProvider Inner { get; } = new();

        public static StaleReadProvider Create(bool moveable, string mainManifestFile)
        {
            var provider = moveable ? new Moveable() : new StaleReadProvider();
            provider._mainManifestFile = mainManifestFile;
            return provider;
        }

        public void FreezeMainManifestReads() => _frozen = File.ReadAllBytes(_mainManifestFile);

        public void UnfreezeMainManifestReads() => _frozen = null;

        public StorageScheme Scheme => Inner.Scheme;

        public bool CanHandle(StorageUri uri) => Inner.CanHandle(uri);

        public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            return IsMainManifest(uri) && _frozen is { } frozen
                ? Task.FromResult<Stream>(new MemoryStream(frozen, false))
                : Inner.OpenReadAsync(uri, cancellationToken);
        }

        public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default) =>
            Inner.OpenWriteAsync(uri, cancellationToken);

        public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default) =>
            Inner.ExistsAsync(uri, cancellationToken);

        public IAsyncEnumerable<StorageItem> ListAsync(
            StorageUri prefix,
            bool recursive = false,
            CancellationToken cancellationToken = default) =>
            Inner.ListAsync(prefix, recursive, cancellationToken);

        public Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default) =>
            Inner.GetMetadataAsync(uri, cancellationToken);

        private static bool IsMainManifest(StorageUri uri) =>
            uri.Path?.EndsWith("/_manifest/manifest.ndjson", StringComparison.Ordinal) == true;

        private sealed class Moveable : StaleReadProvider, IMoveableStorageProvider
        {
            public Task MoveAsync(StorageUri sourceUri, StorageUri destinationUri, CancellationToken cancellationToken = default) =>
                Inner.MoveAsync(sourceUri, destinationUri, cancellationToken);
        }
    }
}
