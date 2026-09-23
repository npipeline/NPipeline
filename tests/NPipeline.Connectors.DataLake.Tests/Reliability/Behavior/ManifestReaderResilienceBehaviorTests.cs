using System.Runtime.CompilerServices;
using System.Text.Json;
using NPipeline.Connectors.DataLake.Manifest;
using NPipeline.Connectors.DataLake.Reliability;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using NResilience;

namespace NPipeline.Connectors.DataLake.Tests.Reliability.Behavior;

public sealed class ManifestReaderResilienceBehaviorTests : IDisposable
{
    // Same attempts and classifier as the preset, without the backoff delay
    private static readonly NResilience.Resilience Fast = DataLakeConnectorResilience.ManifestRead with
    {
        Backoff = Backoff.None,
    };

    private static readonly JsonSerializerOptions JsonOptions = new() { PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower };

    private readonly FaultyProvider _provider = new();
    private readonly StorageUri _tableUri;
    private readonly string _tempDir;

    public ManifestReaderResilienceBehaviorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"datalake_reader_{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(_tempDir);
        _tableUri = StorageUri.FromFilePath(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Fact]
    public void ManifestRead_UsesTheManifestAttemptsAndClassifier()
    {
        var policy = DataLakeConnectorResilience.ManifestRead;

        policy.Attempts.Should().Be(3);
        policy.Backoff.TransientBase.Should().Be(TimeSpan.FromMilliseconds(100));
        policy.Classifier.Should().BeSameAs(DataLakeConnectorResilience.ManifestClassifier);
        policy.Adaptive.Should().BeFalse();
    }

    [Fact]
    public async Task ReadAllAsync_TransientSnapshotReadFailure_IsRetriedAndSucceeds()
    {
        WriteMain("a.parquet");
        WriteSnapshot("s1", "b.parquet");
        _provider.SnapshotReadFailures.Enqueue(new IOException("blip"));

        var entries = await Reader().ReadAllAsync();

        entries.Select(e => e.Path).Should().BeEquivalentTo("a.parquet", "b.parquet");
        _provider.SnapshotReads.Should().Be(2);
    }

    [Fact]
    public async Task ReadAllAsync_PersistentSnapshotReadFailure_Surfaces()
    {
        WriteMain("a.parquet");
        WriteSnapshot("s1", "b.parquet");

        for (var i = 0; i < 5; i++)
        {
            _provider.SnapshotReadFailures.Enqueue(new IOException("down"));
        }

        var act = () => Reader().ReadAllAsync();

        var thrown = await act.Should().ThrowAsync<IOException>();
        thrown.Which.Data.Contains("NResilience.Attempts").Should().BeTrue();
        _provider.SnapshotReads.Should().Be(3);
    }

    [Fact]
    public async Task ReadAllAsync_PersistentListingFailure_Surfaces()
    {
        WriteMain("a.parquet");
        WriteSnapshot("s1", "b.parquet");

        for (var i = 0; i < 5; i++)
        {
            _provider.ListFailures.Enqueue(new IOException("down"));
        }

        var act = () => Reader().ReadAllAsync();

        _ = await act.Should().ThrowAsync<IOException>();
        _provider.ListCalls.Should().Be(3);
    }

    [Fact]
    public async Task ReadAllAsync_ListingAccessDenied_SurfacesWithoutRetry()
    {
        WriteMain("a.parquet");
        _provider.ListFailures.Enqueue(new UnauthorizedAccessException("403"));

        var act = () => Reader().ReadAllAsync();

        _ = await act.Should().ThrowAsync<UnauthorizedAccessException>();
        _provider.ListCalls.Should().Be(1);
    }

    [Fact]
    public async Task ReadAllAsync_NoSnapshotDirectory_ReturnsMainManifestEntries()
    {
        WriteMain("a.parquet", "b.parquet");
        Directory.Exists(Path.Combine(_tempDir, "_manifest", "snapshots")).Should().BeFalse();

        var entries = await Reader().ReadAllAsync();

        entries.Select(e => e.Path).Should().BeEquivalentTo("a.parquet", "b.parquet");
    }

    [Fact]
    public async Task ReadAllAsync_ListingReportsDirectoryNotFound_ReturnsMainManifestEntries()
    {
        WriteMain("a.parquet");
        _provider.ListFailures.Enqueue(new DirectoryNotFoundException());

        var entries = await Reader().ReadAllAsync();

        entries.Select(e => e.Path).Should().Equal("a.parquet");
        _provider.ListCalls.Should().Be(1);
    }

    [Fact]
    public async Task ReadAllAsync_NoManifestAtAll_ReturnsEmpty()
    {
        (await Reader().ReadAllAsync()).Should().BeEmpty();
        (await Reader().ExistsAsync()).Should().BeFalse();
    }

    [Fact]
    public async Task ReadAllAsync_MainManifestMetadataFailure_SurfacesInsteadOfReadingAsEmpty()
    {
        WriteMain("a.parquet");

        for (var i = 0; i < 5; i++)
        {
            _provider.MetadataFailures.Enqueue(new IOException("down"));
        }

        var act = () => Reader().ReadAllAsync();

        _ = await act.Should().ThrowAsync<IOException>();
    }

    private ManifestReader Reader() => new(_provider, _tableUri, Fast);

    private void WriteMain(params string[] paths)
    {
        WriteManifestFile(Path.Combine(_tempDir, "_manifest", "manifest.ndjson"), "main", paths);
    }

    private void WriteSnapshot(string snapshotId, params string[] paths)
    {
        WriteManifestFile(Path.Combine(_tempDir, "_manifest", "snapshots", $"{snapshotId}.ndjson"), snapshotId, paths);
    }

    private static void WriteManifestFile(string file, string snapshotId, string[] paths)
    {
        _ = Directory.CreateDirectory(Path.GetDirectoryName(file)!);

        var lines = paths.Select(path => JsonSerializer.Serialize(new ManifestEntry
        {
            Path = path,
            RowCount = 1,
            WrittenAt = DateTimeOffset.UtcNow,
            FileSizeBytes = 1,
            SnapshotId = snapshotId,
        }, JsonOptions));

        File.WriteAllText(file, string.Join('\n', lines));
    }

    private sealed class FaultyProvider : IStorageProvider
    {
        private readonly FileSystemStorageProvider _inner = new();

        public Queue<Exception> SnapshotReadFailures { get; } = new();

        public Queue<Exception> ListFailures { get; } = new();

        public Queue<Exception> MetadataFailures { get; } = new();

        public int SnapshotReads { get; private set; }

        public int ListCalls { get; private set; }

        public StorageScheme Scheme => _inner.Scheme;

        public bool CanHandle(StorageUri uri) => _inner.CanHandle(uri);

        public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            if (uri.Path?.Contains("/_manifest/snapshots/", StringComparison.Ordinal) == true)
            {
                SnapshotReads++;

                if (SnapshotReadFailures.TryDequeue(out var failure))
                    return Task.FromException<Stream>(failure);
            }

            return _inner.OpenReadAsync(uri, cancellationToken);
        }

        public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default) =>
            _inner.OpenWriteAsync(uri, cancellationToken);

        public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default) =>
            _inner.ExistsAsync(uri, cancellationToken);

        public async IAsyncEnumerable<StorageItem> ListAsync(
            StorageUri prefix,
            bool recursive = false,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ListCalls++;

            if (ListFailures.TryDequeue(out var failure))
                throw failure;

            await foreach (var item in _inner.ListAsync(prefix, recursive, cancellationToken))
            {
                yield return item;
            }
        }

        public Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            return MetadataFailures.TryDequeue(out var failure)
                ? Task.FromException<StorageMetadata?>(failure)
                : _inner.GetMetadataAsync(uri, cancellationToken);
        }
    }
}
