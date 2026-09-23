using NPipeline.Connectors.DataLake.Manifest;
using NPipeline.Connectors.DataLake.Reliability;
using NPipeline.StorageProviders;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using NResilience;

namespace NPipeline.Connectors.DataLake.Tests.Reliability.Behavior;

public sealed class ManifestWriterResilienceBehaviorTests : IDisposable
{
    // Same attempts and classifier as the preset, without the backoff delay
    private static readonly NResilience.Resilience Fast = DataLakeConnectorResilience.ManifestWrite with
    {
        Backoff = Backoff.None,
    };

    private readonly FaultyProvider _provider = new();
    private readonly StorageUri _tableUri;
    private readonly string _tempDir;

    public ManifestWriterResilienceBehaviorTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"datalake_resilience_{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(_tempDir);
        _tableUri = StorageUri.FromFilePath(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, true);
    }

    [Fact]
    public void ManifestWrite_PreservesThreeAttemptsFrom100Milliseconds_WithJitter()
    {
        var policy = DataLakeConnectorResilience.ManifestWrite;

        policy.Attempts.Should().Be(3);
        policy.Backoff.Kind.Should().Be(BackoffKind.Exponential);
        policy.Backoff.TransientBase.Should().Be(TimeSpan.FromMilliseconds(100));
        policy.Backoff.Jitter.Should().Be(Jitter.Full);
        policy.AttemptTimeout.Should().Be(Timeout.InfiniteTimeSpan);
        policy.Deadline.Should().Be(Timeout.InfiniteTimeSpan);
        policy.Adaptive.Should().BeFalse();
        policy.Classifier.Should().BeSameAs(DataLakeConnectorResilience.ManifestClassifier);
        policy.Validate();
    }

    [Fact]
    public void ManifestClassifier_ClassifiesStorageExceptions()
    {
        (Exception Exception, VerdictKind Expected)[] cases =
        [
            (new IOException("transient storage error"), VerdictKind.Transient),
            (new TimeoutException(), VerdictKind.Transient),
            (new UnauthorizedAccessException("403"), VerdictKind.Permanent),
            (new FileNotFoundException("404"), VerdictKind.Permanent),
            (new DirectoryNotFoundException(), VerdictKind.Permanent),
            (new ArgumentException("400"), VerdictKind.Permanent),
            (new InvalidOperationException(), VerdictKind.Permanent),
            (new AggregateException(new UnauthorizedAccessException(), new IOException()), VerdictKind.Transient),
            (new AggregateException(new UnauthorizedAccessException(), new FileNotFoundException()), VerdictKind.Permanent),
        ];

        foreach (var (exception, expected) in cases)
        {
            DataLakeConnectorResilience.ManifestClassifier.ClassifyException(exception).Kind
                .Should().Be(expected, exception.GetType().Name);
        }
    }

    [Fact]
    public async Task FlushAsync_TransientMoveFailure_RetriesAndAppendsOnce()
    {
        await SeedManifestAsync();
        _provider.MoveFailures.Enqueue(new IOException("blip"));

        await WriteAsync("b.parquet");

        _provider.MoveCalls.Should().Be(2);
        (await ReadPathsAsync()).Should().Equal("a.parquet", "b.parquet");
    }

    [Fact]
    public async Task FlushAsync_PersistentTransientFailure_StopsAfterThreeAttempts()
    {
        await SeedManifestAsync();

        for (var i = 0; i < 5; i++)
        {
            _provider.MoveFailures.Enqueue(new IOException("down"));
        }

        var act = () => WriteAsync("b.parquet");

        var thrown = await act.Should().ThrowAsync<IOException>();
        thrown.Which.Data.Contains("NResilience.Attempts").Should().BeTrue();
        _provider.MoveCalls.Should().Be(3);
        (await ReadPathsAsync()).Should().Equal("a.parquet");
    }

    [Fact]
    public async Task FlushAsync_AccessDenied_IsNotRetried()
    {
        await SeedManifestAsync();
        _provider.MoveFailures.Enqueue(new UnauthorizedAccessException("403"));

        var act = () => WriteAsync("b.parquet");

        _ = await act.Should().ThrowAsync<UnauthorizedAccessException>();
        _provider.MoveCalls.Should().Be(1);
    }

    [Fact]
    public async Task FlushAsync_AttemptCommittedBeforeFailing_DoesNotDuplicateEntries()
    {
        await SeedManifestAsync();
        _provider.FailAfterMove = new IOException("response lost after the rename committed");

        await WriteAsync("b.parquet");

        _provider.MoveCalls.Should().Be(1, "the retry saw its entries already present and skipped the rename");
        (await ReadPathsAsync()).Should().Equal("a.parquet", "b.parquet");
    }

    [Fact]
    public async Task FlushAsync_MetadataLookupFails_RetriesInsteadOfOverwritingManifest()
    {
        await SeedManifestAsync();
        _provider.MetadataFailures.Enqueue(new IOException("blip"));

        await WriteAsync("b.parquet");

        (await ReadPathsAsync()).Should().Equal("a.parquet", "b.parquet");
    }

    [Fact]
    public async Task FlushAsync_CallerCancels_StopsWithoutRetrying()
    {
        await SeedManifestAsync();
        using var cts = new CancellationTokenSource();

        _provider.OnMove = () =>
        {
            cts.Cancel();
            throw new IOException("blip while cancelling");
        };

        var act = () => WriteAsync("b.parquet", cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        _provider.MoveCalls.Should().Be(1);
    }

    [Fact]
    public async Task FlushAsync_ForeignCancellation_IsAFailureAndNotRetried()
    {
        await SeedManifestAsync();
        _provider.MoveFailures.Enqueue(new OperationCanceledException("not the caller's token"));

        var act = () => WriteAsync("b.parquet");

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        _provider.MoveCalls.Should().Be(1);
        (await ReadPathsAsync()).Should().Equal("a.parquet");
    }

    private async Task SeedManifestAsync()
    {
        // The first flush creates the main manifest directly; later flushes take the temp-file-and-rename path
        await WriteAsync("a.parquet");
        _provider.MoveCalls = 0;
    }

    private async Task WriteAsync(string path, CancellationToken cancellationToken = default)
    {
        var writer = new ManifestWriter(_provider, _tableUri, ManifestWriter.GenerateSnapshotId(), Fast);

        writer.Append(new ManifestEntry
        {
            Path = path,
            RowCount = 1,
            WrittenAt = DateTimeOffset.UtcNow,
            FileSizeBytes = 1,
            SnapshotId = writer.SnapshotId,
        });

        await writer.FlushAsync(cancellationToken);
    }

    private async Task<IReadOnlyList<string>> ReadPathsAsync()
    {
        var content = await File.ReadAllTextAsync(Path.Combine(_tempDir, "_manifest", "manifest.ndjson"));

        return content.Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(line => System.Text.Json.JsonDocument.Parse(line).RootElement.GetProperty("path").GetString()!)
            .ToList();
    }

    private sealed class FaultyProvider : IStorageProvider, IMoveableStorageProvider
    {
        private readonly FileSystemStorageProvider _inner = new();

        public Queue<Exception> MoveFailures { get; } = new();

        public Queue<Exception> MetadataFailures { get; } = new();

        public Exception? FailAfterMove { get; set; }

        public Action? OnMove { get; set; }

        public int MoveCalls { get; set; }

        public async Task MoveAsync(StorageUri sourceUri, StorageUri destinationUri, CancellationToken cancellationToken = default)
        {
            MoveCalls++;
            OnMove?.Invoke();

            if (MoveFailures.TryDequeue(out var failure))
                throw failure;

            await _inner.MoveAsync(sourceUri, destinationUri, cancellationToken);

            if (FailAfterMove is { } after)
            {
                FailAfterMove = null;
                throw after;
            }
        }

        public StorageScheme Scheme => _inner.Scheme;

        public bool CanHandle(StorageUri uri) => _inner.CanHandle(uri);

        public Task<Stream> OpenReadAsync(StorageUri uri, CancellationToken cancellationToken = default) =>
            _inner.OpenReadAsync(uri, cancellationToken);

        public Task<Stream> OpenWriteAsync(StorageUri uri, CancellationToken cancellationToken = default) =>
            _inner.OpenWriteAsync(uri, cancellationToken);

        public Task<bool> ExistsAsync(StorageUri uri, CancellationToken cancellationToken = default) =>
            _inner.ExistsAsync(uri, cancellationToken);

        public IAsyncEnumerable<StorageItem> ListAsync(
            StorageUri prefix,
            bool recursive = false,
            CancellationToken cancellationToken = default) =>
            _inner.ListAsync(prefix, recursive, cancellationToken);

        public Task<StorageMetadata?> GetMetadataAsync(StorageUri uri, CancellationToken cancellationToken = default)
        {
            return MetadataFailures.TryDequeue(out var failure)
                ? Task.FromException<StorageMetadata?>(failure)
                : _inner.GetMetadataAsync(uri, cancellationToken);
        }
    }
}
