using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using BenchmarkDotNet.Attributes;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using NPipeline.StorageProviders.S3;
using NPipeline.StorageProviders.S3.Compatible;
using Testcontainers.Floci;

namespace NPipeline.Connectors.Benchmarks.Benchmarks;

/// <summary>
///     Writes 256 MiB to an S3 emulator (Floci in Docker). <c>Spooled</c> is the pre-Phase-4 shape: spool to a local temporary
///     file, then upload the file in one go. <c>Streaming</c> is <c>S3WriteStream</c>: parts upload while the writer runs.
///     The emulator is local, so the numbers show the cost of the extra disk pass and the lost overlap, not network time.
/// </summary>
[MemoryDiagnoser]
[ShortRunJob]
#pragma warning disable CA1001 // Disposed in GlobalCleanup, the BenchmarkDotNet lifecycle.
public class ObjectStoreUploadBenchmarks
{
    private const int Size = 256 * 1024 * 1024;
    private const int Slice = 1024 * 1024;

    private readonly byte[] _slice = new byte[Slice];
    private FlociContainer _container = null!;
    private AmazonS3Client _client = null!;
    private S3CompatibleStorageProvider _provider = null!;
    private string _bucket = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        new Random(1).NextBytes(_slice);
        _container = new FlociBuilder("floci/floci:2.1.0").WithLabel("npipeline-bench", "s3-upload").Build();
        await _container.StartAsync();

        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri(_container.GetConnectionString()),
            AccessKey = "test",
            SecretKey = "test",
        };

        _provider = new S3CompatibleStorageProvider(new S3CompatibleClientFactory(options), options);
        _client = new AmazonS3Client(new BasicAWSCredentials("test", "test"), new AmazonS3Config { ServiceURL = options.ServiceUrl.ToString(), ForcePathStyle = true });
        _bucket = $"bench-{Guid.NewGuid():N}";
        _ = await _client.PutBucketAsync(_bucket);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        _client.Dispose();
        await _provider.DisposeAsync();
        await _container.DisposeAsync();
    }

    [Benchmark(Baseline = true)]
    public async Task Spooled()
    {
        var path = Path.Combine(Path.GetTempPath(), $"bench-{Guid.NewGuid():N}.tmp");

        try
        {
            await using (var file = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 81920, FileOptions.Asynchronous))
            {
                for (var written = 0; written < Size; written += Slice)
                    await file.WriteAsync(_slice);
            }

            using var transfer = new Amazon.S3.Transfer.TransferUtility(_client);
            await transfer.UploadAsync(path, _bucket, $"spooled-{Guid.NewGuid():N}");
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Benchmark]
    public async Task Streaming()
    {
        await using var stream = await _provider.OpenWriteAsync(
            StorageUri.Parse($"s3://{_bucket}/streaming-{Guid.NewGuid():N}"),
            new StorageWriteOptions { LengthHint = Size });

        for (var written = 0; written < Size; written += Slice)
            await stream.WriteAsync(_slice);

        await stream.CommitAsync();
    }
}
