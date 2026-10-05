using Amazon.Runtime;
using Amazon.S3;
using Testcontainers.Floci;
using Xunit;

namespace NPipeline.StorageProviders.S3.Compatible.Tests;

/// <summary>One Floci (S3 emulator) container for the whole test class, with a provider pointed at it.</summary>
public sealed class FlociS3Fixture : IAsyncLifetime
{
    public const string AccessKey = "test";
    public const string SecretKey = "test";

    private readonly FlociContainer _container = new FlociBuilder("floci/floci:2.1.0")
        .WithLabel("npipeline-test", "s3-compatible-integration")
        .Build();

    public S3CompatibleStorageProviderOptions Options { get; private set; } = null!;
    public S3CompatibleStorageProvider Provider { get; private set; } = null!;
    public AmazonS3Client Client { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        await _container.StartAsync();

        Options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri(_container.GetConnectionString()),
            AccessKey = AccessKey,
            SecretKey = SecretKey,
            PartSizeBytes = S3CoreOptions.MinPartSizeBytes,
        };

        Provider = new S3CompatibleStorageProvider(new S3CompatibleClientFactory(Options), Options);

        Client = new AmazonS3Client(
            new BasicAWSCredentials(AccessKey, SecretKey),
            new AmazonS3Config { ServiceURL = Options.ServiceUrl.ToString(), ForcePathStyle = true, AuthenticationRegion = Options.SigningRegion });
    }

    public async Task DisposeAsync()
    {
        Client?.Dispose();

        if (Provider is not null)
            await Provider.DisposeAsync();

        await _container.DisposeAsync();
    }

    public async Task<string> CreateBucketAsync()
    {
        var bucket = $"it-{Guid.NewGuid():N}";
        _ = await Client.PutBucketAsync(bucket);

        return bucket;
    }
}
