using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using Testcontainers.Azurite;
using Xunit;

namespace NPipeline.StorageProviders.Adls.Tests;

/// <summary>
///     Test container fixture for ADLS Gen2 integration tests using Azurite.
///     Uses Testcontainers.Azurite to spin up a dedicated Azurite instance per test collection run.
///     Key configuration decisions:
///     • AzuriteBuilder is used with a pinned image and the fixture pins explicit storage API
///     versions to avoid breakages when SDK defaults advance faster than Azurite support.
///     • WithInMemoryPersistence is used to avoid disk I/O during tests.
///     • Options.DefaultConnectionString is set (not ServiceUrl) so the Azure SDK correctly
///     routes DFS API calls through Azurite's blob endpoint.  Using a bare ServiceUrl causes
///     400 errors for DFS path-create operations.
/// </summary>
public sealed class AzuriteAdlsFixture : IAsyncLifetime
{
    public const string AccountName = "devstoreaccount1";

    public const string AccountKey =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    /// <summary>
    ///     Azurite image pinned for deterministic test behavior.
    /// </summary>
    private const string AzuriteImage = "mcr.microsoft.com/azure-storage/azurite:3.35.0";

    private AzuriteContainer? _azuriteContainer;

    public AdlsGen2StorageProviderOptions Options { get; private set; } = null!;
    public AdlsGen2StorageProvider Provider { get; private set; } = null!;
    public DataLakeServiceClient DataLakeServiceClient { get; private set; } = null!;
    public BlobServiceClient BlobServiceClient { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        _azuriteContainer = new AzuriteBuilder(AzuriteImage)
            .WithInMemoryPersistence()
            .Build();

        await _azuriteContainer.StartAsync();

        var connectionString = _azuriteContainer.GetConnectionString();

        // Use DefaultConnectionString so the Azure SDK routes DFS API calls through Azurite's blob
        // endpoint.  Don't set ServiceUrl — a bare ServiceUrl causes 400s for DFS path-create ops.
        Options = new AdlsGen2StorageProviderOptions
        {
            DefaultConnectionString = connectionString,
            ServiceVersion = DataLakeClientOptions.ServiceVersion.V2023_11_03,
            UploadThresholdBytes = 512 * 1024,
            UploadMaximumConcurrency = 4,
            UploadMaximumTransferSizeBytes = 256 * 1024,
            UseDefaultCredentialChain = false,
        };

        var clientFactory = new AdlsGen2ClientFactory(Options);
        Provider = new AdlsGen2StorageProvider(clientFactory, Options);

        DataLakeServiceClient = new DataLakeServiceClient(
            connectionString,
            new DataLakeClientOptions(Options.ServiceVersion!.Value));

        BlobServiceClient = new BlobServiceClient(
            connectionString,
            new BlobClientOptions(BlobClientOptions.ServiceVersion.V2021_12_02));

        using var readyCts = new CancellationTokenSource(TimeSpan.FromSeconds(120));

        try
        {
            await WaitForAzuriteReadyAsync(BlobServiceClient, readyCts.Token);
        }
        catch (Exception ex)
        {
            var logs = await _azuriteContainer.GetLogsAsync().ConfigureAwait(false);

            throw new InvalidOperationException(
                $"Azurite failed to become ready. Container logs:\n{logs.Stdout}\n{logs.Stderr}",
                ex);
        }
    }

    public async Task DisposeAsync()
    {
        if (_azuriteContainer != null)
        {
            await _azuriteContainer.StopAsync();
            await _azuriteContainer.DisposeAsync();
        }
    }

    /// <summary>Returns the Azurite connection string (dynamically mapped host ports).</summary>
    public string GetConnectionString()
    {
        return _azuriteContainer!.GetConnectionString();
    }

    /// <summary>
    ///     Returns the Azurite blob service URI parsed from the connection string.
    ///     Use this when constructing a provider that authenticates via per-URI accountKey credentials
    ///     rather than a connection string.
    /// </summary>
    public Uri GetBlobServiceUri()
    {
        var cs = GetConnectionString();

        foreach (var part in cs.Split(';'))
        {
            if (part.StartsWith("BlobEndpoint=", StringComparison.OrdinalIgnoreCase))
                return new Uri(part["BlobEndpoint=".Length..]);
        }

        throw new InvalidOperationException("BlobEndpoint not found in Azurite connection string.");
    }

    private static async Task WaitForAzuriteReadyAsync(
        BlobServiceClient blobServiceClient,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(120);

        while (DateTimeOffset.UtcNow < deadline && !cancellationToken.IsCancellationRequested)
        {
            try
            {
                _ = await blobServiceClient
                    .GetBlobContainerClient("health-check")
                    .CreateIfNotExistsAsync(cancellationToken: CancellationToken.None)
                    .ConfigureAwait(false);

                return;
            }
            catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken).ConfigureAwait(false);
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        throw new TimeoutException("Azurite did not become ready within the expected time.");
    }
}
