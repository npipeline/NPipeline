using System.Collections.Concurrent;
using Azure.Storage;
using Azure.Storage.Blobs;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Azure.Tests;

public sealed class AzuriteFixture : IAsyncLifetime
{
    public const string AccountName = "devstoreaccount1";
    public const string AccountKey =
        "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    public const int BlobPort = 10000;

    private readonly ConcurrentBag<string> _testContainers = new();
    private IContainer? _azuriteContainer;
    private int _blobHostPort;

    public AzureBlobStorageProviderOptions Options { get; private set; } = null!;
    public AzureBlobStorageProvider Provider { get; private set; } = null!;
    public BlobServiceClient BlobServiceClient { get; private set; } = null!;

    public void TrackContainer(string containerName)
    {
        _testContainers.Add(containerName);
    }

    public string GetConnectionString()
    {
        return
            $"DefaultEndpointsProtocol=http;AccountName={AccountName};AccountKey={AccountKey};BlobEndpoint=http://127.0.0.1:{_blobHostPort}/{AccountName}/;";
    }

    public async Task InitializeAsync()
    {
        _azuriteContainer = new ContainerBuilder("mcr.microsoft.com/azure-storage/azurite")
            .WithPortBinding(BlobPort, true)
            .WithCommand(
                "azurite-blob",
                "--blobHost",
                "0.0.0.0",
                "--blobPort",
                BlobPort.ToString())
            .WithReuse(false)
            .WithLabel("npipeline-test", "azurite-integration")
            .Build();

        using var startCts = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        await _azuriteContainer.StartAsync(startCts.Token);

        _blobHostPort = _azuriteContainer.GetMappedPublicPort(BlobPort);

        Options = new AzureBlobStorageProviderOptions
        {
            ServiceUrl = new Uri($"http://127.0.0.1:{_blobHostPort}/{AccountName}/"),
            ServiceVersion = BlobClientOptions.ServiceVersion.V2021_12_02,
            BlockBlobUploadThresholdBytes = 512 * 1024,
            UploadMaximumConcurrency = 4,
            UploadMaximumTransferSizeBytes = 256 * 1024,
            UseDefaultCredentialChain = false,
        };

        var clientFactory = new AzureBlobClientFactory(Options);
        Provider = new AzureBlobStorageProvider(clientFactory, Options);
        var clientOptions = new BlobClientOptions(Options.ServiceVersion.Value);
        var serviceUri = Options.ServiceUrl;
        var credential = new StorageSharedKeyCredential(AccountName, AccountKey);
        BlobServiceClient = new BlobServiceClient(serviceUri, credential, clientOptions);

        using var readyCts = new CancellationTokenSource(TimeSpan.FromSeconds(120));

        try
        {
            await WaitForAzuriteReadyAsync(BlobServiceClient, readyCts.Token);
        }
        catch (Exception ex)
        {
            var logs = await _azuriteContainer.GetLogsAsync().ConfigureAwait(false);
            throw new InvalidOperationException(
                $"Azurite failed to become ready on host port {_blobHostPort}. " +
                $"Container logs:\n{logs.Stdout}\n{logs.Stderr}",
                ex);
        }
    }

    private static async Task WaitForAzuriteReadyAsync(
        BlobServiceClient blobServiceClient,
        CancellationToken cancellationToken)
    {
        var deadline = DateTimeOffset.UtcNow.AddSeconds(120);

        while (DateTimeOffset.UtcNow < deadline)
        {
            try
            {
                _ = await blobServiceClient
                    .GetBlobContainerClient("health-check")
                    .CreateIfNotExistsAsync(cancellationToken: cancellationToken)
                    .ConfigureAwait(false);
                return;
            }
            catch when (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(250), cancellationToken).ConfigureAwait(false);
            }
        }

        throw new TimeoutException("Azurite did not become ready within the expected time.");
    }

    public async Task DisposeAsync()
    {
        foreach (var containerName in _testContainers)
        {
            try
            {
                var uri = StorageUri.Parse(
                    $"azure://{containerName}?accountName={AccountName}&accountKey={AccountKey}");
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                await Provider.DeleteAsync(uri, cts.Token);
            }
            catch
            {
                // Ignore cleanup errors
            }
        }

        if (_azuriteContainer != null)
        {
            await _azuriteContainer.StopAsync();
            await _azuriteContainer.DisposeAsync();
        }
    }
}
