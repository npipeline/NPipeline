using AwesomeAssertions;
using Azure.Storage.Blobs;
using Azure.Storage.Files.DataLake;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Adls.Tests;

public class AdlsGen2ClientFactoryTests
{
    private const string ConnectionString = "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net";

    private static AdlsGen2ClientFactory NewFactory(Action<AdlsGen2StorageProviderOptions>? configure = null)
    {
        var options = new AdlsGen2StorageProviderOptions { UseDefaultCredentialChain = false, DefaultSasToken = "sv=2022-11-02&sig=abc" };
        configure?.Invoke(options);

        return new AdlsGen2ClientFactory(options);
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new AdlsGen2ClientFactory(null!));
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultConnectionString_ReturnsDataLakeAndBlobClients()
    {
        var factory = NewFactory(o => o.DefaultConnectionString = ConnectionString);
        var uri = StorageUri.Parse("adls://filesystem/path/file.txt");

        var dfs = await factory.GetClientAsync(uri);
        var blob = await factory.GetBlobServiceClientAsync(uri);

        dfs.Should().BeOfType<DataLakeServiceClient>();
        blob.Should().BeAssignableTo<BlobServiceClient>();
        dfs.AccountName.Should().Be("testaccount");
        blob.AccountName.Should().Be("testaccount");
    }

    [Fact]
    public async Task GetClientAsync_WithAccountKeyOption_ReturnsClients()
    {
        var factory = NewFactory(o =>
        {
            o.DefaultSasToken = null;
            o.AccountName = "testaccount";
            o.DefaultAccountKey = "dGVzdA==";
        });

        var uri = StorageUri.Parse("adls://filesystem/path/file.txt");

        (await factory.GetClientAsync(uri)).Uri.Host.Should().Be("testaccount.dfs.core.windows.net");
        (await factory.GetBlobServiceClientAsync(uri)).Uri.Host.Should().Be("testaccount.blob.core.windows.net");
    }

    [Fact]
    public async Task GetClientAsync_WithSasTokenOption_ReturnsClient()
    {
        var factory = NewFactory();

        var client = await factory.GetClientAsync(StorageUri.Parse("adls://filesystem/path/file.txt?accountName=testaccount"));

        client.Should().BeOfType<DataLakeServiceClient>();
    }

    [Fact]
    public async Task GetClientAsync_WithServiceUrlInUri_UsesItForBothClients()
    {
        var factory = NewFactory();
        var uri = StorageUri.Parse($"adls://filesystem/path/file.txt?accountName=acct&serviceUrl={Uri.EscapeDataString("https://acct.dfs.core.windows.net")}");

        (await factory.GetClientAsync(uri)).Uri.Host.Should().Be("acct.dfs.core.windows.net");

        // The Blob API endpoint is derived from the Data Lake one.
        (await factory.GetBlobServiceClientAsync(uri)).Uri.Host.Should().Be("acct.blob.core.windows.net");
    }

    [Fact]
    public async Task GetClientAsync_WithServiceUrlOption_UsesIt()
    {
        var factory = NewFactory(o => o.ServiceUrl = new Uri("https://testaccount.dfs.core.windows.net"));

        var client = await factory.GetClientAsync(StorageUri.Parse("adls://filesystem/path/file.txt"));

        client.Uri.Host.Should().Be("testaccount.dfs.core.windows.net");
    }

    [Fact]
    public async Task GetClientAsync_SameEndpoint_ReturnsTheSameClients()
    {
        var factory = NewFactory(o => o.DefaultConnectionString = ConnectionString);

        var dfs1 = await factory.GetClientAsync(StorageUri.Parse("adls://filesystem/a.txt"));
        var dfs2 = await factory.GetClientAsync(StorageUri.Parse("adls://other/b/c.txt"));
        var blob1 = await factory.GetBlobServiceClientAsync(StorageUri.Parse("adls://filesystem/a.txt"));
        var blob2 = await factory.GetBlobServiceClientAsync(StorageUri.Parse("adls://other/b/c.txt"));

        dfs2.Should().BeSameAs(dfs1);
        blob2.Should().BeSameAs(blob1);
    }

    [Fact]
    public async Task GetClientAsync_DifferentAccountNames_ReturnDifferentClients()
    {
        var factory = NewFactory();

        var uri1 = StorageUri.Parse("adls://filesystem/a.txt?accountName=acct1");
        var uri2 = StorageUri.Parse("adls://filesystem/a.txt?accountName=acct2");

        (await factory.GetClientAsync(uri2)).Should().NotBeSameAs(await factory.GetClientAsync(uri1));
        (await factory.GetBlobServiceClientAsync(uri2)).Should().NotBeSameAs(await factory.GetBlobServiceClientAsync(uri1));
    }

    [Fact]
    public async Task GetClientAsync_DifferentServiceUrls_ReturnDifferentClients()
    {
        var factory = NewFactory();

        var uri1 = StorageUri.Parse($"adls://filesystem/a.txt?accountName=acct&serviceUrl={Uri.EscapeDataString("https://localhost:10000/acct")}");
        var uri2 = StorageUri.Parse($"adls://filesystem/a.txt?accountName=acct&serviceUrl={Uri.EscapeDataString("https://localhost:10001/acct")}");

        (await factory.GetClientAsync(uri2)).Should().NotBeSameAs(await factory.GetClientAsync(uri1));
    }

    [Fact]
    public async Task GetClientAsync_CacheIsBoundedByClientCacheSizeLimit()
    {
        var factory = NewFactory(o => o.ClientCacheSizeLimit = 1);

        var firstA = await factory.GetClientAsync(StorageUri.Parse("adls://filesystem/a.txt?accountName=acct1"));
        var firstBlobA = await factory.GetBlobServiceClientAsync(StorageUri.Parse("adls://filesystem/a.txt?accountName=acct1"));
        _ = await factory.GetClientAsync(StorageUri.Parse("adls://filesystem/a.txt?accountName=acct2"));
        _ = await factory.GetBlobServiceClientAsync(StorageUri.Parse("adls://filesystem/a.txt?accountName=acct2"));

        (await factory.GetClientAsync(StorageUri.Parse("adls://filesystem/a.txt?accountName=acct1"))).Should().NotBeSameAs(firstA);
        (await factory.GetBlobServiceClientAsync(StorageUri.Parse("adls://filesystem/a.txt?accountName=acct1"))).Should().NotBeSameAs(firstBlobA);
    }

    [Theory]
    [InlineData("connectionString", "DefaultConnectionString")]
    [InlineData("sasToken", "DefaultSasToken")]
    [InlineData("accountKey", "DefaultAccountKey")]
    public async Task GetClientAsync_UriCarryingACredential_ThrowsNamingTheOption(string parameter, string option)
    {
        var factory = NewFactory(o => o.AccountName = "acct");
        var uri = StorageUri.Parse($"adls://filesystem/path?accountName=acct&{parameter}=secret");

        var dfs = async () => await factory.GetClientAsync(uri);
        var blob = async () => await factory.GetBlobServiceClientAsync(uri);

        foreach (var act in new Func<Task>[] { dfs, blob })
        {
            var exception = (await act.Should().ThrowAsync<ArgumentException>()).Which;
            exception.Message.Should().Contain(parameter).And.Contain(option);
            exception.Message.Should().NotContain("secret");
        }
    }

    [Fact]
    public async Task GetClientAsync_WithNoCredentials_ThrowsInvalidOperationException()
    {
        var factory = new AdlsGen2ClientFactory(new AdlsGen2StorageProviderOptions { UseDefaultCredentialChain = false, AccountName = "acct" });

        await Assert.ThrowsAsync<InvalidOperationException>(() => factory.GetClientAsync(StorageUri.Parse("adls://filesystem/path/file.txt")));
    }

    [Fact]
    public async Task GetClientAsync_WithoutAccountNameOrServiceUrl_ThrowsInvalidOperationException()
    {
        var factory = NewFactory();

        await Assert.ThrowsAsync<InvalidOperationException>(() => factory.GetClientAsync(StorageUri.Parse("adls://filesystem/path/file.txt")));
    }

    [Fact]
    public async Task GetClientAsync_WithCancelledToken_Throws()
    {
        var factory = NewFactory(o => o.DefaultConnectionString = ConnectionString);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => factory.GetClientAsync(StorageUri.Parse("adls://filesystem/path/file.txt"), cts.Token));
    }

    [Fact]
    public async Task GetClientAsync_ServiceUrlWithDefaultConnectionString_Throws()
    {
        var factory = NewFactory(o => o.DefaultConnectionString = ConnectionString);
        var uri = StorageUri.Parse($"adls://filesystem/path?serviceUrl={Uri.EscapeDataString("https://override.example.com")}");

        var dfs = async () => await factory.GetClientAsync(uri);
        var blob = async () => await factory.GetBlobServiceClientAsync(uri);

        await dfs.Should().ThrowAsync<ArgumentException>();
        await blob.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task GetClientAsync_NoCredentialsAndAnonymousNotAllowed_Throws()
    {
        var factory = new AdlsGen2ClientFactory(new AdlsGen2StorageProviderOptions
        {
            ServiceUrl = new Uri("https://account.dfs.core.windows.net"),
            UseDefaultCredentialChain = false,
        });

        var act = async () => await factory.GetClientAsync(StorageUri.Parse("adls://filesystem/path"));

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*AllowAnonymousAccess*");
    }

    [Fact]
    public async Task GetClientAsync_WithAnonymousAccess_ReturnsClients()
    {
        var factory = new AdlsGen2ClientFactory(new AdlsGen2StorageProviderOptions
        {
            ServiceUrl = new Uri("https://account.dfs.core.windows.net"),
            UseDefaultCredentialChain = false,
            AllowAnonymousAccess = true,
        });

        var uri = StorageUri.Parse("adls://filesystem/path");

        (await factory.GetClientAsync(uri)).Should().NotBeNull();
        (await factory.GetBlobServiceClientAsync(uri)).Should().NotBeNull();
    }

    [Fact]
    public async Task GetBlobServiceClientAsync_SasTokenOptionWithEscapedSignature_IsSentVerbatim()
    {
        // A '%2B' in the signature must reach the wire as %2B: Azure Storage reads a '+' as a space.
        using var server = new RecordingHttpServer();
        const string sas = "sv=2022-11-02&sr=b&sp=r&sig=ab%2Bcd%2Fef%3D";

        var factory = NewFactory(o => o.DefaultSasToken = sas);

        var uri = StorageUri.Parse($"adls://filesystem/file?accountName=acct&serviceUrl={Uri.EscapeDataString(server.BaseUrl + "acct")}");

        var client = await factory.GetBlobServiceClientAsync(uri);

        try
        {
            _ = await client.GetBlobContainerClient("filesystem").GetBlobClient("file").ExistsAsync();
        }
        catch (global::Azure.RequestFailedException)
        {
            // The recording server answers 404 without an error code; only the request URL matters here.
        }

        server.RawUrls.Should().ContainSingle().Which.Should().Contain("sig=ab%2Bcd%2Fef%3D");
    }

    [Fact]
    public async Task GetClientAsync_DataLakeClient_SasTokenIsSentVerbatim()
    {
        using var server = new RecordingHttpServer();
        const string sas = "sv=2022-11-02&sr=b&sp=r&sig=ab%2Bcd%2Fef%3D";

        var factory = NewFactory(o => o.DefaultSasToken = sas);
        var uri = StorageUri.Parse($"adls://filesystem/file?accountName=acct&serviceUrl={Uri.EscapeDataString(server.BaseUrl + "acct")}");

        var client = await factory.GetClientAsync(uri);

        try
        {
            _ = await client.GetFileSystemClient("filesystem").GetFileClient("file").ExistsAsync();
        }
        catch (global::Azure.RequestFailedException)
        {
        }

        server.RawUrls.Should().ContainSingle().Which.Should().Contain("sig=ab%2Bcd%2Fef%3D");
    }

    [Fact]
    public void Dispose_CanBeCalledTwice()
    {
        var factory = NewFactory();

        factory.Dispose();
        factory.Invoking(f => f.Dispose()).Should().NotThrow();
    }
}
