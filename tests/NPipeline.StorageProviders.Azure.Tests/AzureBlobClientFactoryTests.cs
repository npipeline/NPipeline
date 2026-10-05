using AwesomeAssertions;
using Azure.Core;
using Azure.Storage.Blobs;
using FakeItEasy;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Azure.Tests;

public class AzureBlobClientFactoryTests
{
    private const string ConnectionString = "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=dGVzdA==;EndpointSuffix=core.windows.net";

    private static AzureBlobClientFactory NewFactory(Action<AzureBlobStorageProviderOptions>? configure = null)
    {
        var options = new AzureBlobStorageProviderOptions { UseDefaultCredentialChain = false, DefaultSasToken = "sv=2022-11-02&sig=abc" };
        configure?.Invoke(options);

        return new AzureBlobClientFactory(options);
    }

    [Fact]
    public void Constructor_WithNullOptions_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() => new AzureBlobClientFactory(null!));
        exception.ParamName.Should().Be("options");
    }

    [Fact]
    public void Constructor_WithValidOptions_Succeeds()
    {
        new AzureBlobClientFactory(new AzureBlobStorageProviderOptions()).Should().NotBeNull();
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultConnectionString_CreatesClient()
    {
        var factory = NewFactory(o => o.DefaultConnectionString = ConnectionString);

        var client = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob"));

        client.Should().BeAssignableTo<BlobServiceClient>();
        client.AccountName.Should().Be("test");
    }

    [Fact]
    public async Task GetClientAsync_WithAccountKeyOption_CreatesClient()
    {
        var factory = NewFactory(o =>
        {
            o.DefaultSasToken = null;
            o.AccountName = "testaccount";
            o.DefaultAccountKey = "dGVzdGtleQ==";
        });

        var client = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob"));

        client.AccountName.Should().Be("testaccount");
        client.Uri.Host.Should().Be("testaccount.blob.core.windows.net");
    }

    [Fact]
    public async Task GetClientAsync_AccountKeyWithoutAccountName_Throws()
    {
        var factory = NewFactory(o =>
        {
            o.DefaultSasToken = null;
            o.DefaultAccountKey = "dGVzdGtleQ==";
            o.ServiceUrl = new Uri("https://localhost:10000/devstoreaccount1");
        });

        var act = async () => await factory.GetClientAsync(StorageUri.Parse("azure://container/blob"));

        await act.Should().ThrowAsync<ArgumentException>().WithMessage("*account name*");
    }

    [Fact]
    public async Task GetClientAsync_WithSasTokenOption_CreatesClient()
    {
        var factory = NewFactory();

        var client = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=testaccount"));

        client.AccountName.Should().Be("testaccount");
    }

    [Fact]
    public async Task GetClientAsync_WithTokenCredential_CreatesClient()
    {
        var factory = NewFactory(o =>
        {
            o.DefaultSasToken = null;
            o.DefaultCredential = A.Fake<TokenCredential>();
        });

        var client = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=testaccount"));

        client.AccountName.Should().Be("testaccount");
    }

    [Fact]
    public async Task GetClientAsync_WithDefaultCredentialChain_CreatesClient()
    {
        var factory = NewFactory(o =>
        {
            o.DefaultSasToken = null;
            o.UseDefaultCredentialChain = true;
        });

        var client = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=testaccount"));

        client.AccountName.Should().Be("testaccount");
    }

    [Fact]
    public async Task GetClientAsync_WithServiceUrlInUri_UsesIt()
    {
        var factory = NewFactory();

        var client = await factory.GetClientAsync(
            StorageUri.Parse($"azure://container/blob?serviceUrl={Uri.EscapeDataString("https://localhost:10000/devstoreaccount1")}"));

        client.Uri.Should().Be(new Uri("https://localhost:10000/devstoreaccount1"));
    }

    [Fact]
    public async Task GetClientAsync_WithServiceUrlOption_UsesIt()
    {
        var factory = NewFactory(o => o.ServiceUrl = new Uri("https://localhost:10000/devstoreaccount1"));

        var client = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob"));

        client.Uri.Should().Be(new Uri("https://localhost:10000/devstoreaccount1"));
    }

    [Fact]
    public async Task GetClientAsync_WithInvalidServiceUrl_ThrowsArgumentException()
    {
        var factory = NewFactory();

        var act = async () => await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?serviceUrl=invalid-url"));

        await act.Should().ThrowAsync<ArgumentException>();
    }

    [Fact]
    public async Task GetClientAsync_WithoutAccountNameOrServiceUrl_Throws()
    {
        var factory = NewFactory();

        var act = async () => await factory.GetClientAsync(StorageUri.Parse("azure://container/blob"));

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*Account name*");
    }

    [Fact]
    public async Task GetClientAsync_ServiceUrlWithDefaultConnectionString_Throws()
    {
        // Previously this combination silently produced an anonymous client.
        var factory = NewFactory(o => o.DefaultConnectionString = ConnectionString);

        var act = async () => await factory.GetClientAsync(
            StorageUri.Parse($"azure://container/blob?serviceUrl={Uri.EscapeDataString("https://override.example.com")}"));

        await act.Should().ThrowAsync<ArgumentException>().WithMessage("*service URL cannot be combined with a connection string*");
    }

    [Fact]
    public async Task GetClientAsync_ServiceUrlOptionWithDefaultConnectionString_Throws()
    {
        var factory = NewFactory(o =>
        {
            o.DefaultConnectionString = ConnectionString;
            o.ServiceUrl = new Uri("https://override.example.com");
        });

        var act = async () => await factory.GetClientAsync(StorageUri.Parse("azure://container/blob"));

        await act.Should().ThrowAsync<ArgumentException>().WithMessage("*service URL cannot be combined with a connection string*");
    }

    [Fact]
    public async Task GetClientAsync_NoCredentialsAndAnonymousNotAllowed_Throws()
    {
        var factory = NewFactory(o =>
        {
            o.DefaultSasToken = null;
            o.ServiceUrl = new Uri("https://publicaccount.blob.core.windows.net");
        });

        var act = async () => await factory.GetClientAsync(StorageUri.Parse("azure://container/blob"));

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("*AllowAnonymousAccess*");
    }

    [Fact]
    public async Task GetClientAsync_WithAnonymousAccess_CreatesClient()
    {
        var factory = NewFactory(o =>
        {
            o.DefaultSasToken = null;
            o.ServiceUrl = new Uri("https://publicaccount.blob.core.windows.net");
            o.AllowAnonymousAccess = true;
        });

        var client = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob"));

        client.Should().BeAssignableTo<BlobServiceClient>();
    }

    [Theory]
    [InlineData("connectionString", "DefaultConnectionString")]
    [InlineData("sasToken", "DefaultSasToken")]
    [InlineData("accountKey", "DefaultAccountKey")]
    public async Task GetClientAsync_UriCarryingACredential_ThrowsNamingTheOption(string parameter, string option)
    {
        var factory = NewFactory(o => o.AccountName = "acct");
        var uri = StorageUri.Parse($"azure://container/blob?accountName=acct&{parameter}=secret");

        var act = async () => await factory.GetClientAsync(uri);

        var exception = (await act.Should().ThrowAsync<ArgumentException>()).Which;
        exception.Message.Should().Contain(parameter).And.Contain(option);
        exception.Message.Should().NotContain("secret", "the exception must not echo the credential");
    }

    [Fact]
    public async Task GetClientAsync_SameEndpoint_ReturnsTheSameClient()
    {
        var factory = NewFactory();
        var uri = StorageUri.Parse("azure://container/blob?accountName=acct");

        var first = await factory.GetClientAsync(uri);
        var second = await factory.GetClientAsync(StorageUri.Parse("azure://other/path/x.txt?accountName=acct"));

        second.Should().BeSameAs(first);
    }

    [Fact]
    public async Task GetClientAsync_DifferentAccountNames_ReturnDifferentClients()
    {
        var factory = NewFactory();

        var first = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=acct1"));
        var second = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=acct2"));

        second.Should().NotBeSameAs(first);
        first.AccountName.Should().Be("acct1");
        second.AccountName.Should().Be("acct2");
    }

    [Fact]
    public async Task GetClientAsync_DifferentServiceUrls_ReturnDifferentClients()
    {
        var factory = NewFactory();

        var first = await factory.GetClientAsync(StorageUri.Parse($"azure://container/blob?serviceUrl={Uri.EscapeDataString("https://localhost:10000/a")}"));
        var second = await factory.GetClientAsync(StorageUri.Parse($"azure://container/blob?serviceUrl={Uri.EscapeDataString("https://localhost:10001/a")}"));

        second.Should().NotBeSameAs(first);
    }

    [Fact]
    public async Task GetClientAsync_AccountNameInUri_OverridesTheOptionDefault()
    {
        var factory = NewFactory(o => o.AccountName = "default");

        var overridden = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=other"));
        var defaulted = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob"));

        overridden.AccountName.Should().Be("other");
        defaulted.AccountName.Should().Be("default");
    }

    [Fact]
    public async Task GetClientAsync_EmptyAccountNameParameter_FallsBackToTheOption()
    {
        var factory = NewFactory(o => o.AccountName = "default");

        var client = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName="));

        client.AccountName.Should().Be("default");
    }

    [Fact]
    public async Task GetClientAsync_CacheIsBoundedByClientCacheSizeLimit()
    {
        var factory = NewFactory(o => o.ClientCacheSizeLimit = 1);

        var firstA = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=acct1"));
        var stillA = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=acct1"));
        _ = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=acct2"));
        var secondA = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=acct1"));

        stillA.Should().BeSameAs(firstA);
        secondA.Should().NotBeSameAs(firstA, "acct1 was evicted when acct2 filled the one-entry cache");
    }

    [Fact]
    public async Task GetClientAsync_CacheWithRoom_KeepsEveryEndpoint()
    {
        var factory = NewFactory(o => o.ClientCacheSizeLimit = 2);

        var firstA = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=acct1"));
        _ = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=acct2"));
        var secondA = await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=acct1"));

        secondA.Should().BeSameAs(firstA);
    }

    [Fact]
    public async Task GetClientAsync_AfterDispose_Throws()
    {
        var factory = NewFactory();
        factory.Dispose();

        var act = async () => await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=acct"));

        await act.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public async Task GetClientAsync_WithCancelledToken_Throws()
    {
        var factory = NewFactory();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var act = async () => await factory.GetClientAsync(StorageUri.Parse("azure://container/blob?accountName=acct"), cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task GetClientAsync_SasTokenOptionWithEscapedSignature_IsSentVerbatim()
    {
        // The signature's %2B must reach the wire as %2B: a '+' would be read as a space by Azure Storage and the
        // signature would not match.
        using var server = new RecordingHttpServer();
        const string sas = "sv=2022-11-02&sr=b&sp=r&sig=ab%2Bcd%2Fef%3D";

        var options = new AzureBlobStorageProviderOptions { UseDefaultCredentialChain = false, DefaultSasToken = sas };
        var provider = new AzureBlobStorageProvider(new AzureBlobClientFactory(options), options);

        var uri = StorageUri.Parse($"azure://container/blob?accountName=acct&serviceUrl={Uri.EscapeDataString(server.BaseUrl + "acct")}");

        _ = await provider.ExistsAsync(uri);

        server.RawUrls.Should().ContainSingle().Which.Should().Contain("sig=ab%2Bcd%2Fef%3D");
    }

    [Theory]
    [InlineData("sv=2022-11-02&sr=b&sp=r&sig=ab+cd/ef=")]
    [InlineData("sv=2022-11-02&sr=b&sp=r&sig=ab%2Bcd%2Fef%3D")]
    public async Task GetClientAsync_SasTokenSignature_NeverChangesOnTheWire(string sas)
    {
        using var server = new RecordingHttpServer();
        var options = new AzureBlobStorageProviderOptions { UseDefaultCredentialChain = false, DefaultSasToken = sas };
        var provider = new AzureBlobStorageProvider(new AzureBlobClientFactory(options), options);

        _ = await provider.ExistsAsync(StorageUri.Parse($"azure://container/blob?accountName=acct&serviceUrl={Uri.EscapeDataString(server.BaseUrl + "acct")}"));

        var sig = sas[(sas.IndexOf("sig=", StringComparison.Ordinal) + 4)..];
        server.RawUrls.Should().ContainSingle().Which.Should().Contain("sig=" + sig);
    }
}
