using AwesomeAssertions;
using Xunit;

namespace NPipeline.StorageProviders.S3.Compatible.Tests;

public class S3CompatibleStorageProviderOptionsTests
{
    private static S3CompatibleStorageProviderOptions CreateValid()
    {
        return new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
            AccessKey = "test-access-key",
            SecretKey = "test-secret-key",
        };
    }

    // ── Defaults ──────────────────────────────────────────────────────────

    [Fact]
    public void DefaultSigningRegion_IsUsEast1()
    {
        var options = CreateValid();
        options.SigningRegion.Should().Be("us-east-1");
    }

    [Fact]
    public void DefaultForcePathStyle_IsTrue()
    {
        var options = CreateValid();
        options.ForcePathStyle.Should().BeTrue();
    }

    [Fact]
    public void DefaultMultipartUploadThresholdBytes_Is64MB()
    {
        var options = CreateValid();
        options.MultipartUploadThresholdBytes.Should().Be(64 * 1024 * 1024);
    }

    // ── Required fields ───────────────────────────────────────────────────

    [Fact]
    public void ServiceUrl_IsRequiredAndRetained()
    {
        var url = new Uri("https://nyc3.digitaloceanspaces.com");

        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = url,
            AccessKey = "key",
            SecretKey = "secret",
        };

        options.ServiceUrl.Should().Be(url);
    }

    [Fact]
    public void AccessKey_IsRequiredAndRetained()
    {
        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
            AccessKey = "my-access",
            SecretKey = "my-secret",
        };

        options.AccessKey.Should().Be("my-access");
    }

    [Fact]
    public void SecretKey_IsRequiredAndRetained()
    {
        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
            AccessKey = "key",
            SecretKey = "super-secret",
        };

        options.SecretKey.Should().Be("super-secret");
    }

    // ── Optional overrides ────────────────────────────────────────────────

    [Fact]
    public void SigningRegion_CanBeOverridden()
    {
        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("https://account.r2.cloudflarestorage.com"),
            AccessKey = "key",
            SecretKey = "secret",
            SigningRegion = "auto",
        };

        options.SigningRegion.Should().Be("auto");
    }

    [Fact]
    public void ForcePathStyle_CanBeSetToFalse()
    {
        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("https://nyc3.digitaloceanspaces.com"),
            AccessKey = "key",
            SecretKey = "secret",
            ForcePathStyle = false,
        };

        options.ForcePathStyle.Should().BeFalse();
    }

    [Fact]
    public void MultipartUploadThresholdBytes_CanBeOverridden()
    {
        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
            AccessKey = "key",
            SecretKey = "secret",
            MultipartUploadThresholdBytes = 128 * 1024 * 1024,
        };

        options.MultipartUploadThresholdBytes.Should().Be(128 * 1024 * 1024);
    }

    // ── Immutability (init-only) ───────────────────────────────────────────

    [Fact]
    public void TwoInstances_AreIndependent()
    {
        var options1 = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9001"),
            AccessKey = "key1",
            SecretKey = "secret1",
        };

        var options2 = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9002"),
            AccessKey = "key2",
            SecretKey = "secret2",
        };

        options1.ServiceUrl.Should().NotBe(options2.ServiceUrl);
        options1.AccessKey.Should().NotBe(options2.AccessKey);
        options1.SecretKey.Should().NotBe(options2.SecretKey);
    }

    // ── Provider-specific scenarios ───────────────────────────────────────

    [Theory]
    [InlineData("http://localhost:9000")] // MinIO
    [InlineData("https://nyc3.digitaloceanspaces.com")] // DigitalOcean Spaces
    [InlineData("https://account.r2.cloudflarestorage.com")] // Cloudflare R2
    [InlineData("https://s3.us-central-1.wasabisys.com")] // Wasabi
    public void ServiceUrl_AcceptsVariousEndpoints(string url)
    {
        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri(url),
            AccessKey = "key",
            SecretKey = "secret",
        };

        options.ServiceUrl.Should().Be(new Uri(url));
    }

    [Theory]
    [InlineData("us-east-1")]
    [InlineData("us-west-2")]
    [InlineData("auto")] // Cloudflare R2
    [InlineData("nyc3")] // DigitalOcean Spaces
    public void SigningRegion_AcceptsCommonValues(string region)
    {
        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
            AccessKey = "key",
            SecretKey = "secret",
            SigningRegion = region,
        };

        options.SigningRegion.Should().Be(region);
    }
}
