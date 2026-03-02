using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.S3.Compatible.Tests;

public class ServiceCollectionExtensionsTests
{
    private static S3CompatibleStorageProviderOptions CreateValidOptions()
    {
        return new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
            AccessKey = "test-access-key",
            SecretKey = "test-secret-key",
        };
    }

    // ── Null guards ───────────────────────────────────────────────────────

    [Fact]
    public void AddS3CompatibleStorageProvider_WithNullServices_ThrowsArgumentNullException()
    {
        IServiceCollection? services = null;
        var exception = Assert.Throws<ArgumentNullException>(() => services!.AddS3CompatibleStorageProvider(CreateValidOptions()));
        exception.ParamName.Should().Be("services");
    }

    [Fact]
    public void AddS3CompatibleStorageProvider_WithNullOptions_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();
        var exception = Assert.Throws<ArgumentNullException>(() => services.AddS3CompatibleStorageProvider(null!));
        exception.ParamName.Should().Be("options");
    }

    // ── Validation ────────────────────────────────────────────────────────

    [Fact]
    public void AddS3CompatibleStorageProvider_WithEmptyAccessKey_ThrowsArgumentException()
    {
        var services = new ServiceCollection();

        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
            AccessKey = "   ",
            SecretKey = "secret",
        };

        var exception = Assert.Throws<ArgumentException>(() => services.AddS3CompatibleStorageProvider(options));
        exception.Message.Should().Contain("AccessKey");
    }

    [Fact]
    public void AddS3CompatibleStorageProvider_WithEmptySecretKey_ThrowsArgumentException()
    {
        var services = new ServiceCollection();

        var options = new S3CompatibleStorageProviderOptions
        {
            ServiceUrl = new Uri("http://localhost:9000"),
            AccessKey = "key",
            SecretKey = "   ",
        };

        var exception = Assert.Throws<ArgumentException>(() => services.AddS3CompatibleStorageProvider(options));
        exception.Message.Should().Contain("SecretKey");
    }

    // ── DI registration ───────────────────────────────────────────────────

    [Fact]
    public void AddS3CompatibleStorageProvider_WithValidOptions_RegistersStorageProvider()
    {
        var services = new ServiceCollection();

        services.AddS3CompatibleStorageProvider(CreateValidOptions());

        var sp = services.BuildServiceProvider();
        var provider = sp.GetService<S3CompatibleStorageProvider>();
        provider.Should().NotBeNull();
    }

    [Fact]
    public void AddS3CompatibleStorageProvider_WithValidOptions_RegistersClientFactory()
    {
        var services = new ServiceCollection();

        services.AddS3CompatibleStorageProvider(CreateValidOptions());

        var sp = services.BuildServiceProvider();
        var factory = sp.GetService<S3CompatibleClientFactory>();
        factory.Should().NotBeNull();
    }

    [Fact]
    public void AddS3CompatibleStorageProvider_WithValidOptions_RegistersOptions()
    {
        var services = new ServiceCollection();
        var options = CreateValidOptions();

        services.AddS3CompatibleStorageProvider(options);

        var sp = services.BuildServiceProvider();
        var resolved = sp.GetRequiredService<S3CompatibleStorageProviderOptions>();
        resolved.Should().BeSameAs(options);
    }

    [Fact]
    public void AddS3CompatibleStorageProvider_StorageProviderIsSingleton()
    {
        var services = new ServiceCollection();
        services.AddS3CompatibleStorageProvider(CreateValidOptions());

        var sp = services.BuildServiceProvider();
        var instance1 = sp.GetRequiredService<S3CompatibleStorageProvider>();
        var instance2 = sp.GetRequiredService<S3CompatibleStorageProvider>();

        instance1.Should().BeSameAs(instance2);
    }

    [Fact]
    public void AddS3CompatibleStorageProvider_ClientFactoryIsSingleton()
    {
        var services = new ServiceCollection();
        services.AddS3CompatibleStorageProvider(CreateValidOptions());

        var sp = services.BuildServiceProvider();
        var instance1 = sp.GetRequiredService<S3CompatibleClientFactory>();
        var instance2 = sp.GetRequiredService<S3CompatibleClientFactory>();

        instance1.Should().BeSameAs(instance2);
    }

    [Fact]
    public void AddS3CompatibleStorageProvider_ReturnsServiceCollectionForChaining()
    {
        var services = new ServiceCollection();

        var result = services.AddS3CompatibleStorageProvider(CreateValidOptions());

        result.Should().BeSameAs(services);
    }

    // ── Provider functionality after DI ───────────────────────────────────

    [Fact]
    public void AddS3CompatibleStorageProvider_ProviderCanHandleS3Uris()
    {
        var services = new ServiceCollection();
        services.AddS3CompatibleStorageProvider(CreateValidOptions());

        var sp = services.BuildServiceProvider();
        var provider = sp.GetRequiredService<S3CompatibleStorageProvider>();

        provider.CanHandle(StorageUri.Parse("s3://bucket/key"))
            .Should().BeTrue();
    }
}
