using AwesomeAssertions;
using Microsoft.Extensions.DependencyInjection;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.DependencyInjection;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.S3.Aws.Tests;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddAwsS3StorageProvider_WithNullServices_ThrowsArgumentNullException()
    {
        IServiceCollection? services = null;

        var ex = Assert.Throws<ArgumentNullException>(() => services!.AddAwsS3StorageProvider());
        ex.ParamName.Should().Be("services");
    }

    [Fact]
    public void AddAwsS3StorageProvider_WithNullOptions_ThrowsArgumentNullException()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => new ServiceCollection().AddAwsS3StorageProvider((AwsS3StorageProviderOptions)null!));
        ex.ParamName.Should().Be("options");
    }

    [Fact]
    public async Task AddAwsS3StorageProvider_Twice_RegistersOneProvider()
    {
        var services = new ServiceCollection();

        services.AddAwsS3StorageProvider();
        services.AddAwsS3StorageProvider(new AwsS3StorageProviderOptions());

        await using var sp = services.BuildServiceProvider();
        sp.GetServices<IStorageProvider>().OfType<AwsS3StorageProvider>().Should().ContainSingle();
    }

    [Fact]
    public async Task AddAwsS3StorageProvider_ConfigureAction_AppliesOptions()
    {
        var services = new ServiceCollection();

        services.AddAwsS3StorageProvider(o => o.MaxConcurrency = 123);

        await using var sp = services.BuildServiceProvider();
        sp.GetRequiredService<AwsS3StorageProviderOptions>().MaxConcurrency.Should().Be(123);
    }

    [Fact]
    public async Task AddAwsS3StorageProvider_ProviderIsResolvableThroughIStorageResolver()
    {
        var services = new ServiceCollection();
        services.AddStorageResolver(false);
        services.AddAwsS3StorageProvider();

        await using var sp = services.BuildServiceProvider();
        var resolver = sp.GetRequiredService<IStorageResolver>();

        resolver.Resolve(StorageUri.Parse("s3://bucket/key")).Should().BeOfType<AwsS3StorageProvider>();
    }
}
