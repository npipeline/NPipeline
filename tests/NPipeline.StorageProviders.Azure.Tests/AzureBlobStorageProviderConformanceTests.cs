using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common.Storage;
using Xunit;

namespace NPipeline.StorageProviders.Azure.Tests;

/// <summary>Runs the shared storage provider contract against Azurite.</summary>
public sealed class AzureBlobStorageProviderConformanceTests(AzuriteFixture fixture) : StorageProviderConformanceTests, IClassFixture<AzuriteFixture>
{
    private readonly string _container = $"conformance-{Guid.NewGuid():N}";

    protected override StorageUri RootUri => StorageUri.Parse(
        $"azure://{_container}/root/?accountName={AzuriteFixture.AccountName}");

    // The Azure client resolves "." and ".." segments in the request URL, so a blob cannot be named with them.
    protected override bool PreservesNonCanonicalPaths => false;

    protected override Task<IStorageProvider> CreateProviderAsync() => Task.FromResult<IStorageProvider>(fixture.Provider);
}
