using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common;
using NPipeline.Tests.Common.Storage;

namespace NPipeline.StorageProviders.Tests;

/// <summary>Runs the shared contract against the in-memory test provider, so the suite's conditional-write cases run without a container.</summary>
public sealed class InMemoryStorageProviderConformanceTests : StorageProviderConformanceTests
{
    protected override StorageUri RootUri => InMemoryStorageProvider.Uri($"conformance-{Guid.NewGuid():N}/");

    // The test provider lists loosely (no directory entries); listing is covered by the real providers.
    protected override bool SupportsList => false;

    protected override Task<IStorageProvider> CreateProviderAsync() => Task.FromResult<IStorageProvider>(new InMemoryStorageProvider());
}
