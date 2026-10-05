using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using NPipeline.Tests.Common.Storage;

namespace NPipeline.StorageProviders.Tests;

public sealed class FileSystemStorageProviderConformanceTests : StorageProviderConformanceTests
{
    private readonly string _directory = Directory.CreateTempSubdirectory("np-conformance-").FullName;

    protected override StorageUri RootUri => StorageUri.FromFilePath(_directory + Path.DirectorySeparatorChar);

    protected override bool PreservesNonCanonicalPaths => false;

    protected override int MaxSegmentLength => 200;

    protected override int LongKeyLength => 400;

    protected override Task<IStorageProvider> CreateProviderAsync() => Task.FromResult<IStorageProvider>(new FileSystemStorageProvider());

    protected override Task CleanupAsync()
    {
        Directory.Delete(_directory, true);
        return Task.CompletedTask;
    }
}
