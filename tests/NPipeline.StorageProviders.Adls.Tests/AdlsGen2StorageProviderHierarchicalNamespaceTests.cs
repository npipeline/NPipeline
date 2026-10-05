using AwesomeAssertions;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using FakeItEasy;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Adls.Tests;

/// <summary>
///     Tests, against faked SDK clients, of the behaviour that depends on whether the account has a hierarchical namespace
///     (HNS): moves (ADLS-1, ADLS-2), listing (ADLS-3) and container creation (WR-5). Azurite has no HNS, so the HNS paths
///     can only be covered here.
/// </summary>
public class AdlsGen2StorageProviderHierarchicalNamespaceTests
{
    private readonly BlobServiceClient _blobService = A.Fake<BlobServiceClient>();
    private readonly AdlsGen2ClientFactory _factory = A.Fake<AdlsGen2ClientFactory>();
    private readonly DataLakeFileSystemClient _fileSystem = A.Fake<DataLakeFileSystemClient>();
    private readonly DataLakeServiceClient _lake = A.Fake<DataLakeServiceClient>();

    public AdlsGen2StorageProviderHierarchicalNamespaceTests()
    {
        A.CallTo(() => _factory.GetBlobServiceClientAsync(A<StorageUri>._, A<CancellationToken>._)).Returns(Task.FromResult(_blobService));
        A.CallTo(() => _factory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._)).Returns(Task.FromResult(_lake));
        A.CallTo(() => _lake.GetFileSystemClient(A<string>._)).Returns(_fileSystem);
    }

    private AdlsGen2StorageProvider NewProvider(bool hns, bool createContainer = false)
    {
        SetHierarchicalNamespace(hns);

        return new AdlsGen2StorageProvider(_factory, new AdlsGen2StorageProviderOptions { CreateContainerIfMissing = createContainer });
    }

    private void SetHierarchicalNamespace(bool enabled) =>
        A.CallTo(() => _blobService.GetAccountInfoAsync(A<CancellationToken>._))
            .Returns(Task.FromResult(Response.FromValue(
                BlobsModelFactory.AccountInfo(SkuName.StandardLrs, AccountKind.StorageV2, enabled), A.Fake<Response>())));

    private static PathItem Path(string name, bool isDirectory, long length = 0) =>
        DataLakeModelFactory.PathItem(name, isDirectory, DateTimeOffset.UnixEpoch, new ETag("\"e\""), length, "owner", "group", "rwxr-x---");

    private void StubPaths(params PathItem[] items) =>
        A.CallTo(() => _fileSystem.GetPathsAsync(A<string>._, A<bool>._, A<bool>._, A<CancellationToken>._))
            .Returns(new FakeAsyncPageable<PathItem>(items));

    private static async Task<List<StorageItem>> ListAsync(IStorageProvider provider, string uri, bool recursive)
    {
        var items = new List<StorageItem>();

        await foreach (var item in provider.ListAsync(StorageUri.Parse(uri), recursive))
        {
            items.Add(item);
        }

        return items;
    }

    #region Moves on an HNS account (ADLS-1, ADLS-2)

    [Fact]
    public async Task MoveAsync_HierarchicalNamespace_RenamesAndNeverCopies()
    {
        var provider = NewProvider(true);
        var source = A.Fake<DataLakeFileClient>();
        A.CallTo(() => _fileSystem.GetFileClient("a.txt")).Returns(source);

        await provider.MoveAsync(StorageUri.Parse("adls://filesystem/a.txt"), StorageUri.Parse("adls://filesystem/b.txt"));

        A.CallTo(() => source.RenameAsync("b.txt", null, A<DataLakeRequestConditions>._, A<DataLakeRequestConditions>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
        A.CallTo(() => _blobService.GetBlobContainerClient(A<string>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task MoveAsync_AcrossFilesystems_LandsInDestinationFilesystem()
    {
        var provider = NewProvider(true);
        var source = A.Fake<DataLakeFileClient>();
        A.CallTo(() => _lake.GetFileSystemClient("source-fs")).Returns(_fileSystem);
        A.CallTo(() => _fileSystem.GetFileClient("dir/file.txt")).Returns(source);

        await provider.MoveAsync(StorageUri.Parse("adls://source-fs/dir/file.txt"), StorageUri.Parse("adls://dest-fs/moved/file.txt"));

        // The destination path is relative to the destination filesystem, which is passed separately.
        A.CallTo(() => source.RenameAsync("moved/file.txt", "dest-fs", A<DataLakeRequestConditions>._, A<DataLakeRequestConditions>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Theory]
    [InlineData(400, typeof(ArgumentException))]
    [InlineData(404, typeof(FileNotFoundException))]
    [InlineData(403, typeof(UnauthorizedAccessException))]
    [InlineData(412, typeof(StoragePreconditionFailedException))]
    public async Task MoveAsync_RenameFails_ThrowsTheTranslatedErrorAndDoesNotCopy(int status, Type expected)
    {
        var provider = NewProvider(true);
        var source = A.Fake<DataLakeFileClient>();
        A.CallTo(() => _fileSystem.GetFileClient("a.txt")).Returns(source);

        A.CallTo(() => source.RenameAsync(A<string>._, A<string>._, A<DataLakeRequestConditions>._, A<DataLakeRequestConditions>._, A<CancellationToken>._))
            .Throws(new RequestFailedException(status, "rename failed", "SomeCode", null));

        var act = async () => await provider.MoveAsync(StorageUri.Parse("adls://filesystem/a.txt"), StorageUri.Parse("adls://filesystem/b.txt"));

        var thrown = await Assert.ThrowsAnyAsync<Exception>(act);
        thrown.Should().BeAssignableTo(expected);
        thrown.InnerException.Should().BeOfType<RequestFailedException>();

        // A rejected rename is an error. It is never degraded to copy-and-delete, which would not be atomic.
        A.CallTo(() => _blobService.GetBlobContainerClient(A<string>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task MoveAsync_RenameFailsWith400_DoesNotCopy()
    {
        var provider = NewProvider(true);
        var source = A.Fake<DataLakeFileClient>();
        A.CallTo(() => _fileSystem.GetFileClient("a.txt")).Returns(source);

        A.CallTo(() => source.RenameAsync(A<string>._, A<string>._, A<DataLakeRequestConditions>._, A<DataLakeRequestConditions>._, A<CancellationToken>._))
            .Throws(new RequestFailedException(400, "bad request", "InvalidRenameSourceOrDestinationPath", null));

        var act = async () => await provider.MoveAsync(StorageUri.Parse("adls://filesystem/a.txt"), StorageUri.Parse("adls://filesystem/b.txt"));

        await act.Should().ThrowAsync<ArgumentException>();
        Fake.GetCalls(_blobService).Should().NotContain(c => c.Method.Name == "GetBlobContainerClient");
    }

    [Fact]
    public async Task MoveAsync_WhenDetectionFails_AssumesHierarchicalNamespace()
    {
        A.CallTo(() => _blobService.GetAccountInfoAsync(A<CancellationToken>._)).Throws(new RequestFailedException(403, "no permission"));
        var provider = new AdlsGen2StorageProvider(_factory, new AdlsGen2StorageProviderOptions());
        var source = A.Fake<DataLakeFileClient>();
        A.CallTo(() => _fileSystem.GetFileClient("a.txt")).Returns(source);

        await provider.MoveAsync(StorageUri.Parse("adls://filesystem/a.txt"), StorageUri.Parse("adls://filesystem/b.txt"));

        A.CallTo(() => source.RenameAsync("b.txt", null, A<DataLakeRequestConditions>._, A<DataLakeRequestConditions>._, A<CancellationToken>._))
            .MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task MoveAsync_DetectsHierarchicalNamespaceOncePerEndpoint()
    {
        var provider = NewProvider(true);
        A.CallTo(() => _fileSystem.GetFileClient(A<string>._)).Returns(A.Fake<DataLakeFileClient>());

        await provider.MoveAsync(StorageUri.Parse("adls://filesystem/a.txt"), StorageUri.Parse("adls://filesystem/b.txt"));
        await provider.MoveAsync(StorageUri.Parse("adls://filesystem/c.txt"), StorageUri.Parse("adls://filesystem/d.txt"));
        _ = await ListAsync(provider, "adls://filesystem/", false);

        A.CallTo(() => _blobService.GetAccountInfoAsync(A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    #endregion

    #region Moves on a non-HNS account

    [Fact]
    public async Task MoveAsync_WithoutHierarchicalNamespace_CopiesThenDeletesAndNeverRenames()
    {
        var provider = NewProvider(false);
        var container = A.Fake<BlobContainerClient>();
        var source = A.Fake<BlobClient>();
        var destination = A.Fake<BlobClient>();
        var copy = A.Fake<CopyFromUriOperation>();
        var sourceUri = new Uri("http://127.0.0.1:10000/acct/filesystem/a.txt");

        A.CallTo(() => _blobService.GetBlobContainerClient("filesystem")).Returns(container);
        A.CallTo(() => container.GetBlobClient("a.txt")).Returns(source);
        A.CallTo(() => container.GetBlobClient("b.txt")).Returns(destination);
        A.CallTo(() => source.Uri).Returns(sourceUri);

        A.CallTo(destination).Where(c => c.Method.Name == "StartCopyFromUriAsync").WithReturnType<Task<CopyFromUriOperation>>()
            .Returns(Task.FromResult(copy));

        A.CallTo(() => copy.WaitForCompletionAsync(A<CancellationToken>._))
            .Returns(new ValueTask<Response<long>>(Response.FromValue(0L, A.Fake<Response>())));

        await provider.MoveAsync(StorageUri.Parse("adls://filesystem/a.txt"), StorageUri.Parse("adls://filesystem/b.txt"));

        Fake.GetCalls(destination).Should().Contain(c => c.Method.Name == "StartCopyFromUriAsync");
        Fake.GetCalls(source).Should().Contain(c => c.Method.Name == "DeleteIfExistsAsync");
        Fake.GetCalls(_lake).Should().BeEmpty("a move without a hierarchical namespace does not use the Data Lake API");
    }

    [Fact]
    public async Task MoveAsync_WithoutHierarchicalNamespace_AcrossFilesystems_CopiesIntoTheDestinationContainer()
    {
        var provider = NewProvider(false);
        var sourceContainer = A.Fake<BlobContainerClient>();
        var destinationContainer = A.Fake<BlobContainerClient>();
        var source = A.Fake<BlobClient>();
        var destination = A.Fake<BlobClient>();
        var copy = A.Fake<CopyFromUriOperation>();

        A.CallTo(() => _blobService.GetBlobContainerClient("source-fs")).Returns(sourceContainer);
        A.CallTo(() => _blobService.GetBlobContainerClient("dest-fs")).Returns(destinationContainer);
        A.CallTo(() => sourceContainer.GetBlobClient("dir/file.txt")).Returns(source);
        A.CallTo(() => destinationContainer.GetBlobClient("moved/file.txt")).Returns(destination);
        A.CallTo(() => source.Uri).Returns(new Uri("http://127.0.0.1:10000/acct/source-fs/dir/file.txt"));

        A.CallTo(destination).Where(c => c.Method.Name == "StartCopyFromUriAsync").WithReturnType<Task<CopyFromUriOperation>>()
            .Returns(Task.FromResult(copy));

        A.CallTo(() => copy.WaitForCompletionAsync(A<CancellationToken>._))
            .Returns(new ValueTask<Response<long>>(Response.FromValue(0L, A.Fake<Response>())));

        await provider.MoveAsync(StorageUri.Parse("adls://source-fs/dir/file.txt"), StorageUri.Parse("adls://dest-fs/moved/file.txt"));

        Fake.GetCalls(destination).Should().Contain(c => c.Method.Name == "StartCopyFromUriAsync");
        Fake.GetCalls(source).Should().Contain(c => c.Method.Name == "DeleteIfExistsAsync");
    }

    #endregion

    #region Cross-account moves

    [Theory]
    [InlineData("adls://fs1/a.txt?accountName=account1", "adls://fs2/b.txt?accountName=account2")]
    [InlineData("adls://fs1/a.txt?serviceUrl=http%3A%2F%2Flocalhost%3A10000%2Facct", "adls://fs1/b.txt?serviceUrl=http%3A%2F%2Flocalhost%3A10001%2Facct")]
    [InlineData("adls://fs1/a.txt?accountName=acct&serviceUrl=http%3A%2F%2Flocalhost%3A10000%2Facct", "adls://fs1/b.txt?accountName=acct")]
    public async Task MoveAsync_AcrossAccounts_ThrowsNotSupportedBeforeAnyRequest(string source, string destination)
    {
        var provider = NewProvider(true);

        var act = async () => await provider.MoveAsync(StorageUri.Parse(source), StorageUri.Parse(destination));

        await act.Should().ThrowAsync<NotSupportedException>();
        A.CallTo(() => _blobService.GetAccountInfoAsync(A<CancellationToken>._)).MustNotHaveHappened();
        Fake.GetCalls(_lake).Should().BeEmpty();
    }

    [Fact]
    public async Task MoveAsync_SameEndpointWrittenDifferently_IsNotCrossAccount()
    {
        // The default account name in the options and the same name in the URI identify one endpoint.
        SetHierarchicalNamespace(true);
        var provider = new AdlsGen2StorageProvider(_factory, new AdlsGen2StorageProviderOptions { AccountName = "acct" });
        A.CallTo(() => _fileSystem.GetFileClient(A<string>._)).Returns(A.Fake<DataLakeFileClient>());

        var act = async () => await provider.MoveAsync(StorageUri.Parse("adls://filesystem/a.txt"), StorageUri.Parse("adls://filesystem/b.txt?accountName=acct"));

        await act.Should().NotThrowAsync();
    }

    #endregion

    #region Listing an HNS account (ADLS-3)

    [Fact]
    public async Task ListAsync_HierarchicalNamespace_NonRecursive_ReturnsFilesAndRealDirectories()
    {
        var provider = NewProvider(true);
        StubPaths(Path("data/sub", true), Path("data/empty", true), Path("data/a.txt", false, 5));

        var items = await ListAsync(provider, "adls://filesystem/data", false);

        items.Should().HaveCount(3);
        items.Where(i => i.IsDirectory).Select(i => i.Uri.Path).Should().BeEquivalentTo("/data/sub/", "/data/empty/");
        var file = items.Single(i => !i.IsDirectory);
        file.Uri.Path.Should().Be("/data/a.txt");
        file.Size.Should().Be(5);
        items.Where(i => i.IsDirectory).Should().OnlyContain(i => i.LastModified == DateTimeOffset.UnixEpoch);
    }

    [Fact]
    public async Task ListAsync_HierarchicalNamespace_Recursive_YieldsFilesOnly()
    {
        var provider = NewProvider(true);
        StubPaths(Path("data/sub", true), Path("data/sub/b.txt", false, 2), Path("data/a.txt", false, 1));

        var items = await ListAsync(provider, "adls://filesystem/data", true);

        items.Should().OnlyContain(i => !i.IsDirectory);
        items.Select(i => i.Uri.Path).Should().BeEquivalentTo("/data/sub/b.txt", "/data/a.txt");

        A.CallTo(() => _fileSystem.GetPathsAsync("data", true, false, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    [Theory]
    [InlineData("adls://filesystem/data", "data")]
    [InlineData("adls://filesystem/data/", "data")]
    [InlineData("adls://filesystem/", null)]
    [InlineData("adls://filesystem", null)]
    public async Task ListAsync_HierarchicalNamespace_PassesTheDirectoryWithoutATrailingSlash(string uri, string? expectedPath)
    {
        var provider = NewProvider(true);
        StubPaths();

        _ = await ListAsync(provider, uri, false);

        A.CallTo(() => _fileSystem.GetPathsAsync(expectedPath, false, false, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task ListAsync_HierarchicalNamespace_KeepsTheCallersParametersOnEveryItem()
    {
        var provider = NewProvider(true);
        StubPaths(Path("data/sub", true), Path("data/a.txt", false, 1));

        var items = await ListAsync(provider, "adls://filesystem/data?accountName=acct&contentType=text/plain", false);

        items.Should().HaveCount(2);
        items.Should().OnlyContain(i => i.Uri.Parameters["accountName"] == "acct" && i.Uri.Parameters["contentType"] == "text/plain");
    }

    [Fact]
    public async Task ListAsync_HierarchicalNamespace_CaseDistinctNamesBothAppear()
    {
        var provider = NewProvider(true);
        StubPaths(Path("Data", true), Path("data", true), Path("File.txt", false, 1), Path("file.txt", false, 2));

        var items = await ListAsync(provider, "adls://filesystem/", false);

        items.Select(i => i.Uri.Path).Should().BeEquivalentTo("/Data/", "/data/", "/File.txt", "/file.txt");
    }

    [Fact]
    public async Task ListAsync_HierarchicalNamespace_DoesNotCheckTheFilesystemExists()
    {
        var provider = NewProvider(true);
        StubPaths(Path("a.txt", false, 1));

        _ = await ListAsync(provider, "adls://filesystem/", false);

        Fake.GetCalls(_fileSystem).Should().NotContain(c => c.Method.Name == "ExistsAsync");
        A.CallTo(() => _blobService.GetBlobContainerClient(A<string>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task ListAsync_HierarchicalNamespace_WhenTheFirstPageIs404_YieldsNothing()
    {
        var provider = NewProvider(true);

        A.CallTo(() => _fileSystem.GetPathsAsync(A<string>._, A<bool>._, A<bool>._, A<CancellationToken>._))
            .Returns(new ThrowingPageable<PathItem>(new RequestFailedException(404, "missing", "FilesystemNotFound", null)));

        (await ListAsync(provider, "adls://missing-fs/dir", false)).Should().BeEmpty();
        (await ListAsync(provider, "adls://missing-fs/dir", true)).Should().BeEmpty();
    }

    [Fact]
    public async Task ListAsync_HierarchicalNamespace_WhenAccessIsDenied_ThrowsUnauthorizedAccessException()
    {
        var provider = NewProvider(true);

        A.CallTo(() => _fileSystem.GetPathsAsync(A<string>._, A<bool>._, A<bool>._, A<CancellationToken>._))
            .Returns(new ThrowingPageable<PathItem>(new RequestFailedException(403, "denied", "AuthorizationFailure", null)));

        var act = async () => await ListAsync(provider, "adls://filesystem/dir", false);

        await act.Should().ThrowAsync<UnauthorizedAccessException>();
    }

    #endregion

    #region Listing a non-HNS account

    [Fact]
    public async Task ListAsync_WithoutHierarchicalNamespace_ListsBlobsByPrefixWithNoExistsCheckAndNoMetadata()
    {
        var provider = NewProvider(false);
        var container = A.Fake<BlobContainerClient>();
        BlobTraits? traits = null;
        string? prefix = null;
        A.CallTo(() => _blobService.GetBlobContainerClient("filesystem")).Returns(container);

        A.CallTo(() => container.GetBlobsAsync(A<BlobTraits>._, A<BlobStates>._, A<string>._, A<CancellationToken>._))
            .Invokes((BlobTraits t, BlobStates _, string p, CancellationToken _) =>
            {
                traits = t;
                prefix = p;
            })
            .Returns(new FakeAsyncPageable<BlobItem>([BlobsModelFactory.BlobItem("data/a.txt", false, null, null, null)]));

        var items = await ListAsync(provider, "adls://filesystem/data", true);

        items.Should().ContainSingle().Which.Uri.Path.Should().Be("/data/a.txt");
        traits.Should().Be(BlobTraits.None);
        prefix.Should().Be("data/");
        Fake.GetCalls(container).Should().NotContain(c => c.Method.Name == "ExistsAsync");
        Fake.GetCalls(_lake).Should().BeEmpty();
    }

    [Fact]
    public async Task ListAsync_WithoutHierarchicalNamespace_CaseDistinctPrefixesBothAppear()
    {
        var provider = NewProvider(false);
        var container = A.Fake<BlobContainerClient>();
        A.CallTo(() => _blobService.GetBlobContainerClient("filesystem")).Returns(container);

        A.CallTo(() => container.GetBlobsByHierarchyAsync(A<BlobTraits>._, A<BlobStates>._, A<string>._, A<string>._, A<CancellationToken>._))
            .Returns(new FakeAsyncPageable<BlobHierarchyItem>(
            [
                BlobsModelFactory.BlobHierarchyItem("Data/", null),
                BlobsModelFactory.BlobHierarchyItem("data/", null),
            ]));

        var items = await ListAsync(provider, "adls://filesystem/", false);

        items.Select(i => i.Uri.Path).Should().BeEquivalentTo("/Data/", "/data/");
        items.Should().OnlyContain(i => i.IsDirectory);
    }

    [Fact]
    public async Task ListAsync_WithoutHierarchicalNamespace_WhenTheFirstPageIs404_YieldsNothing()
    {
        var provider = NewProvider(false);
        var container = A.Fake<BlobContainerClient>();
        A.CallTo(() => _blobService.GetBlobContainerClient("filesystem")).Returns(container);

        A.CallTo(() => container.GetBlobsAsync(A<BlobTraits>._, A<BlobStates>._, A<string>._, A<CancellationToken>._))
            .Returns(new ThrowingPageable<BlobItem>(new RequestFailedException(404, "missing", "ContainerNotFound", null)));

        (await ListAsync(provider, "adls://filesystem/dir", true)).Should().BeEmpty();
    }

    #endregion

    #region Container creation (WR-5)

    private int ContainerCreations(BlobContainerClient container) =>
        Fake.GetCalls(container).Count(c => c.Method.Name == "CreateIfNotExistsAsync");

    private static async Task WriteAsync(IStorageProvider provider, string uri)
    {
        await using var stream = await provider.OpenWriteAsync(StorageUri.Parse(uri));
        await stream.WriteAsync(new byte[] { 1 });
    }

    [Fact]
    public async Task OpenWrite_DefaultOptions_DoesNotCreateContainer()
    {
        var provider = NewProvider(false);
        var container = A.Fake<BlobContainerClient>();
        A.CallTo(() => _blobService.GetBlobContainerClient(A<string>._)).Returns(container);

        await WriteAsync(provider, "adls://filesystem/a.txt");
        await WriteAsync(provider, "adls://filesystem/b.txt");

        ContainerCreations(container).Should().Be(0);
        Fake.GetCalls(container).Should().NotContain(c => c.Method.Name == "ExistsAsync");
        Fake.GetCalls(_lake).Should().BeEmpty("opening a write no longer builds a Data Lake client");
    }

    [Fact]
    public async Task OpenWrite_CreateContainerIfMissing_CreatesTheContainerExactlyOnceAcrossWrites()
    {
        var provider = NewProvider(true, true);
        var container = A.Fake<BlobContainerClient>();
        A.CallTo(() => _blobService.GetBlobContainerClient(A<string>._)).Returns(container);

        for (var i = 0; i < 5; i++)
        {
            await WriteAsync(provider, $"adls://filesystem/dir/file-{i}.txt");
        }

        ContainerCreations(container).Should().Be(1);
    }

    #endregion

    private sealed class ThrowingPageable<T>(RequestFailedException exception) : AsyncPageable<T>
        where T : notnull
    {
        public override IAsyncEnumerable<Page<T>> AsPages(string? continuationToken = null, int? pageSizeHint = null) =>
            throw exception;

        public override IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default) =>
            new ThrowingEnumerator(exception);

        private sealed class ThrowingEnumerator(RequestFailedException exception) : IAsyncEnumerator<T>
        {
            public T Current => throw new InvalidOperationException();

            public ValueTask<bool> MoveNextAsync() => throw exception;

            public ValueTask DisposeAsync() => default;
        }
    }
}
