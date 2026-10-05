using AwesomeAssertions;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using FakeItEasy;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.StorageProviders.Azure.Tests;

/// <summary>
///     Tests of the container-creation (WR-5) and listing behaviour of <see cref="AzureBlobStorageProvider" /> against faked
///     SDK clients.
/// </summary>
public class AzureBlobStorageProviderBehaviorTests
{
    private readonly BlobContainerClient _container = A.Fake<BlobContainerClient>();
    private readonly AzureBlobClientFactory _factory;
    private readonly BlobServiceClient _service = A.Fake<BlobServiceClient>();

    public AzureBlobStorageProviderBehaviorTests()
    {
        _factory = A.Fake<AzureBlobClientFactory>(c => c
            .WithArgumentsForConstructor([new AzureBlobStorageProviderOptions()])
            .CallsBaseMethods());

        A.CallTo(() => _factory.GetClientAsync(A<StorageUri>._, A<CancellationToken>._)).Returns(Task.FromResult(_service));
        A.CallTo(() => _service.GetBlobContainerClient(A<string>._)).Returns(_container);
    }

    private AzureBlobStorageProvider NewProvider(bool createContainer = false) =>
        new(_factory, new AzureBlobStorageProviderOptions { CreateContainerIfMissing = createContainer });

    private int ContainerCreations() =>
        Fake.GetCalls(_container).Count(c => c.Method.Name == "CreateIfNotExistsAsync");

    private static async Task WriteAsync(IStorageProvider provider, StorageUri uri)
    {
        await using var stream = await provider.OpenWriteAsync(uri);
        await stream.WriteAsync(new byte[] { 1 });
    }

    [Fact]
    public async Task OpenWrite_DefaultOptions_DoesNotCreateContainer()
    {
        var provider = NewProvider();

        await WriteAsync(provider, StorageUri.Parse("azure://test-container/a.txt"));
        await WriteAsync(provider, StorageUri.Parse("azure://test-container/b.txt"));

        ContainerCreations().Should().Be(0);
        Fake.GetCalls(_container).Should().NotContain(c => c.Method.Name == "ExistsAsync");
    }

    [Fact]
    public async Task OpenWrite_CreateContainerIfMissing_CreatesTheContainerExactlyOnceAcrossWrites()
    {
        var provider = NewProvider(true);

        for (var i = 0; i < 5; i++)
        {
            await WriteAsync(provider, StorageUri.Parse($"azure://test-container/file-{i}.txt"));
        }

        ContainerCreations().Should().Be(1);
    }

    [Fact]
    public async Task OpenWrite_CreateContainerIfMissing_CreatesEachContainerOnce()
    {
        var provider = NewProvider(true);

        await WriteAsync(provider, StorageUri.Parse("azure://container-a/x.txt"));
        await WriteAsync(provider, StorageUri.Parse("azure://container-b/x.txt"));
        await WriteAsync(provider, StorageUri.Parse("azure://container-a/y.txt"));
        await WriteAsync(provider, StorageUri.Parse("azure://container-b/y.txt"));

        A.CallTo(() => _service.GetBlobContainerClient("container-a")).MustHaveHappened();
        ContainerCreations().Should().Be(2);
    }

    [Fact]
    public async Task OpenWrite_CreateContainerIfMissing_ConcurrentWritesCreateOnce()
    {
        var provider = NewProvider(true);

        await Task.WhenAll(Enumerable.Range(0, 10)
            .Select(i => WriteAsync(provider, StorageUri.Parse($"azure://test-container/file-{i}.txt"))));

        ContainerCreations().Should().Be(1);
    }

    [Fact]
    public async Task OpenWrite_CreateContainerIfMissing_RetriesAfterAFailedCreation()
    {
        var provider = NewProvider(true);
        var attempts = 0;

        A.CallTo(_container).Where(c => c.Method.Name == "CreateIfNotExistsAsync")
            .Invokes(() =>
            {
                if (Interlocked.Increment(ref attempts) == 1)
                    throw new RequestFailedException(500, "boom");
            });

        var first = async () => await provider.OpenWriteAsync(StorageUri.Parse("azure://test-container/a.txt"));
        await first.Should().ThrowAsync<IOException>();

        await WriteAsync(provider, StorageUri.Parse("azure://test-container/b.txt"));

        ContainerCreations().Should().Be(2);
    }

    [Fact]
    public async Task ListAsync_DoesNotCheckTheContainerExistsAndRequestsNoMetadata()
    {
        var uri = StorageUri.Parse("azure://test-container/prefix/");
        var traits = new List<BlobTraits>();

        A.CallTo(() => _container.GetBlobsAsync(A<BlobTraits>._, A<BlobStates>._, A<string>._, A<CancellationToken>._))
            .ReturnsLazily((BlobTraits t, BlobStates _, string _, CancellationToken _) =>
            {
                traits.Add(t);

                return new TestAsyncPageable<BlobItem>(new SimpleAsyncEnumerable<BlobItem>([]));
            });

        _ = await NewProvider().ListAsync(uri, true).ToListAsync();

        Fake.GetCalls(_container).Should().NotContain(c => c.Method.Name == "ExistsAsync");
        traits.Should().ContainSingle().Which.Should().Be(BlobTraits.None);
    }

    [Fact]
    public async Task ListAsync_NonRecursive_DoesNotCheckTheContainerExistsAndRequestsNoMetadata()
    {
        var uri = StorageUri.Parse("azure://test-container/prefix/");
        var traits = new List<BlobTraits>();

        A.CallTo(() => _container.GetBlobsByHierarchyAsync(A<BlobTraits>._, A<BlobStates>._, A<string>._, A<string>._, A<CancellationToken>._))
            .ReturnsLazily((BlobTraits t, BlobStates _, string _, string _, CancellationToken _) =>
            {
                traits.Add(t);

                return new TestAsyncPageable<BlobHierarchyItem>(new SimpleAsyncEnumerable<BlobHierarchyItem>([]));
            });

        _ = await NewProvider().ListAsync(uri, false).ToListAsync();

        Fake.GetCalls(_container).Should().NotContain(c => c.Method.Name == "ExistsAsync");
        traits.Should().ContainSingle().Which.Should().Be(BlobTraits.None);
    }

    [Fact]
    public async Task ListAsync_WhenTheFirstPageIs404_YieldsNothing()
    {
        A.CallTo(() => _container.GetBlobsAsync(A<BlobTraits>._, A<BlobStates>._, A<string>._, A<CancellationToken>._))
            .Returns(new ThrowingPageable<BlobItem>(new RequestFailedException(404, "no container", "ContainerNotFound", null)));

        var items = await NewProvider().ListAsync(StorageUri.Parse("azure://missing-container/"), true).ToListAsync();

        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListAsync_NonRecursive_WhenTheFirstPageIs404_YieldsNothing()
    {
        A.CallTo(() => _container.GetBlobsByHierarchyAsync(A<BlobTraits>._, A<BlobStates>._, A<string>._, A<string>._, A<CancellationToken>._))
            .Returns(new ThrowingPageable<BlobHierarchyItem>(new RequestFailedException(404, "no container", "ContainerNotFound", null)));

        var items = await NewProvider().ListAsync(StorageUri.Parse("azure://missing-container/"), false).ToListAsync();

        items.Should().BeEmpty();
    }

    [Fact]
    public async Task ListAsync_WhenTheServiceDeniesAccess_ThrowsUnauthorizedAccessException()
    {
        A.CallTo(() => _container.GetBlobsAsync(A<BlobTraits>._, A<BlobStates>._, A<string>._, A<CancellationToken>._))
            .Returns(new ThrowingPageable<BlobItem>(new RequestFailedException(403, "denied", "AuthorizationFailure", null)));

        var act = async () => await NewProvider().ListAsync(StorageUri.Parse("azure://test-container/"), true).ToListAsync();

        await act.Should().ThrowAsync<UnauthorizedAccessException>();
    }

    [Fact]
    public async Task ListAsync_WithServiceUrl_KeepsTheCallersParametersOnEveryItem()
    {
        var uri = StorageUri.Parse("azure://test-container/data?serviceUrl=http://localhost:10000/acct");

        A.CallTo(() => _container.GetBlobsByHierarchyAsync(A<BlobTraits>._, A<BlobStates>._, A<string>._, A<string>._, A<CancellationToken>._))
            .Returns(new TestAsyncPageable<BlobHierarchyItem>(new SimpleAsyncEnumerable<BlobHierarchyItem>(
            [
                BlobsModelFactory.BlobHierarchyItem("data/sub/", null),
                BlobsModelFactory.BlobHierarchyItem(null, BlobsModelFactory.BlobItem("data/a.txt", false, BlobsModelFactory.BlobItemProperties(false, contentLength: 3))),
            ])));

        var items = await NewProvider().ListAsync(uri, false).ToListAsync();

        items.Should().HaveCount(2);
        items.Should().OnlyContain(i => i.Uri.Parameters.ContainsKey("serviceUrl"));
        items.Should().Contain(i => i.IsDirectory && i.Uri.Path == "/data/sub/");
    }

    internal sealed class ThrowingPageable<T>(RequestFailedException exception) : AsyncPageable<T>
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
