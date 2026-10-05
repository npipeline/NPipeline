using AwesomeAssertions;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using FakeItEasy;
using NPipeline.StorageProviders.Exceptions;
using Xunit;

namespace NPipeline.StorageProviders.Azure.Tests;

public class AzureBlobWriteStreamTests
{
    private const string TestContainer = "test-container";
    private const string TestBlob = "test-blob";
    private readonly BlockBlobClient _blockBlob = A.Fake<BlockBlobClient>();
    private readonly List<(string Id, byte[] Data)> _staged = [];
    private readonly object _lock = new();
    private byte[]? _singleUpload;
    private BlobUploadOptions? _singleOptions;
    private CommitBlockListOptions? _commitOptions;
    private List<string>? _committedIds;

    public AzureBlobWriteStreamTests()
    {
        A.CallTo(() => _blockBlob.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .ReturnsLazily((Stream content, BlobUploadOptions options, CancellationToken _) =>
            {
                _singleUpload = ReadAll(content);
                _singleOptions = options;

                return Task.FromResult(Response.FromValue(
                    BlobsModelFactory.BlobContentInfo(new ETag("\"0x1\""), DateTimeOffset.UtcNow, null, null, null, 0), A.Fake<Response>()));
            });

        A.CallTo(() => _blockBlob.StageBlockAsync(A<string>._, A<Stream>._, A<BlockBlobStageBlockOptions>._, A<CancellationToken>._))
            .ReturnsLazily((string id, Stream content, BlockBlobStageBlockOptions _, CancellationToken _) =>
            {
                var data = ReadAll(content);

                lock (_lock)
                {
                    _staged.Add((id, data));
                }

                return Task.FromResult(Response.FromValue(
                    BlobsModelFactory.BlockInfo(null, null, null), A.Fake<Response>()));
            });

        A.CallTo(() => _blockBlob.CommitBlockListAsync(A<IEnumerable<string>>._, A<CommitBlockListOptions>._, A<CancellationToken>._))
            .ReturnsLazily((IEnumerable<string> ids, CommitBlockListOptions options, CancellationToken _) =>
            {
                _committedIds = ids.ToList();
                _commitOptions = options;

                return Task.FromResult(Response.FromValue(
                    BlobsModelFactory.BlobContentInfo(new ETag("\"0x2\""), DateTimeOffset.UtcNow, null, null, null, 0), A.Fake<Response>()));
            });
    }

    private AzureBlobWriteStream NewStream(
        string? contentType = null,
        int partSize = 100,
        int maxConcurrency = 4,
        long? lengthHint = null,
        string? ifMatch = null,
        bool overwrite = true,
        Func<RequestFailedException, Exception>? translate = null) =>
        new(_blockBlob, TestContainer, TestBlob, contentType, partSize, maxConcurrency, lengthHint, ifMatch, overwrite, translate);

    private static byte[] ReadAll(Stream stream)
    {
        using var copy = new MemoryStream();
        stream.CopyTo(copy);

        return copy.ToArray();
    }

    private static byte[] Pattern(int length)
    {
        var data = new byte[length];

        for (var i = 0; i < length; i++)
        {
            data[i] = (byte)(i % 251);
        }

        return data;
    }

    private void FailEverythingWith(RequestFailedException ex)
    {
        A.CallTo(() => _blockBlob.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._)).Throws(ex);
        A.CallTo(() => _blockBlob.StageBlockAsync(A<string>._, A<Stream>._, A<BlockBlobStageBlockOptions>._, A<CancellationToken>._)).Throws(ex);
        A.CallTo(() => _blockBlob.CommitBlockListAsync(A<IEnumerable<string>>._, A<CommitBlockListOptions>._, A<CancellationToken>._)).Throws(ex);
    }

    private void MustNotHaveUploaded()
    {
        A.CallTo(() => _blockBlob.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => _blockBlob.CommitBlockListAsync(A<IEnumerable<string>>._, A<CommitBlockListOptions>._, A<CancellationToken>._)).MustNotHaveHappened();
    }

    #region Construction

    [Fact]
    public void Constructor_WithNullBlobServiceClient_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() => new AzureBlobWriteStream(null!, TestContainer, TestBlob));
        exception.ParamName.Should().Be("blobServiceClient");
    }

    [Fact]
    public void Constructor_WithNullContainer_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() => new AzureBlobWriteStream(A.Fake<BlobServiceClient>(), null!, TestBlob));
        exception.ParamName.Should().Be("container");
    }

    [Fact]
    public void Constructor_WithNullBlob_ThrowsArgumentNullException()
    {
        var exception = Assert.Throws<ArgumentNullException>(() => new AzureBlobWriteStream(A.Fake<BlobServiceClient>(), TestContainer, null!));
        exception.ParamName.Should().Be("blob");
    }

    [Fact]
    public void Constructor_WithNonPositivePartSize_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => NewStream(partSize: 0));
    }

    [Fact]
    public void Constructor_WithNonPositiveConcurrency_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => NewStream(maxConcurrency: 0));
    }

    [Fact]
    public void Constructor_WithValidParameters_CreatesWriteOnlyStream()
    {
        using var stream = NewStream();

        stream.CanRead.Should().BeFalse();
        stream.CanSeek.Should().BeFalse();
        stream.CanWrite.Should().BeTrue();
    }

    #endregion

    #region Stream contract

    [Fact]
    public void Write_WithValidBuffer_CountsTheBytes()
    {
        using var stream = NewStream();
        var data = new byte[] { 1, 2, 3, 4, 5 };

        stream.Write(data, 0, data.Length);

        stream.Length.Should().Be(data.Length);
    }

    [Fact]
    public async Task WriteAsync_WithValidBuffer_CountsTheBytes()
    {
        await using var stream = NewStream();

        await stream.WriteAsync(new byte[] { 1, 2, 3, 4, 5 }, 0, 5);

        stream.Length.Should().Be(5);
    }

    [Fact]
    public void Write_WhenDisposed_ThrowsObjectDisposedException()
    {
        var stream = NewStream();
        stream.Dispose();

        Assert.Throws<ObjectDisposedException>(() => stream.Write(new byte[] { 1 }, 0, 1));
    }

    [Fact]
    public async Task WriteAsync_WhenDisposed_ThrowsObjectDisposedException()
    {
        var stream = NewStream();
        await stream.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await stream.WriteAsync(new byte[] { 1 }, 0, 1));
    }

    [Fact]
    public async Task WriteAsync_AfterCommit_Throws()
    {
        await using var stream = NewStream();
        await stream.WriteAsync(new byte[] { 1 });
        await stream.CommitAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.WriteAsync(new byte[] { 1 }));
    }

    [Fact]
    public async Task Flush_IsNoOp_UploadsNothing()
    {
        await using var stream = NewStream();
        await stream.WriteAsync(new byte[] { 1, 2, 3 });

        stream.Flush();
        await stream.FlushAsync();

        MustNotHaveUploaded();
        A.CallTo(() => _blockBlob.StageBlockAsync(A<string>._, A<Stream>._, A<BlockBlobStageBlockOptions>._, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public void Read_Seek_SetLength_Position_AreNotSupported()
    {
        using var stream = NewStream();

        Assert.Throws<NotSupportedException>(() => stream.Read(new byte[10], 0, 10));
        Assert.Throws<NotSupportedException>(() => stream.Seek(0, SeekOrigin.Begin));
        Assert.Throws<NotSupportedException>(() => stream.SetLength(10));
        Assert.Throws<NotSupportedException>(() => _ = stream.Position);
        Assert.Throws<NotSupportedException>(() => stream.Position = 1);
    }

    [Fact]
    public async Task Length_ReturnsWrittenBytes_AcrossBlocks()
    {
        await using var stream = NewStream(partSize: 100);

        await stream.WriteAsync(Pattern(250));

        stream.Length.Should().Be(250);
    }

    #endregion

    #region Small objects: one request

    [Fact]
    public async Task CommitAsync_SmallObject_UploadsInOneRequest()
    {
        var data = Pattern(60);
        await using var stream = NewStream();
        await stream.WriteAsync(data);

        await stream.CommitAsync();

        _singleUpload.Should().Equal(data);
        A.CallTo(() => _blockBlob.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
        A.CallTo(() => _blockBlob.StageBlockAsync(A<string>._, A<Stream>._, A<BlockBlobStageBlockOptions>._, A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => _blockBlob.CommitBlockListAsync(A<IEnumerable<string>>._, A<CommitBlockListOptions>._, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task CommitAsync_ObjectOfExactlyOnePart_UploadsInOneRequest()
    {
        var data = Pattern(100);
        await using var stream = NewStream(partSize: 100);
        await stream.WriteAsync(data);

        await stream.CommitAsync();

        _singleUpload.Should().Equal(data);
        _staged.Should().BeEmpty();
    }

    [Fact]
    public async Task CommitAsync_EmptyObject_UploadsAnEmptyBlob()
    {
        await using var stream = NewStream();

        await stream.CommitAsync();

        _singleUpload.Should().NotBeNull().And.BeEmpty();
    }

    [Fact]
    public async Task CommitAsync_MultipleSmallWrites_UploadsOneBlobInOneRequest()
    {
        await using var stream = NewStream(partSize: 1000);

        for (var i = 0; i < 10; i++)
        {
            await stream.WriteAsync(new byte[100]);
        }

        await stream.CommitAsync();

        _singleUpload.Should().HaveCount(1000);
        A.CallTo(() => _blockBlob.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task CommitAsync_ExposesTheETagOfTheUploadedBlob()
    {
        await using var stream = NewStream();
        await stream.WriteAsync(new byte[] { 1 });

        await stream.CommitAsync();

        stream.ETag.Should().Be("\"0x1\"");
    }

    [Fact]
    public async Task CommitAsync_CalledTwice_Throws()
    {
        await using var stream = NewStream();
        await stream.WriteAsync(new byte[] { 1 });
        await stream.CommitAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.CommitAsync());
        A.CallTo(() => _blockBlob.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    #endregion

    #region Large objects: blocks then a block list

    [Fact]
    public async Task CommitAsync_LargeObject_StagesBlocksThenCommitsTheBlockListInOrder()
    {
        var data = Pattern(1024);
        await using var stream = NewStream(partSize: 100);

        await stream.WriteAsync(data);
        await stream.CommitAsync();

        // 1024 bytes in blocks of 100 is eleven blocks, the last one 24 bytes.
        _staged.Should().HaveCount(11);
        _committedIds.Should().HaveCount(11);
        _committedIds!.Should().OnlyHaveUniqueItems();
        _committedIds.Select(id => id.Length).Distinct().Should().ContainSingle("block ids of one blob must have the same length");

        var byId = _staged.ToDictionary(s => s.Id, s => s.Data);
        var reassembled = _committedIds.SelectMany(id => byId[id]).ToArray();

        reassembled.Should().Equal(data);
        A.CallTo(() => _blockBlob.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => _blockBlob.CommitBlockListAsync(A<IEnumerable<string>>._, A<CommitBlockListOptions>._, A<CancellationToken>._)).MustHaveHappenedOnceExactly();
    }

    [Fact]
    public async Task CommitAsync_LargeObject_StagesBlocksWhileWriting()
    {
        await using var stream = NewStream(partSize: 100);

        await stream.WriteAsync(Pattern(350));

        // Three full blocks are handed off before commit; the tail waits in the buffer.
        await Task.Delay(100);
        A.CallTo(() => _blockBlob.StageBlockAsync(A<string>._, A<Stream>._, A<BlockBlobStageBlockOptions>._, A<CancellationToken>._))
            .MustHaveHappened(3, Times.Exactly);
        MustNotHaveUploaded();
    }

    [Fact]
    public async Task CommitAsync_LargeObject_ExposesTheETagOfTheCommittedBlockList()
    {
        await using var stream = NewStream(partSize: 100);
        await stream.WriteAsync(Pattern(250));

        await stream.CommitAsync();

        stream.ETag.Should().Be("\"0x2\"");
    }

    [Fact]
    public async Task CommitAsync_LargeObject_HonoursMaxConcurrency()
    {
        var current = 0;
        var peak = 0;

        A.CallTo(() => _blockBlob.StageBlockAsync(A<string>._, A<Stream>._, A<BlockBlobStageBlockOptions>._, A<CancellationToken>._))
            .ReturnsLazily(async () =>
            {
                var now = Interlocked.Increment(ref current);
                int seen;

                while (now > (seen = Volatile.Read(ref peak)))
                {
                    _ = Interlocked.CompareExchange(ref peak, now, seen);
                }

                await Task.Delay(30);
                _ = Interlocked.Decrement(ref current);

                return Response.FromValue(BlobsModelFactory.BlockInfo(null, null, null), A.Fake<Response>());
            });

        await using var stream = NewStream(partSize: 10, maxConcurrency: 2);

        await stream.WriteAsync(new byte[200]);
        await stream.CommitAsync();

        peak.Should().BeInRange(1, 2);
        _committedIds.Should().HaveCount(20);
    }

    [Fact]
    public async Task CommitAsync_WhenABlockFails_ThrowsTheTranslatedErrorAndCommitsNothing()
    {
        A.CallTo(() => _blockBlob.StageBlockAsync(A<string>._, A<Stream>._, A<BlockBlobStageBlockOptions>._, A<CancellationToken>._))
            .Throws(new RequestFailedException(403, "denied", "AuthorizationFailure", null));

        await using var stream = NewStream(partSize: 100);

        Func<Task> act = async () =>
        {
            await stream.WriteAsync(Pattern(500));
            await stream.CommitAsync();
        };

        await act.Should().ThrowAsync<UnauthorizedAccessException>();
        A.CallTo(() => _blockBlob.CommitBlockListAsync(A<IEnumerable<string>>._, A<CommitBlockListOptions>._, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task WriteAsync_WithLengthHint_ChoosesBlocksLargeEnoughToStayWithinTheBlockLimit()
    {
        // A 100 MiB object in 1-byte blocks would need far more than 50,000 blocks, so the hint raises the block size to 1 MiB.
        await using var stream = NewStream(partSize: 1, lengthHint: 100L * 1024 * 1024);

        await stream.WriteAsync(new byte[3 * 1024 * 1024]);
        await Task.Delay(100);

        // Two full 1 MiB blocks are handed off; the third fills the buffer and waits for more data.
        A.CallTo(() => _blockBlob.StageBlockAsync(A<string>._, A<Stream>._, A<BlockBlobStageBlockOptions>._, A<CancellationToken>._))
            .MustHaveHappened(2, Times.Exactly);
    }

    #endregion

    #region Dispose without commit

    [Fact]
    public async Task DisposeAsync_WithoutCommit_SmallObject_UploadsNothing()
    {
        var stream = NewStream();
        await stream.WriteAsync(new byte[] { 1, 2, 3 });

        await stream.DisposeAsync();

        MustNotHaveUploaded();
        A.CallTo(() => _blockBlob.StageBlockAsync(A<string>._, A<Stream>._, A<BlockBlobStageBlockOptions>._, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task DisposeAsync_WithoutCommit_LargeObject_StagesBlocksButNeverCommitsTheBlockList()
    {
        var stream = NewStream(partSize: 100);
        await stream.WriteAsync(Pattern(450));

        await stream.DisposeAsync();

        // Abandoned blocks are uncommitted: the service discards them and no blob appears.
        A.CallTo(() => _blockBlob.CommitBlockListAsync(A<IEnumerable<string>>._, A<CommitBlockListOptions>._, A<CancellationToken>._)).MustNotHaveHappened();
        A.CallTo(() => _blockBlob.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._)).MustNotHaveHappened();
    }

    [Fact]
    public async Task Dispose_WithoutCommit_CalledMultipleTimes_DoesNotThrow()
    {
        var stream = NewStream();
        await stream.WriteAsync(new byte[] { 1 });

        stream.Dispose();
        stream.Dispose();
        await stream.DisposeAsync();

        MustNotHaveUploaded();
    }

    #endregion

    #region Content type

    [Theory]
    [InlineData("application/json")]
    [InlineData("text/csv")]
    [InlineData("application/octet-stream")]
    [InlineData("image/png")]
    public async Task CommitAsync_SmallObject_SetsTheContentType(string contentType)
    {
        await using var stream = NewStream(contentType);
        await stream.WriteAsync(new byte[] { 1 });

        await stream.CommitAsync();

        _singleOptions!.HttpHeaders!.ContentType.Should().Be(contentType);
    }

    [Fact]
    public async Task CommitAsync_LargeObject_SetsTheContentTypeOnTheBlockList()
    {
        await using var stream = NewStream("application/json", 100);
        await stream.WriteAsync(Pattern(250));

        await stream.CommitAsync();

        _commitOptions!.HttpHeaders!.ContentType.Should().Be("application/json");
    }

    [Fact]
    public async Task CommitAsync_WithoutContentType_SendsNoContentTypeOnEitherPath()
    {
        await using (var small = NewStream())
        {
            await small.WriteAsync(new byte[] { 1 });
            await small.CommitAsync();
        }

        await using (var large = NewStream(partSize: 10))
        {
            await large.WriteAsync(new byte[50]);
            await large.CommitAsync();
        }

        _singleOptions!.HttpHeaders.Should().BeNull();
        _commitOptions!.HttpHeaders.Should().BeNull();
    }

    #endregion

    #region Conditions

    [Fact]
    public async Task CommitAsync_SmallObject_WithIfMatch_SendsTheETagAsACondition()
    {
        await using var stream = NewStream(ifMatch: "\"0x8DC\"");
        await stream.WriteAsync(new byte[] { 1 });

        await stream.CommitAsync();

        _singleOptions!.Conditions.Should().NotBeNull();
        _singleOptions.Conditions!.IfMatch.Should().Be(new ETag("\"0x8DC\""));
        _singleOptions.Conditions.IfNoneMatch.Should().BeNull();
    }

    [Fact]
    public async Task CommitAsync_LargeObject_WithIfMatch_SendsTheConditionOnTheBlockListOnly()
    {
        await using var stream = NewStream(partSize: 10, ifMatch: "\"0x8DC\"");
        await stream.WriteAsync(new byte[50]);

        await stream.CommitAsync();

        _commitOptions!.Conditions!.IfMatch.Should().Be(new ETag("\"0x8DC\""));
        A.CallTo(() => _blockBlob.StageBlockAsync(A<string>._, A<Stream>._, A<BlockBlobStageBlockOptions>._, A<CancellationToken>._))
            .MustHaveHappened();
    }

    [Fact]
    public async Task CommitAsync_SmallObject_WithOverwriteFalse_SendsIfNoneMatchStar()
    {
        await using var stream = NewStream(overwrite: false);
        await stream.WriteAsync(new byte[] { 1 });

        await stream.CommitAsync();

        _singleOptions!.Conditions!.IfNoneMatch.Should().Be(ETag.All);
    }

    [Fact]
    public async Task CommitAsync_LargeObject_WithOverwriteFalse_SendsIfNoneMatchStarOnTheBlockList()
    {
        await using var stream = NewStream(partSize: 10, overwrite: false);
        await stream.WriteAsync(new byte[50]);

        await stream.CommitAsync();

        _commitOptions!.Conditions!.IfNoneMatch.Should().Be(ETag.All);
    }

    [Fact]
    public async Task CommitAsync_WithNoConditions_SendsNoneOnEitherPath()
    {
        await using (var small = NewStream())
        {
            await small.WriteAsync(new byte[] { 1 });
            await small.CommitAsync();
        }

        await using (var large = NewStream(partSize: 10))
        {
            await large.WriteAsync(new byte[50]);
            await large.CommitAsync();
        }

        _singleOptions!.Conditions.Should().BeNull();
        _commitOptions!.Conditions.Should().BeNull();
    }

    [Theory]
    [InlineData(412, "ConditionNotMet", 1)]
    [InlineData(409, "BlobAlreadyExists", 1)]
    [InlineData(412, "ConditionNotMet", 50)]
    [InlineData(409, "BlobAlreadyExists", 50)]
    public async Task CommitAsync_WhenTheConditionFails_ThrowsStoragePreconditionFailedException(int status, string code, int length)
    {
        A.CallTo(() => _blockBlob.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .Throws(new RequestFailedException(status, "failed", code, null));

        A.CallTo(() => _blockBlob.CommitBlockListAsync(A<IEnumerable<string>>._, A<CommitBlockListOptions>._, A<CancellationToken>._))
            .Throws(new RequestFailedException(status, "failed", code, null));

        await using var stream = NewStream(partSize: 10, overwrite: false);
        await stream.WriteAsync(new byte[length]);

        await Assert.ThrowsAsync<StoragePreconditionFailedException>(async () => await stream.CommitAsync());
    }

    #endregion

    #region Error translation

    [Theory]
    [InlineData(401, "AuthenticationFailed", typeof(UnauthorizedAccessException))]
    [InlineData(403, "AuthorizationFailed", typeof(UnauthorizedAccessException))]
    [InlineData(400, "InvalidResourceName", typeof(ArgumentException))]
    [InlineData(400, "InvalidQueryParameterValue", typeof(ArgumentException))]
    [InlineData(404, "ContainerNotFound", typeof(FileNotFoundException))]
    [InlineData(404, "BlobNotFound", typeof(FileNotFoundException))]
    [InlineData(500, "InternalError", typeof(IOException))]
    public async Task CommitAsync_TranslatesServiceFailures(int status, string code, Type expected)
    {
        FailEverythingWith(new RequestFailedException(status, "failed", code, null));

        await using var stream = NewStream();
        await stream.WriteAsync(new byte[] { 1, 2, 3, 4, 5 });

        var act = async () => await stream.CommitAsync();

        var thrown = await Assert.ThrowsAnyAsync<Exception>(act);
        thrown.Should().BeAssignableTo(expected);
        thrown.InnerException.Should().BeOfType<RequestFailedException>();
    }

    [Fact]
    public async Task CommitAsync_UsesTheCallersTranslator()
    {
        FailEverythingWith(new RequestFailedException(404, "gone", "PathNotFound", null));

        await using var stream = NewStream(translate: ex => new InvalidOperationException("custom", ex));
        await stream.WriteAsync(new byte[] { 1 });

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await stream.CommitAsync());
    }

    [Fact]
    public async Task CommitAsync_WithAdlsTranslator_MapsPathAlreadyExistsToPreconditionFailed()
    {
        FailEverythingWith(new RequestFailedException(409, "exists", "PathAlreadyExists", null));

        await using var stream = NewStream(overwrite: false, translate: ex => AzureErrors.Translate(ex, TestContainer, TestBlob));
        await stream.WriteAsync(new byte[] { 1 });

        await Assert.ThrowsAsync<StoragePreconditionFailedException>(async () => await stream.CommitAsync());
    }

    #endregion

    #region Cancellation

    [Fact]
    public async Task CommitAsync_WithoutCallerToken_UploadHasNoTimeout()
    {
        // The upload used to run under a hard-coded 5-minute CancelAfter, which cut off large uploads. With no caller
        // token, the upload's token must not be cancellable at all.
        CancellationToken uploadToken = default;

        A.CallTo(() => _blockBlob.UploadAsync(A<Stream>._, A<BlobUploadOptions>._, A<CancellationToken>._))
            .Invokes((Stream _, BlobUploadOptions _, CancellationToken token) => uploadToken = token)
            .ReturnsLazily(() => Task.FromResult(A.Fake<Response<BlobContentInfo>>()));

        await using var stream = NewStream();
        await stream.WriteAsync(new byte[] { 1 });

        await stream.CommitAsync();

        uploadToken.CanBeCanceled.Should().BeFalse();
    }

    [Fact]
    public async Task CommitAsync_WithCancelledToken_Throws()
    {
        await using var stream = NewStream();
        await stream.WriteAsync(new byte[] { 1 });
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await stream.CommitAsync(cts.Token));
        MustNotHaveUploaded();
    }

    #endregion
}
