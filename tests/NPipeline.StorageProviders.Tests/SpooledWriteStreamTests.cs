using AwesomeAssertions;
using NPipeline.StorageProviders.Abstractions;
using Xunit;

namespace NPipeline.StorageProviders.Tests;

public sealed class SpooledWriteStreamTests
{
    [Fact]
    public async Task CommitAsync_UploadsTheWrittenBytesOnceUnderTheCallersToken()
    {
        var stream = new RecordingSpooledStream { Result = "\"etag-1\"" };
        using var cts = new CancellationTokenSource();

        await stream.WriteAsync(new byte[] { 1, 2, 3 });
        await stream.WriteAsync(new byte[] { 4, 5 });
        await stream.CommitAsync(cts.Token);
        await stream.DisposeAsync();

        stream.Uploads.Should().ContainSingle().Which.Should().Equal(1, 2, 3, 4, 5);
        stream.Token.Should().Be(cts.Token);
        stream.ETag.Should().Be("\"etag-1\"");
    }

    [Fact]
    public async Task DisposeAsync_WithoutCommit_UploadsNothing()
    {
        var stream = new RecordingSpooledStream();
        await stream.WriteAsync(new byte[] { 1, 2, 3 });

        await stream.DisposeAsync();

        stream.Uploads.Should().BeEmpty();
    }

    [Fact]
    public async Task Dispose_WithoutCommit_UploadsNothingAndNeverBlocksOnTheNetwork()
    {
        var stream = new RecordingSpooledStream();
        stream.Write([1, 2, 3], 0, 3);

        stream.Dispose();
        stream.Dispose();
        await stream.DisposeAsync();

        stream.Uploads.Should().BeEmpty();
    }

    [Fact]
    public async Task CommitAsync_Twice_Throws()
    {
        var stream = new RecordingSpooledStream();
        await stream.CommitAsync();

        var second = () => stream.CommitAsync();

        await second.Should().ThrowAsync<InvalidOperationException>();
        stream.Uploads.Should().HaveCount(1);
    }

    [Fact]
    public async Task WriteAsync_AfterCommit_Throws()
    {
        var stream = new RecordingSpooledStream();
        await stream.CommitAsync();

        var write = async () => await stream.WriteAsync(new byte[] { 1 });

        await write.Should().ThrowAsync<InvalidOperationException>();
        stream.CanWrite.Should().BeFalse();
    }

    [Fact]
    public async Task CommitAsync_AfterDispose_Throws()
    {
        var stream = new RecordingSpooledStream();
        await stream.DisposeAsync();

        var commit = () => stream.CommitAsync();

        await commit.Should().ThrowAsync<ObjectDisposedException>();
    }

    [Fact]
    public async Task CommitAsync_WhenTheUploadFails_CanBeRetriedFromTheStart()
    {
        var stream = new RecordingSpooledStream { FailFirstUpload = true };
        await stream.WriteAsync(new byte[] { 7, 8, 9 });

        var first = () => stream.CommitAsync();
        await first.Should().ThrowAsync<IOException>();
        await stream.CommitAsync();

        stream.Uploads.Should().HaveCount(2);
        stream.Uploads[1].Should().Equal(7, 8, 9);
        stream.ETag.Should().BeNull();
    }

    [Fact]
    public async Task CommitAsync_WithACancelledToken_UploadsNothing()
    {
        var stream = new RecordingSpooledStream();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var commit = () => stream.CommitAsync(cts.Token);

        await commit.Should().ThrowAsync<OperationCanceledException>();
        stream.Uploads.Should().BeEmpty();
    }

    private sealed class RecordingSpooledStream : SpooledWriteStream
    {
        public RecordingSpooledStream()
            : base("test-spool")
        {
        }

        public List<byte[]> Uploads { get; } = [];

        public CancellationToken Token { get; private set; }

        public string? Result { get; init; }

        public bool FailFirstUpload { get; init; }

        protected override async Task<string?> UploadAsync(Stream content, CancellationToken cancellationToken)
        {
            Token = cancellationToken;
            using var copy = new MemoryStream();
            await content.CopyToAsync(copy, cancellationToken);
            Uploads.Add(copy.ToArray());

            if (FailFirstUpload && Uploads.Count == 1)
                throw new IOException("boom");

            return Result;
        }
    }
}
