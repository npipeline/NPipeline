using System.Net;
using AwesomeAssertions;
using NPipeline.StorageProviders.Gcp.Tests.Support;
using NResilience;

namespace NPipeline.StorageProviders.Gcp.Tests;

public class GcsStreamingWriteTests
{
    private const int Chunk = 256 * 1024;

    [Fact]
    public async Task CommitAsync_CompletesTheUpload_AndSetsTheETag()
    {
        var session = new FakeGcsHttpServer.UploadSession();
        using var server = new FakeGcsHttpServer(session.HandleAsync);
        var provider = server.CreateProvider(Resilience.None);

        await using var stream = await provider.OpenWriteAsync(FakeGcsHttpServer.Uri());
        await stream.WriteAsync("hello gcs"u8.ToArray());

        session.Finalized.Should().BeFalse("nothing is visible before the commit");

        await stream.CommitAsync();

        session.Finalized.Should().BeTrue();
        session.Received.Should().Be(9);
        stream.ETag.Should().Be("etag-1");
    }

    [Fact]
    public async Task CommitAsync_WithNoBytes_CreatesAnEmptyObject()
    {
        var session = new FakeGcsHttpServer.UploadSession();
        using var server = new FakeGcsHttpServer(session.HandleAsync);
        var provider = server.CreateProvider(Resilience.None);

        await using var stream = await provider.OpenWriteAsync(FakeGcsHttpServer.Uri());
        await stream.CommitAsync();

        session.Finalized.Should().BeTrue();
        session.Received.Should().Be(0);
    }

    [Fact]
    public async Task Dispose_WithoutCommit_CommitsNothing()
    {
        var firstChunkSeen = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var session = new FakeGcsHttpServer.UploadSession
        {
            OnBytesReceived = received =>
            {
                if (received >= Chunk)
                    firstChunkSeen.TrySetResult();
            },
        };

        using var server = new FakeGcsHttpServer(session.HandleAsync);
        var provider = server.CreateProvider(Resilience.None, Chunk);

        var stream = await provider.OpenWriteAsync(FakeGcsHttpServer.Uri());
        await stream.WriteAsync(new byte[3 * Chunk]);

        // Part of the object has reached the server, so the abandoned session is real.
        (await Task.WhenAny(firstChunkSeen.Task, Task.Delay(TimeSpan.FromSeconds(10)))).Should().BeSameAs(firstChunkSeen.Task);

        await stream.DisposeAsync();
        await Task.Delay(300);

        session.Finalized.Should().BeFalse("only the final chunk makes the object appear");
    }

    [Fact]
    public async Task Dispose_WithoutCommit_DoesNotBlockAndCommitsNothing_ForTheSynchronousPath()
    {
        var session = new FakeGcsHttpServer.UploadSession();
        using var server = new FakeGcsHttpServer(session.HandleAsync);
        var provider = server.CreateProvider(Resilience.None, Chunk);

        var stream = await provider.OpenWriteAsync(FakeGcsHttpServer.Uri());
        stream.Write(new byte[10]);
        stream.Dispose();
        stream.Dispose();
        await Task.Delay(300);

        session.Finalized.Should().BeFalse();
    }

    [Fact]
    public async Task CommitAsync_WhenTheCallersTokenIsCancelled_AbortsWithoutCommitting()
    {
        var stall = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var session = new FakeGcsHttpServer.UploadSession
        {
            BeforeChunk = async (_, _) =>
            {
                await stall.Task;
                return null;
            },
        };

        using var server = new FakeGcsHttpServer(session.HandleAsync);
        var provider = server.CreateProvider(Resilience.None);

        await using var stream = await provider.OpenWriteAsync(FakeGcsHttpServer.Uri());
        await stream.WriteAsync(new byte[1024]);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => stream.CommitAsync(cts.Token));

        // The server never answered the final chunk, so the object was not finalized. (A real server that had already
        // received the whole final chunk could still finalize it; cancelling a commit cannot undo that.)
        session.Finalized.Should().BeFalse();
        stall.SetCanceled();
    }

    [Fact]
    public async Task WriteAsync_WhenTheUploadFails_ThrowsTheTranslatedFailure()
    {
        var session = new FakeGcsHttpServer.UploadSession
        {
            BeforeChunk = (_, _) => Task.FromResult<HttpStatusCode?>(HttpStatusCode.Forbidden),
        };

        using var server = new FakeGcsHttpServer(session.HandleAsync);
        var provider = server.CreateProvider(Resilience.None, Chunk);

        await using var stream = await provider.OpenWriteAsync(FakeGcsHttpServer.Uri());

        // The first full chunk is refused; a later write or the commit reports it.
        await Assert.ThrowsAsync<UnauthorizedAccessException>(async () =>
        {
            for (var i = 0; i < 40; i++)
            {
                await stream.WriteAsync(new byte[Chunk]);
                await Task.Delay(20);
            }

            await stream.CommitAsync();
        });

        session.Finalized.Should().BeFalse();
    }

    [Fact]
    public async Task WriteAsync_OfTwentyFourMegabytes_StreamsWithBoundedMemory()
    {
        const int chunk = 1024 * 1024;
        const int total = 24 * 1024 * 1024;
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var firstChunkRequested = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var session = new FakeGcsHttpServer.UploadSession
        {
            // The server holds the first chunk, so the writer can only fill what the stream buffers.
            BeforeChunk = async (_, n) =>
            {
                if (n == 2)
                {
                    firstChunkRequested.TrySetResult();
                    await release.Task;
                }

                return null;
            },
        };

        using var server = new FakeGcsHttpServer(session.HandleAsync);
        var provider = server.CreateProvider(Resilience.None, chunk);

        await using var stream = await provider.OpenWriteAsync(FakeGcsHttpServer.Uri());
        long accepted = 0;
        var block = new byte[64 * 1024];
        Random.Shared.NextBytes(block);

        var writer = Task.Run(async () =>
        {
            for (var written = 0; written < total; written += block.Length)
            {
                await stream.WriteAsync(block);
                Interlocked.Add(ref accepted, block.Length);
            }
        });

        (await Task.WhenAny(firstChunkRequested.Task, Task.Delay(TimeSpan.FromSeconds(10)))).Should().BeSameAs(firstChunkRequested.Task);
        await Task.Delay(500);

        writer.IsCompleted.Should().BeFalse("back-pressure pauses the writer while the upload is stalled");
        Interlocked.Read(ref accepted).Should().BeLessThanOrEqualTo(4L * chunk, "buffering is bounded by about two chunks, not by the object");

        release.SetResult();
        await writer;
        await stream.CommitAsync();

        session.Received.Should().Be(total);
        session.Finalized.Should().BeTrue();
    }
}
