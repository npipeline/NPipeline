using System.Collections.Concurrent;
using AwesomeAssertions;
using NPipeline.StorageProviders.Abstractions;
using Xunit;

namespace NPipeline.StorageProviders.Tests;

public sealed class ChunkedUploadStreamTests
{
    private const int Part = 1024;

    [Fact]
    public async Task CommitAsync_ObjectSmallerThanOnePart_UsesSingleUpload()
    {
        var stream = new RecordingChunkedStream();
        await stream.WriteAsync(Bytes(10, 1));
        await stream.CommitAsync();
        await stream.DisposeAsync();

        stream.Single.Should().Equal(Bytes(10, 1));
        stream.Parts.Should().BeEmpty();
        stream.Begun.Should().BeFalse();
        stream.ETag.Should().Be("single");
    }

    [Fact]
    public async Task CommitAsync_ObjectExactlyOnePart_UsesSingleUpload()
    {
        var stream = new RecordingChunkedStream();
        await stream.WriteAsync(Bytes(Part, 7));
        await stream.CommitAsync();
        await stream.DisposeAsync();

        stream.Single.Should().HaveCount(Part);
        stream.Parts.Should().BeEmpty();
    }

    [Fact]
    public async Task CommitAsync_EmptyObject_UsesSingleUploadOfNothing()
    {
        var stream = new RecordingChunkedStream();
        await stream.CommitAsync();
        await stream.DisposeAsync();

        stream.Single.Should().BeEmpty();
    }

    [Fact]
    public async Task CommitAsync_ObjectLargerThanOnePart_UploadsPartsAndCompletes()
    {
        var stream = new RecordingChunkedStream();
        var expected = Bytes((Part * 2) + 100, 3);

        // Write in awkward slices so part boundaries fall inside a write.
        for (var i = 0; i < expected.Length; i += 333)
            await stream.WriteAsync(expected.AsMemory(i, Math.Min(333, expected.Length - i)));

        await stream.CommitAsync();
        await stream.DisposeAsync();

        stream.Begun.Should().BeTrue();
        stream.Single.Should().BeNull();
        stream.Parts.Keys.Should().BeEquivalentTo([1, 2, 3]);
        stream.Parts.OrderBy(p => p.Key).SelectMany(p => p.Value).Should().Equal(expected);
        stream.Offsets.OrderBy(p => p.Key).Select(p => p.Value).Should().Equal(0L, Part, Part * 2);
        stream.Completed.Should().Be((3, (long)expected.Length));
        stream.Aborted.Should().BeFalse();
    }

    [Fact]
    public async Task DisposeAsync_WithoutCommit_AbortsAndCompletesNothing()
    {
        var stream = new RecordingChunkedStream();
        await stream.WriteAsync(Bytes(Part * 2, 1));
        await stream.DisposeAsync();

        stream.Aborted.Should().BeTrue();
        stream.Completed.Should().BeNull();
        stream.Single.Should().BeNull();
    }

    [Fact]
    public async Task DisposeAsync_BeforeAnyPart_DoesNotAbort()
    {
        var stream = new RecordingChunkedStream();
        await stream.WriteAsync(Bytes(10, 1));
        await stream.DisposeAsync();

        stream.Aborted.Should().BeFalse();
        stream.Single.Should().BeNull();
    }

    [Fact]
    public async Task WriteAsync_WhenMaxPartsAreInFlight_WaitsForASlot()
    {
        var gate = new TaskCompletionSource();
        var stream = new RecordingChunkedStream(maxConcurrency: 2) { PartGate = gate.Task };

        // Parts 1 and 2 dispatch and block on the gate. The third full buffer needs a slot.
        await stream.WriteAsync(Bytes(Part * 2, 1));
        var third = stream.WriteAsync(Bytes(Part + 1, 2)).AsTask();
        await Task.Delay(100);
        third.IsCompleted.Should().BeFalse();

        gate.SetResult();
        await third.WaitAsync(TimeSpan.FromSeconds(5));
        await stream.CommitAsync();
        await stream.DisposeAsync();

        stream.MaxObservedConcurrency.Should().BeLessThanOrEqualTo(2);
    }

    [Fact]
    public async Task WriteAsync_AfterAPartFailed_ThrowsThatFailure()
    {
        var stream = new RecordingChunkedStream { FailPart = 1 };
        await stream.WriteAsync(Bytes(Part + 1, 1));

        var act = async () =>
        {
            // Give the failed part time to record its fault, then keep writing until a boundary is crossed.
            for (var i = 0; i < 50; i++)
            {
                await Task.Delay(10);
                await stream.WriteAsync(Bytes(Part, 1));
            }
        };

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("part 1 failed");
        await stream.DisposeAsync();
    }

    [Fact]
    public async Task CommitAsync_AfterAPartFailed_ThrowsThatFailureAndDoesNotComplete()
    {
        var stream = new RecordingChunkedStream { FailPart = 2 };
        await stream.WriteAsync(Bytes((Part * 2) + 5, 1));

        var act = () => stream.CommitAsync();

        await act.Should().ThrowAsync<InvalidOperationException>().WithMessage("part 2 failed");
        stream.Completed.Should().BeNull();
        await stream.DisposeAsync();
        stream.Aborted.Should().BeTrue();
    }

    [Fact]
    public async Task CommitAsync_Cancelled_CancelsInFlightParts()
    {
        var gate = new TaskCompletionSource();
        var stream = new RecordingChunkedStream { PartGate = gate.Task };
        await stream.WriteAsync(Bytes(Part + 1, 1));
        using var cts = new CancellationTokenSource();

        var commit = stream.CommitAsync(cts.Token);
        await cts.CancelAsync();

        await commit.Invoking(c => c).Should().ThrowAsync<OperationCanceledException>();
        await stream.DisposeAsync();
        stream.Completed.Should().BeNull();
    }

    [Fact]
    public async Task WriteAsync_AfterCommit_Throws()
    {
        var stream = new RecordingChunkedStream();
        await stream.CommitAsync();

        var act = async () => await stream.WriteAsync(Bytes(1, 1));

        await act.Should().ThrowAsync<InvalidOperationException>();
        await stream.DisposeAsync();
    }

    [Fact]
    public async Task CommitAsync_Twice_Throws()
    {
        var stream = new RecordingChunkedStream();
        await stream.CommitAsync();

        var act = () => stream.CommitAsync();

        await act.Should().ThrowAsync<InvalidOperationException>();
        await stream.DisposeAsync();
    }

    [Fact]
    public async Task PartSizeFor_Override_ChangesLaterPartSizes()
    {
        var stream = new RecordingChunkedStream { GrowAfterFirst = true };
        await stream.WriteAsync(Bytes(Part + (Part * 2) + 1, 1));
        await stream.CommitAsync();
        await stream.DisposeAsync();

        stream.Parts.OrderBy(p => p.Key).Select(p => p.Value.Length).Should().Equal(Part, Part * 2, 1);
    }

    private static byte[] Bytes(int count, byte seed)
    {
        var bytes = new byte[count];

        for (var i = 0; i < count; i++)
            bytes[i] = (byte)(seed + i);

        return bytes;
    }

    private sealed class RecordingChunkedStream : ChunkedUploadStream
    {
        private int _active;
        private int _maxActive;

        public RecordingChunkedStream(int maxConcurrency = 4)
            : base(Part, maxConcurrency)
        {
        }

        public ConcurrentDictionary<int, byte[]> Parts { get; } = new();
        public ConcurrentDictionary<int, long> Offsets { get; } = new();
        public byte[]? Single { get; private set; }
        public bool Begun { get; private set; }
        public bool Aborted { get; private set; }
        public (int Count, long Length)? Completed { get; private set; }
        public Task? PartGate { get; set; }
        public int FailPart { get; set; }
        public bool GrowAfterFirst { get; set; }
        public int MaxObservedConcurrency => Volatile.Read(ref _maxActive);

        protected override int PartSizeFor(int partNumber) => GrowAfterFirst && partNumber == 2 ? Part * 2 : Part;

        protected override Task BeginAsync(CancellationToken cancellationToken)
        {
            Begun = true;
            return Task.CompletedTask;
        }

        protected override async Task UploadPartAsync(int partNumber, long offset, ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            var active = Interlocked.Increment(ref _active);
            int seen;

            while ((seen = Volatile.Read(ref _maxActive)) < active && Interlocked.CompareExchange(ref _maxActive, active, seen) != seen)
            {
            }

            try
            {
                if (PartGate is not null)
                    await PartGate.WaitAsync(cancellationToken);

                if (partNumber == FailPart)
                    throw new InvalidOperationException($"part {partNumber} failed");

                Parts[partNumber] = data.ToArray();
                Offsets[partNumber] = offset;
            }
            finally
            {
                _ = Interlocked.Decrement(ref _active);
            }
        }

        protected override Task<string?> UploadSingleAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
        {
            Single = data.ToArray();
            return Task.FromResult<string?>("single");
        }

        protected override Task<string?> CompleteAsync(int partCount, long totalLength, CancellationToken cancellationToken)
        {
            Completed = (partCount, totalLength);
            return Task.FromResult<string?>("multi");
        }

        protected override Task AbortAsync()
        {
            Aborted = true;
            return Task.CompletedTask;
        }
    }
}
