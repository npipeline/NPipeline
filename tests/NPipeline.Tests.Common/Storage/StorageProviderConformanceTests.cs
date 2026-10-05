using System.Text;
using AwesomeAssertions;
using NPipeline.StorageProviders.Abstractions;
using NPipeline.StorageProviders.Exceptions;
using NPipeline.StorageProviders.Models;
using Xunit;

namespace NPipeline.Tests.Common.Storage;

/// <summary>
///     Behaviour every <see cref="IStorageProvider" /> must share. Subclass it once per provider, supply
///     <see cref="CreateProviderAsync" /> and <see cref="RootUri" />, and override the capability switches for what
///     the backend cannot represent. Container-backed subclasses skip themselves when Docker is unavailable.
/// </summary>
public abstract class StorageProviderConformanceTests : IAsyncLifetime
{
    private const int PartSize = 8 * 1024 * 1024;

    /// <summary>The provider under test, available after <see cref="InitializeAsync" />.</summary>
    protected IStorageProvider Provider { get; private set; } = null!;

    /// <summary>An empty, unique directory (or prefix) that this test instance may write to.</summary>
    protected abstract StorageUri RootUri { get; }

    /// <summary><see langword="false" /> when the backend rewrites <c>//</c>, <c>.</c> and <c>..</c> segments (a file system does).</summary>
    protected virtual bool PreservesNonCanonicalPaths => true;

    /// <summary>The longest single path segment the backend accepts.</summary>
    protected virtual int MaxSegmentLength => 1024;

    /// <summary>The length of the key used by the long-key case. Local file systems limit the whole path, so they lower it.</summary>
    protected virtual int LongKeyLength => 1000;

    /// <summary><see langword="true" /> when the provider can list <see cref="RootUri" />.</summary>
    protected virtual bool SupportsList => true;

    /// <summary>Creates the provider. It is called once per test instance, before <see cref="RootUri" /> is read.</summary>
    protected abstract Task<IStorageProvider> CreateProviderAsync();

    /// <summary>Releases backend resources created for the test instance.</summary>
    protected virtual Task CleanupAsync() => Task.CompletedTask;

    public async Task InitializeAsync() => Provider = await CreateProviderAsync().ConfigureAwait(false);

    public Task DisposeAsync() => CleanupAsync();

    public static TheoryData<string> ObjectNames() =>
    [
        "plain.csv",
        "with space.csv",
        "hash#1.csv",
        "percent%sign.csv",
        "escaped%20space.csv",
        "plus+sign.csv",
        "amp&equals=.csv",
        "dots..in..name.csv",
        "ünïcödé-日本語.csv",
    ];

    [Theory]
    [MemberData(nameof(ObjectNames))]
    public async Task RoundTrip_ObjectName_WritesAndReadsTheSameObject(string name)
    {
        var uri = RootUri.Combine(name);
        var content = Encoding.UTF8.GetBytes($"content of {name}");

        await WriteAsync(uri, content);

        (await ReadAsync(uri)).Should().Equal(content);
        (await Provider.ExistsAsync(uri)).Should().BeTrue();
    }

    [Fact]
    public async Task RoundTrip_NameSurvivesToStringAndParse()
    {
        var uri = RootUri.Combine("a b%20#c+d.csv");
        await WriteAsync(uri, [1, 2, 3]);

        var reparsed = StorageUri.Parse(uri.ToUnredactedString());

        reparsed.Should().Be(uri);
        (await ReadAsync(reparsed)).Should().Equal(1, 2, 3);
    }

    [Fact]
    public async Task RoundTrip_LongKey_Works()
    {
        var segmentLength = Math.Min(MaxSegmentLength, 199);
        var key = new StringBuilder();

        for (var i = 0; key.Length < LongKeyLength; i++)
        {
            key.Append(i == 0 ? "" : "/").Append((char)('a' + i % 26), segmentLength);
        }

        key.Length = LongKeyLength;
        key[^1] = 'z';

        var uri = RootUri.Combine(key.ToString());

        await WriteAsync(uri, [42]);

        (await ReadAsync(uri)).Should().Equal(42);
    }

    [Fact]
    public async Task RoundTrip_NonCanonicalSegments_AreKeptVerbatim()
    {
        if (!PreservesNonCanonicalPaths)
            return;

        var doubleSlash = RootUri.Combine("a//b.csv");
        var dotDot = RootUri.Combine("d/../e.csv");

        await WriteAsync(doubleSlash, [1]);
        await WriteAsync(dotDot, [2]);

        (await ReadAsync(doubleSlash)).Should().Equal(1);
        (await ReadAsync(dotDot)).Should().Equal(2);
        (await Provider.ExistsAsync(RootUri.Combine("e.csv"))).Should().BeFalse();
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(PartSize)]
    [InlineData(PartSize + 1)]
    public async Task WriteThenRead_ReturnsExactBytes(int length)
    {
        var uri = RootUri.Combine($"size-{length}.bin");
        var content = Pattern(length);

        await WriteAsync(uri, content);

        (await ReadAsync(uri)).Should().Equal(content);
    }

    [Fact]
    public async Task Overwrite_WithSmallerContent_ResultIsExactlyTheSmallerContent()
    {
        var uri = RootUri.Combine("overwrite.bin");

        await WriteAsync(uri, Pattern(10_000));
        await WriteAsync(uri, Pattern(10));

        (await ReadAsync(uri)).Should().Equal(Pattern(10));
    }

    [Fact]
    public async Task Exists_MissingObject_IsFalse()
    {
        (await Provider.ExistsAsync(RootUri.Combine("missing.bin"))).Should().BeFalse();
    }

    [Fact]
    public async Task List_YieldedUris_ParseBackAndOpen()
    {
        if (!SupportsList)
            return;

        await WriteAsync(RootUri.Combine("listed a b#1.csv"), [7]);

        var items = new List<StorageItem>();

        await foreach (var item in Provider.ListAsync(RootUri, true))
        {
            items.Add(item);
        }

        var file = items.Single(i => !i.IsDirectory).Uri;

        StorageUri.Parse(file.ToUnredactedString()).Should().Be(file);
        file.Path.Should().EndWith("/listed a b#1.csv");
        (await ReadAsync(file)).Should().Equal(7);
    }

    [Fact]
    public async Task OpenRead_MissingObject_ThrowsFileNotFound()
    {
        var act = () => Provider.OpenReadAsync(RootUri.Combine("missing.bin"));

        await act.Should().ThrowAsync<FileNotFoundException>();
    }

    [Fact]
    public async Task GetMetadata_MissingObject_ReturnsNull()
    {
        (await Provider.GetMetadataAsync(RootUri.Combine("missing.bin"))).Should().BeNull();
    }

    [Fact]
    public async Task GetMetadata_ExistingObject_ReportsTheSize()
    {
        var uri = RootUri.Combine("metadata.bin");
        await WriteAsync(uri, Pattern(123));

        var metadata = await Provider.GetMetadataAsync(uri);

        metadata.Should().NotBeNull();
        metadata!.Size.Should().Be(123);
        metadata.IsDirectory.Should().BeFalse();
    }

    [Fact]
    public async Task Delete_MissingObject_Succeeds()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.Delete))
            return;

        await Provider.DeleteAsync(RootUri.Combine("missing.bin"));
    }

    [Fact]
    public async Task Delete_ExistingObject_RemovesIt()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.Delete))
            return;

        var uri = RootUri.Combine("delete-me.bin");
        await WriteAsync(uri, [1]);

        await Provider.DeleteAsync(uri);

        (await Provider.ExistsAsync(uri)).Should().BeFalse();
    }

    [Fact]
    public async Task Move_ExistingDestination_IsOverwritten()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.Move))
            return;

        var source = RootUri.Combine("move-source.bin");
        var destination = RootUri.Combine("moved/destination.bin");
        await WriteAsync(source, [1, 2, 3]);
        await WriteAsync(destination, Pattern(50));

        await Provider.MoveAsync(source, destination);

        (await ReadAsync(destination)).Should().Equal(1, 2, 3);
        (await Provider.ExistsAsync(source)).Should().BeFalse();
    }

    [Fact]
    public async Task Move_MissingSource_ThrowsFileNotFound()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.Move))
            return;

        var act = () => Provider.MoveAsync(RootUri.Combine("missing.bin"), RootUri.Combine("elsewhere.bin"));

        await act.Should().ThrowAsync<FileNotFoundException>();
    }

    [Fact]
    public async Task List_NonRecursive_YieldsDirectChildrenAndDirectories()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.List) || !SupportsList)
            return;

        await WriteListingLayoutAsync();

        var items = await ListAsync(RootUri.Combine("logs/"), false);

        items.Where(i => !i.IsDirectory).Select(i => i.Uri.Name).Should().BeEquivalentTo("a.csv");
        items.Where(i => i.IsDirectory).Select(i => i.Uri.Path.TrimEnd('/').Split('/')[^1]).Should().BeEquivalentTo("sub");
    }

    [Fact]
    public async Task List_Recursive_YieldsEveryFileAndNoDirectories()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.List) || !SupportsList)
            return;

        await WriteListingLayoutAsync();

        var items = await ListAsync(RootUri.Combine("logs/"), true);

        items.Should().OnlyContain(i => !i.IsDirectory);
        items.Select(i => i.Uri.Name).Should().BeEquivalentTo("a.csv", "b.csv");
    }

    [Fact]
    public async Task List_DirectoryWithoutTrailingSlash_DoesNotMatchSiblingPrefix()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.List) || !SupportsList)
            return;

        await WriteListingLayoutAsync();

        var items = await ListAsync(RootUri.Combine("logs"), true);

        items.Select(i => i.Uri.Name).Should().BeEquivalentTo("a.csv", "b.csv");
    }

    [Fact]
    public async Task List_MissingDirectory_YieldsNothing()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.List) || !SupportsList)
            return;

        (await ListAsync(RootUri.Combine("no-such-directory/"), true)).Should().BeEmpty();
        (await ListAsync(RootUri.Combine("no-such-directory/"), false)).Should().BeEmpty();
    }

    [Fact]
    public async Task List_YieldedUris_KeepTheCallersParameters()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.List) || !SupportsList)
            return;

        await WriteListingLayoutAsync();
        var directory = RootUri.Combine("logs/").WithParameter("marker", "kept");

        var items = await ListAsync(directory, true);

        items.Should().NotBeEmpty();
        items.Should().OnlyContain(i => i.Uri.Parameters.ContainsKey("marker"));
    }

    [Fact]
    public async Task Capabilities_UndeclaredOperations_ThrowUnsupportedStorageCapability()
    {
        var uri = RootUri.Combine("capability.bin");
        var declared = Provider.Capabilities;

        if (!declared.HasFlag(StorageCapabilities.Read))
        {
            await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => Provider.OpenReadAsync(uri));
            await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => Provider.GetMetadataAsync(uri));
            await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => Provider.ExistsAsync(uri));
        }

        if (!declared.HasFlag(StorageCapabilities.Write))
            await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => Provider.OpenWriteAsync(uri));

        if (!declared.HasFlag(StorageCapabilities.List))
            Assert.Throws<UnsupportedStorageCapabilityException>(() => Provider.ListAsync(uri));

        if (!declared.HasFlag(StorageCapabilities.Delete))
            await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => Provider.DeleteAsync(uri));

        if (!declared.HasFlag(StorageCapabilities.Move))
            await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => Provider.MoveAsync(uri, uri));

        if (!declared.HasFlag(StorageCapabilities.ConditionalWrite))
            await Assert.ThrowsAsync<UnsupportedStorageCapabilityException>(() => Provider.OpenWriteAsync(uri, new StorageWriteOptions { Overwrite = false }));
    }

    [Fact]
    public async Task CancelledToken_ThrowsOperationCanceled()
    {
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();
        var token = cancelled.Token;
        var uri = RootUri.Combine("cancelled.bin");
        var declared = Provider.Capabilities;

        if (declared.HasFlag(StorageCapabilities.Read))
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Provider.OpenReadAsync(uri, token));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Provider.GetMetadataAsync(uri, token));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Provider.ExistsAsync(uri, token));
        }

        if (declared.HasFlag(StorageCapabilities.Write))
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Provider.OpenWriteAsync(uri, null, token));

        if (declared.HasFlag(StorageCapabilities.Delete))
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Provider.DeleteAsync(uri, token));

        if (declared.HasFlag(StorageCapabilities.Move))
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Provider.MoveAsync(uri, uri, token));

        if (declared.HasFlag(StorageCapabilities.List))
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await ListAsync(RootUri, true, token));
    }

    [Fact]
    public async Task Write_DisposedWithoutCommit_LeavesNothingAtTheTarget()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.Write))
            return;

        var uri = RootUri.Combine("abandoned.bin");

        await using (var stream = await Provider.OpenWriteAsync(uri))
        {
            await stream.WriteAsync(Pattern(1024));
        }

        (await Provider.ExistsAsync(uri)).Should().BeFalse("an uncommitted write must never become visible");
        (await Provider.GetMetadataAsync(uri)).Should().BeNull();
    }

    [Fact]
    public async Task Write_DisposedWithoutCommit_KeepsTheExistingObject()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.Write))
            return;

        var uri = RootUri.Combine("existing.bin");
        var original = Pattern(512);
        await WriteAsync(uri, original);

        await using (var stream = await Provider.OpenWriteAsync(uri))
        {
            await stream.WriteAsync(Pattern(4096));
        }

        (await ReadAsync(uri)).Should().Equal(original);
    }

    [Fact]
    public async Task Write_Committed_ReplacesTheExistingObject()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.Write))
            return;

        var uri = RootUri.Combine("replaced.bin");
        await WriteAsync(uri, Pattern(10_000));

        var replacement = Pattern(100);
        await WriteAsync(uri, replacement);

        (await ReadAsync(uri)).Should().Equal(replacement, "the new content must not leave the old object's tail behind");
    }

    [Fact]
    public async Task Write_CommittedTwice_Throws()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.Write))
            return;

        await using var stream = await Provider.OpenWriteAsync(RootUri.Combine("twice.bin"));
        await stream.WriteAsync(Pattern(16));
        await stream.CommitAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => stream.CommitAsync());
    }

    [Fact]
    public async Task Write_Committed_LeavesNoTemporaryObjectsBehind()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.Write) || !SupportsList)
            return;

        await WriteAsync(RootUri.Combine("only.bin"), Pattern(64));

        var names = (await ListAsync(RootUri, true)).Select(i => i.Uri.Name).ToList();
        names.Should().Equal("only.bin");
    }

    [Fact]
    public async Task Write_OverwriteFalse_RefusesAnExistingObject()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.ConditionalWrite))
            return;

        var uri = RootUri.Combine("create-only.bin");
        await WriteAsync(uri, Pattern(8));

        await using var stream = await Provider.OpenWriteAsync(uri, new StorageWriteOptions { Overwrite = false });
        await stream.WriteAsync(Pattern(16));

        await Assert.ThrowsAsync<StoragePreconditionFailedException>(() => stream.CommitAsync());
        (await ReadAsync(uri)).Should().Equal(Pattern(8));
    }

    [Fact]
    public async Task Write_OverwriteFalse_CreatesAMissingObject()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.ConditionalWrite))
            return;

        var uri = RootUri.Combine("create-new.bin");

        await using (var stream = await Provider.OpenWriteAsync(uri, new StorageWriteOptions { Overwrite = false }))
        {
            await stream.WriteAsync(Pattern(32));
            await stream.CommitAsync();
        }

        (await ReadAsync(uri)).Should().Equal(Pattern(32));
    }

    [Fact]
    public async Task Write_IfMatch_CommitsOnlyWhileTheETagIsCurrent()
    {
        if (!Provider.Capabilities.HasFlag(StorageCapabilities.ConditionalWrite))
            return;

        var uri = RootUri.Combine("if-match.bin");
        await WriteAsync(uri, Pattern(8));
        var etag = (await Provider.GetMetadataAsync(uri))!.ETag;
        etag.Should().NotBeNullOrEmpty();

        await using (var current = await Provider.OpenWriteAsync(uri, new StorageWriteOptions { IfMatch = etag }))
        {
            await current.WriteAsync(Pattern(16));
            await current.CommitAsync();
        }

        // The first commit changed the object, so the same ETag is now stale.
        await using var stale = await Provider.OpenWriteAsync(uri, new StorageWriteOptions { IfMatch = etag });
        await stale.WriteAsync(Pattern(24));

        await Assert.ThrowsAsync<StoragePreconditionFailedException>(() => stale.CommitAsync());
        (await ReadAsync(uri)).Should().Equal(Pattern(16));
    }

    private async Task WriteListingLayoutAsync()
    {
        await WriteAsync(RootUri.Combine("logs/a.csv"), [1]);
        await WriteAsync(RootUri.Combine("logs/sub/b.csv"), [2]);
        await WriteAsync(RootUri.Combine("logs-archive/c.csv"), [3]);
    }

    private async Task<List<StorageItem>> ListAsync(StorageUri directory, bool recursive, CancellationToken cancellationToken = default)
    {
        var items = new List<StorageItem>();

        await foreach (var item in Provider.ListAsync(directory, recursive, cancellationToken))
        {
            items.Add(item);
        }

        return items;
    }

    private async Task WriteAsync(StorageUri uri, byte[] content)
    {
        await using var stream = await Provider.OpenWriteAsync(uri);
        await stream.WriteAsync(content);
        await stream.CommitAsync();
    }

    private async Task<byte[]> ReadAsync(StorageUri uri)
    {
        await using var stream = await Provider.OpenReadAsync(uri);
        using var buffer = new MemoryStream();
        await stream.CopyToAsync(buffer);
        return buffer.ToArray();
    }

    private static byte[] Pattern(int length)
    {
        var bytes = new byte[length];

        for (var i = 0; i < bytes.Length; i++)
        {
            bytes[i] = (byte)(i * 31 + 7);
        }

        return bytes;
    }
}
