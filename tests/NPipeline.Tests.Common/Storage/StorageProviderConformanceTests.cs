using System.Text;
using AwesomeAssertions;
using NPipeline.StorageProviders.Abstractions;
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

    private async Task WriteAsync(StorageUri uri, byte[] content)
    {
        await using var stream = await Provider.OpenWriteAsync(uri);
        await stream.WriteAsync(content);
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
