using System.Text;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Sftp.Tests;

public sealed class SftpStorageProviderIntegrationTests(SftpServerFixture server) : IClassFixture<SftpServerFixture>
{
    [Fact]
    public async Task OpenWriteAsync_OverLargerFile_Truncates()
    {
        using var factory = new SftpClientFactory(server.CreateOptions());
        var provider = new SftpStorageProvider(factory, server.CreateOptions());
        var uri = server.Uri($"{Guid.NewGuid():N}/file.txt");

        await WriteAsync(provider, uri, new string('x', 10_000));
        await WriteAsync(provider, uri, "short");

        (await ReadAsync(provider, uri)).Should().Be("short");
    }

    [Fact]
    public async Task ListAsync_RecursiveDeeperThanPool_Completes()
    {
        var options = server.CreateOptions(maxPoolSize: 2);
        using var factory = new SftpClientFactory(options);
        var provider = new SftpStorageProvider(factory, options);
        var root = $"{Guid.NewGuid():N}";
        var leaf = string.Join('/', Enumerable.Range(1, 5).Select(i => $"d{i}"));
        await WriteAsync(provider, server.Uri($"{root}/{leaf}/file.txt"), "deep");

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var items = new List<StorageItem>();

        await foreach (var item in provider.ListAsync(server.Uri(root), true, timeout.Token))
        {
            items.Add(item);
        }

        items.Should().ContainSingle().Which.Uri.Path.Should().EndWith($"{leaf}/file.txt");
        items.Should().NotContain(i => i.IsDirectory, "a recursive listing yields files only");
    }

    [Fact]
    public async Task ListAsync_NonRecursive_YieldsDirectChildrenIncludingDirectoriesWithNullPlaceholders()
    {
        var (provider, root) = Create();
        await WriteAsync(provider, server.Uri($"{root}/a.txt"), "a");
        await WriteAsync(provider, server.Uri($"{root}/sub/b.txt"), "b");

        var items = await provider.ListAsync(server.Uri($"{root}/")).ToListAsync();

        items.Should().HaveCount(2);
        var file = items.Single(i => !i.IsDirectory);
        file.Uri.Path.Should().EndWith($"{root}/a.txt");
        file.Uri.UserName.Should().Be(SftpServerFixture.UserName);
        file.Size.Should().Be(1);
        file.LastModified.Should().NotBeNull();

        var directory = items.Single(i => i.IsDirectory);
        directory.Uri.Path.Should().EndWith($"{root}/sub/");
        directory.Size.Should().BeNull();
        directory.LastModified.Should().BeNull();
    }

    [Fact]
    public async Task ListAsync_SiblingWithSharedPrefix_IsNotMatched()
    {
        var (provider, root) = Create();
        await WriteAsync(provider, server.Uri($"{root}/logs/a.txt"), "a");
        await WriteAsync(provider, server.Uri($"{root}/logs-archive/b.txt"), "b");

        var items = await provider.ListAsync(server.Uri($"{root}/logs"), true).ToListAsync();

        items.Should().ContainSingle().Which.Uri.Path.Should().EndWith("logs/a.txt");
    }

    [Fact]
    public async Task ListAsync_MissingDirectory_YieldsNothing()
    {
        var (provider, root) = Create();

        (await provider.ListAsync(server.Uri($"{root}/nope/")).ToListAsync()).Should().BeEmpty();
    }

    [Fact]
    public async Task OpenReadAsync_MissingFile_ThrowsFileNotFound()
    {
        var (provider, root) = Create();

        var act = () => provider.OpenReadAsync(server.Uri($"{root}/missing.txt"));

        await act.Should().ThrowAsync<FileNotFoundException>();
    }

    [Fact]
    public async Task GetMetadataAsync_MissingFile_ReturnsNull_AndExistsIsFalse()
    {
        var (provider, root) = Create();
        var uri = server.Uri($"{root}/missing.txt");

        (await provider.GetMetadataAsync(uri)).Should().BeNull();
        (await provider.ExistsAsync(uri)).Should().BeFalse();
    }

    [Fact]
    public async Task ExistsAsync_Directory_IsTrue()
    {
        var (provider, root) = Create();
        await WriteAsync(provider, server.Uri($"{root}/sub/a.txt"), "a");

        (await provider.ExistsAsync(server.Uri($"{root}/sub"))).Should().BeTrue();
    }

    [Fact]
    public async Task GetMetadataAsync_ExistingFile_ReportsSizeAndModifiedTime()
    {
        var (provider, root) = Create();
        var uri = server.Uri($"{root}/a.txt");
        await WriteAsync(provider, uri, "hello");

        var metadata = await provider.GetMetadataAsync(uri);

        metadata.Should().NotBeNull();
        metadata!.Size.Should().Be(5);
        metadata.LastModified.Should().BeCloseTo(DateTimeOffset.UtcNow, TimeSpan.FromMinutes(5));
    }

    [Fact]
    public async Task OpenWriteAsync_OutsideWritableArea_ThrowsUnauthorizedAccess()
    {
        var (provider, _) = Create();
        var uri = StorageUri.Parse($"sftp://{SftpServerFixture.UserName}:{SftpServerFixture.Password}@{server.Host}:{server.Port}/forbidden/file.txt");

        var act = () => provider.OpenWriteAsync(uri);

        await act.Should().ThrowAsync<UnauthorizedAccessException>();
    }

    [Fact]
    public async Task DeleteAsync_MissingFile_Succeeds()
    {
        var (provider, root) = Create();

        await provider.DeleteAsync(server.Uri($"{root}/missing.txt"));
    }

    [Fact]
    public async Task DeleteAsync_ExistingFile_RemovesIt()
    {
        var (provider, root) = Create();
        var uri = server.Uri($"{root}/a.txt");
        await WriteAsync(provider, uri, "a");

        await provider.DeleteAsync(uri);

        (await provider.ExistsAsync(uri)).Should().BeFalse();
    }

    [Fact]
    public async Task MoveAsync_MovesTheFileAndRemovesTheSource()
    {
        var (provider, root) = Create();
        var source = server.Uri($"{root}/old.txt");
        var destination = server.Uri($"{root}/nested/new.txt");
        await WriteAsync(provider, source, "payload");

        await provider.MoveAsync(source, destination);

        (await ReadAsync(provider, destination)).Should().Be("payload");
        (await provider.ExistsAsync(source)).Should().BeFalse();
    }

    [Fact]
    public async Task MoveAsync_OntoExistingFile_Overwrites()
    {
        var (provider, root) = Create();
        var source = server.Uri($"{root}/old.txt");
        var destination = server.Uri($"{root}/new.txt");
        await WriteAsync(provider, source, "fresh");
        await WriteAsync(provider, destination, "stale and longer");

        await provider.MoveAsync(source, destination);

        (await ReadAsync(provider, destination)).Should().Be("fresh");
    }

    [Fact]
    public async Task MoveAsync_MissingSource_ThrowsFileNotFound()
    {
        var (provider, root) = Create();

        var act = () => provider.MoveAsync(server.Uri($"{root}/missing.txt"), server.Uri($"{root}/new.txt"));

        await act.Should().ThrowAsync<FileNotFoundException>();
    }

    [Fact]
    public async Task ReadStream_DisposedAfterProvider_DoesNotThrow()
    {
        var options = server.CreateOptions();
        var factory = new SftpClientFactory(options);
        var provider = new SftpStorageProvider(factory, options);
        var uri = server.Uri($"{Guid.NewGuid():N}/a.txt");
        await WriteAsync(provider, uri, "a");

        var stream = await provider.OpenReadAsync(uri);
        await factory.DisposeAsync();

        var act = async () => await stream.DisposeAsync();

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task UserInformationPasswords_DoNotShareConnections()
    {
        var (provider, root) = Create();
        await WriteAsync(provider, server.Uri($"{root}/a.txt"), "a");
        var wrongPassword = StorageUri.Parse($"sftp://{SftpServerFixture.UserName}:wrong@{server.Host}:{server.Port}/upload/{root}/a.txt");

        var act = () => provider.OpenReadAsync(wrongPassword);

        await act.Should().ThrowAsync<UnauthorizedAccessException>();
    }

    private (SftpStorageProvider Provider, string Root) Create()
    {
        var options = server.CreateOptions();
        var factory = new SftpClientFactory(options);
        return (new SftpStorageProvider(factory, options), Guid.NewGuid().ToString("N"));
    }

    [Fact]
    public async Task Connect_ConfiguredFingerprint_Succeeds()
    {
        using var factory = new SftpClientFactory(server.CreateOptions());

        using var client = await factory.CreateClientAsync(server.Uri("x"), CancellationToken.None);

        client.IsConnected.Should().BeTrue();
    }

    [Fact]
    public async Task Connect_WrongFingerprint_Fails()
    {
        using var factory = new SftpClientFactory(new SftpStorageProviderOptions
        {
            HostKeyFingerprints = ["SHA256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
        });

        var act = () => factory.CreateClientAsync(server.Uri("x"), CancellationToken.None);

        await act.Should().ThrowAsync<Renci.SshNet.Common.SshConnectionException>();
    }

    private static async Task WriteAsync(SftpStorageProvider provider, StorageUri uri, string content)
    {
        await using var stream = await provider.OpenWriteAsync(uri);
        await stream.WriteAsync(Encoding.UTF8.GetBytes(content));
    }

    private static async Task<string> ReadAsync(SftpStorageProvider provider, StorageUri uri)
    {
        await using var stream = await provider.OpenReadAsync(uri);
        using var reader = new StreamReader(stream);
        return await reader.ReadToEndAsync();
    }
}
