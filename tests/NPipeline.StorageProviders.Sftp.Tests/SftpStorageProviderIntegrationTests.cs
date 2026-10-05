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

        items.Should().Contain(i => !i.IsDirectory && i.Uri.Path.EndsWith($"{leaf}/file.txt", StringComparison.Ordinal));
        items.Count(i => i.IsDirectory).Should().Be(5);
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
