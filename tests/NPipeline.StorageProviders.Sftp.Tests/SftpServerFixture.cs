using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using NPipeline.StorageProviders.Models;
using Renci.SshNet;

namespace NPipeline.StorageProviders.Sftp.Tests;

/// <summary>An OpenSSH SFTP server (atmoz/sftp) with one user whose writable directory is <c>/upload</c>.</summary>
public sealed class SftpServerFixture : IAsyncLifetime
{
    public const string UserName = "tester";
    public const string Password = "secret";
    private const int SshPort = 22;

    private IContainer? _container;

    public string Host => _container!.Hostname;

    public int Port { get; private set; }

    /// <summary>The server's SHA-256 host key fingerprint, as SSH.NET reports it.</summary>
    public string HostKeyFingerprint { get; private set; } = string.Empty;

    public async Task InitializeAsync()
    {
        _container = new ContainerBuilder("atmoz/sftp:alpine")
            .WithPortBinding(SshPort, true)
            .WithCommand($"{UserName}:{Password}:::upload")
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Server listening on"))
            .WithLabel("npipeline-test", "sftp-integration")
            .Build();

        using var startCts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
        await _container.StartAsync(startCts.Token);
        Port = _container.GetMappedPublicPort(SshPort);

        using var client = new SftpClient(Host, Port, UserName, Password);
        client.HostKeyReceived += (_, e) => HostKeyFingerprint = e.FingerPrintSHA256;
        await client.ConnectAsync(startCts.Token);
        client.Disconnect();
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }

    /// <summary>Options that trust this server's host key.</summary>
    public SftpStorageProviderOptions CreateOptions(int maxPoolSize = 10) =>
        new()
        {
            HostKeyFingerprints = [$"SHA256:{HostKeyFingerprint}"],
            MaxPoolSize = maxPoolSize,
        };

    /// <summary>A URI under a fresh directory in the user's writable area.</summary>
    public StorageUri Uri(string path) => StorageUri.Parse($"sftp://{UserName}:{Password}@{Host}:{Port}/upload/{path.TrimStart('/')}");
}
