using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using NPipeline.StorageProviders.Models;
using Renci.SshNet.Common;

namespace NPipeline.StorageProviders.Sftp.Tests.Reliability.Behavior;

/// <summary>
///     Connects to a TCP listener that accepts the socket but never sends an SSH banner, so the connect hangs in the
///     handshake until it is cancelled or times out.
/// </summary>
public sealed class SftpConnectCancellationTests : IDisposable
{
    private readonly TcpListener _listener;
    private readonly List<Socket> _accepted = [];
    private int _acceptCount;

    public SftpConnectCancellationTests()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _ = AcceptLoopAsync();
    }

    private int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public void Dispose()
    {
        _listener.Stop();

        lock (_accepted)
        {
            foreach (var socket in _accepted)
            {
                socket.Dispose();
            }
        }
    }

    [Fact]
    public async Task CreateClientAsync_CallerCancelsDuringHandshake_StopsPromptly()
    {
        using var factory = new SftpClientFactory(new SftpStorageProviderOptions
        {
            ConnectionTimeout = TimeSpan.FromSeconds(30),
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(300));
        var stopwatch = Stopwatch.StartNew();

        var act = () => factory.CreateClientAsync(SilentServerUri(), cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        stopwatch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(10), "the running connect observes the caller's token");
        Volatile.Read(ref _acceptCount).Should().Be(1, "the connect had started before it was cancelled");
    }

    [Fact]
    public async Task CreateClientAsync_ConnectionTimeoutElapses_FailsWithTimeoutNotCancellation()
    {
        using var factory = new SftpClientFactory(new SftpStorageProviderOptions
        {
            ConnectionTimeout = TimeSpan.FromMilliseconds(500),
        });

        var stopwatch = Stopwatch.StartNew();

        var act = () => factory.CreateClientAsync(SilentServerUri(), CancellationToken.None);

        _ = await act.Should().ThrowAsync<SshOperationTimeoutException>();
        stopwatch.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task CreateClientAsync_AlreadyCancelled_DoesNotConnect()
    {
        using var factory = new SftpClientFactory(new SftpStorageProviderOptions());
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var act = () => factory.CreateClientAsync(SilentServerUri(), cts.Token);

        _ = await act.Should().ThrowAsync<OperationCanceledException>();
        await Task.Delay(100);
        Volatile.Read(ref _acceptCount).Should().Be(0);
    }

    private StorageUri SilentServerUri() => StorageUri.Parse($"sftp://user@127.0.0.1:{Port}/file.txt?password=secret");

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (true)
            {
                var socket = await _listener.AcceptSocketAsync();

                lock (_accepted)
                {
                    _accepted.Add(socket);
                }

                _ = Interlocked.Increment(ref _acceptCount);
            }
        }
        catch (Exception ex) when (ex is SocketException or ObjectDisposedException)
        {
            // Listener stopped
        }
    }
}
