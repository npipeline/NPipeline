using System.Net;
using System.Net.Sockets;

namespace NPipeline.StorageProviders.Adls.Tests;

/// <summary>A loopback HTTP server that answers 404 to everything and records each request's raw URL.</summary>
internal sealed class RecordingHttpServer : IDisposable
{
    private readonly HttpListener _listener = new();

    public RecordingHttpServer()
    {
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();

        BaseUrl = $"http://127.0.0.1:{port}/";
        _listener.Prefixes.Add(BaseUrl);
        _listener.Start();
        _ = AcceptLoopAsync();
    }

    public string BaseUrl { get; }

    public List<string> RawUrls { get; } = [];

    public void Dispose() => _listener.Close();

    private async Task AcceptLoopAsync()
    {
        while (_listener.IsListening)
        {
            HttpListenerContext context;

            try
            {
                context = await _listener.GetContextAsync();
            }
            catch (Exception) when (!_listener.IsListening)
            {
                return;
            }

            lock (RawUrls)
            {
                RawUrls.Add(context.Request.RawUrl ?? string.Empty);
            }

            context.Response.StatusCode = 404;
            context.Response.Close();
        }
    }
}
