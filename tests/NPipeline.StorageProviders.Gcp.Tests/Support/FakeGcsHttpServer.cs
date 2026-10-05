using System.Net;
using System.Net.Sockets;
using System.Text;
using Google.Apis.Auth.OAuth2;
using NPipeline.StorageProviders.Models;
using NResilience;

namespace NPipeline.StorageProviders.Gcp.Tests.Support;

/// <summary>
///     A loopback HTTP server that stands in for GCS. Every request is handled on its own task, so a handler can stall
///     one response while the test keeps going. <see cref="UploadSession" /> speaks enough of the resumable-upload
///     protocol for the Google SDK to run a complete upload against it.
/// </summary>
internal sealed class FakeGcsHttpServer : IDisposable
{
    private readonly Func<HttpListenerContext, int, Task> _handler;
    private readonly HttpListener _listener = new();
    private readonly List<RecordedRequest> _requests = [];
    private int _count;

    public FakeGcsHttpServer(Func<HttpListenerContext, int, Task> handler)
    {
        _handler = handler;
        var port = FreePort();
        BaseUri = new Uri($"http://127.0.0.1:{port}/storage/v1/");
        _listener.Prefixes.Add($"http://127.0.0.1:{port}/");
        _listener.Start();
        _ = Task.Run(ServeAsync);
    }

    public Uri BaseUri { get; }

    public IReadOnlyList<RecordedRequest> Requests
    {
        get
        {
            lock (_requests)
            {
                return [.. _requests];
            }
        }
    }

    public void Dispose()
    {
        _listener.Abort();
    }

    public GcsStorageProvider CreateProvider(Resilience resilience, int chunkSizeBytes = 16 * 1024 * 1024)
    {
        var options = new GcsStorageProviderOptions
        {
            ServiceUrl = BaseUri,
            DefaultCredentials = GoogleCredential.FromAccessToken("test-token"),
            Resilience = resilience,
            UploadChunkSizeBytes = chunkSizeBytes,
        };

        return new GcsStorageProvider(new GcsClientFactory(options), options);
    }

    public static StorageUri Uri(string path = "test-bucket/test-object.txt") => StorageUri.Parse($"gs://{path}");

    public static async Task WriteJsonAsync(HttpListenerResponse response, HttpStatusCode status, string body)
    {
        response.StatusCode = (int)status;
        response.ContentType = "application/json";
        var bytes = Encoding.UTF8.GetBytes(body);
        response.ContentLength64 = bytes.Length;
        await response.OutputStream.WriteAsync(bytes);
        response.Close();
    }

    public static Task WriteErrorAsync(HttpListenerResponse response, HttpStatusCode status) =>
        WriteJsonAsync(response, status, "{\"error\":{\"code\":" + (int)status + ",\"message\":\"scripted failure\"}}");

    private async Task ServeAsync()
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

            var request = context.Request;

            lock (_requests)
            {
                _requests.Add(new RecordedRequest(
                    request.HttpMethod,
                    request.Url!.PathAndQuery,
                    request.Headers["Range"],
                    request.Headers["Content-Range"]));
            }

            var n = Interlocked.Increment(ref _count);

            _ = Task.Run(async () =>
            {
                try
                {
                    await _handler(context, n);
                }
                catch (Exception)
                {
                    // The test tore the server down while a handler was running.
                }
            });
        }
    }

    private static int FreePort()
    {
        using var socket = new TcpListener(IPAddress.Loopback, 0);
        socket.Start();
        return ((IPEndPoint)socket.LocalEndpoint).Port;
    }

    internal sealed record RecordedRequest(string Method, string PathAndQuery, string? Range, string? ContentRange);

    /// <summary>One resumable upload: starts on POST, takes chunks on PUT, and finishes on the chunk that carries the total.</summary>
    internal sealed class UploadSession
    {
        private static readonly uint[] Table = BuildTable();
        private readonly object _gate = new();
        private uint _crc = 0xFFFFFFFFu;

        /// <summary>Decides a chunk's outcome before it is stored; return a status to fail it.</summary>
        public Func<HttpListenerContext, int, Task<HttpStatusCode?>>? BeforeChunk { get; init; }

        /// <summary>Called with the running byte count each time a chunk body has been read.</summary>
        public Action<long>? OnBytesReceived { get; init; }

        public long Received { get; private set; }

        public bool Finalized { get; private set; }

        public int ChunkRequests { get; private set; }

        public async Task HandleAsync(HttpListenerContext context, int n)
        {
            var request = context.Request;

            if (request.HttpMethod == "POST")
            {
                // Starting the session: point the client at it.
                await DrainAsync(request.InputStream);
                context.Response.Headers["Location"] = $"http://127.0.0.1:{request.Url!.Port}/session/{Guid.NewGuid():N}";
                await WriteJsonAsync(context.Response, HttpStatusCode.OK, "{}");
                return;
            }

            var contentRange = request.Headers["Content-Range"] ?? string.Empty;

            if (BeforeChunk is not null && contentRange.StartsWith("bytes */", StringComparison.Ordinal) == false)
            {
                var failure = await BeforeChunk(context, n);

                if (failure is { } status)
                {
                    await DrainAsync(request.InputStream);
                    await WriteErrorAsync(context.Response, status);
                    return;
                }
            }

            if (contentRange.StartsWith("bytes */", StringComparison.Ordinal))
            {
                await DrainAsync(request.InputStream);
                await RespondAsync(context.Response, contentRange.EndsWith("/*", StringComparison.Ordinal) ? null : long.Parse(contentRange[8..]));
                return;
            }

            // "bytes {first}-{last}/{total|*}"
            var slash = contentRange.LastIndexOf('/');
            var total = contentRange[(slash + 1)..];
            var count = await ConsumeAsync(request.InputStream);
            long? knownTotal = total == "*" ? null : long.Parse(total);

            lock (_gate)
            {
                ChunkRequests++;
            }

            OnBytesReceived?.Invoke(Received);
            await RespondAsync(context.Response, knownTotal);
            _ = count;
        }

        private async Task<long> ConsumeAsync(Stream body)
        {
            var buffer = new byte[81920];
            long total = 0;
            int read;

            while ((read = await body.ReadAsync(buffer)) > 0)
            {
                lock (_gate)
                {
                    for (var i = 0; i < read; i++)
                    {
                        _crc = Table[(_crc ^ buffer[i]) & 0xFF] ^ (_crc >> 8);
                    }

                    Received += read;
                }

                total += read;
            }

            return total;
        }

        private static async Task DrainAsync(Stream body)
        {
            var buffer = new byte[81920];

            while (await body.ReadAsync(buffer) > 0)
            {
            }
        }

        private async Task RespondAsync(HttpListenerResponse response, long? total)
        {
            if (total is { } t && t == Received)
            {
                Finalized = true;
                var crc = ~_crc;
                var checksum = Convert.ToBase64String([(byte)(crc >> 24), (byte)(crc >> 16), (byte)(crc >> 8), (byte)crc]);

                await WriteJsonAsync(
                    response,
                    HttpStatusCode.OK,
                    "{\"kind\":\"storage#object\",\"bucket\":\"test-bucket\",\"name\":\"test-object.txt\",\"etag\":\"etag-1\"," +
                    $"\"size\":\"{Received}\",\"crc32c\":\"{checksum}\"}}");

                return;
            }

            // 308 Resume Incomplete, with the range the server holds.
            response.StatusCode = 308;

            if (Received > 0)
                response.Headers["Range"] = $"bytes=0-{Received - 1}";

            response.ContentLength64 = 0;
            response.Close();
        }

        private static uint[] BuildTable()
        {
            var table = new uint[256];

            for (uint i = 0; i < 256; i++)
            {
                var crc = i;

                for (var bit = 0; bit < 8; bit++)
                {
                    crc = (crc & 1) != 0 ? (crc >> 1) ^ 0x82F63B78u : crc >> 1;
                }

                table[i] = crc;
            }

            return table;
        }
    }
}
