using System.Collections.Concurrent;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using NPipeline.StorageProviders.Gcp.Reliability;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Gcp;

/// <summary>
///     Factory for creating and caching Google Cloud Storage clients. Credentials come from
///     <see cref="GcsStorageProviderOptions" /> only; a URI carries routing data (<c>serviceUrl</c>, <c>projectId</c>),
///     never secrets.
/// </summary>
/// <remarks>
///     The factory owns the clients it creates and disposes them in <see cref="Dispose()" />. Evicting a client from the
///     bounded cache does not dispose it, because a read or write stream opened earlier may still be using it.
/// </remarks>
public class GcsClientFactory : IDisposable, IAsyncDisposable
{
    private readonly ConcurrentDictionary<GcsClientKey, CacheEntry> _clients = new();
    private readonly object _evictionLock = new();
    private readonly GcsStorageProviderOptions _options;
    private long _clock;
    private int _disposed;

    /// <summary>
    ///     Initializes a new instance of the <see cref="GcsClientFactory" /> class.
    /// </summary>
    /// <param name="options">The GCS storage provider options.</param>
    public GcsClientFactory(GcsStorageProviderOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _options.Validate();
    }

    /// <summary>
    ///     Gets or creates a Google Cloud Storage client for the endpoint the URI names. The client is shared by every URI
    ///     with the same <c>serviceUrl</c> and <c>projectId</c>. Credentials come from
    ///     <see cref="GcsStorageProviderOptions.DefaultCredentials" />, then Application Default Credentials when
    ///     <see cref="GcsStorageProviderOptions.UseDefaultCredentials" /> is true.
    /// </summary>
    /// <param name="uri">The storage URI containing the bucket and optional routing parameters.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a <see cref="StorageClient" />.</returns>
    /// <exception cref="ArgumentException">
    ///     The URI carries an <c>accessToken</c> or <c>credentialsPath</c> parameter. Secrets do not belong in URIs; set
    ///     <see cref="GcsStorageProviderOptions.DefaultCredentials" /> instead.
    /// </exception>
    public virtual Task<StorageClient> GetClientAsync(
        StorageUri uri,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);
        cancellationToken.ThrowIfCancellationRequested();

        RejectCredentialParameters(uri);

        return GetClientAsync(GetServiceUrl(uri), GetProjectId(uri), cancellationToken);
    }

    /// <summary>
    ///     Gets or creates a Google Cloud Storage client for the specified endpoint.
    /// </summary>
    /// <param name="serviceUrl">Optional service URL for emulator or custom endpoints.</param>
    /// <param name="projectId">Optional project ID for operations that require project context.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a <see cref="StorageClient" />.</returns>
    public virtual Task<StorageClient> GetClientAsync(
        Uri? serviceUrl,
        string? projectId,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        // The key is non-secret endpoint data, so a hit is one dictionary lookup and one write of the recency stamp.
        var key = new GcsClientKey(serviceUrl?.AbsoluteUri, projectId);

        if (!_clients.TryGetValue(key, out var entry))
        {
            entry = _clients.GetOrAdd(key, static (_, state) => new CacheEntry(() => state.Factory.CreateClient(state.ServiceUrl)), (Factory: this, ServiceUrl: serviceUrl));
        }

        entry.LastUsed = Interlocked.Increment(ref _clock);

        try
        {
            var client = entry.Client.Value;

            if (_clients.Count > _options.ClientCacheSizeLimit)
                Evict(key);

            return Task.FromResult(client);
        }
        catch
        {
            // A Lazy caches its exception; drop the entry so the next call tries again.
            _ = _clients.TryRemove(new KeyValuePair<GcsClientKey, CacheEntry>(key, entry));
            throw;
        }
    }

    /// <summary>Disposes every client the factory created.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        Dispose();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    /// <summary>Disposes the cached clients.</summary>
    /// <param name="disposing"><see langword="true" /> when called from <see cref="Dispose()" />.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!disposing || Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        foreach (var key in _clients.Keys)
        {
            if (_clients.TryRemove(key, out var entry) && entry.Client.IsValueCreated)
                entry.Client.Value.Dispose();
        }
    }

    private StorageClient CreateClient(Uri? serviceUrl)
    {
        var builder = new StorageClientBuilder();

        if (_options.DefaultCredentials is not null)
            builder.Credential = _options.DefaultCredentials;
        else if (_options.UseDefaultCredentials)
        {
            try
            {
                builder.Credential = GoogleCredential.GetApplicationDefault();
            }

            // Google.Apis.Auth reports missing credentials as an AggregateException around InvalidOperationException.
            catch (Exception ex) when (ex is InvalidOperationException or AggregateException { InnerException: InvalidOperationException } &&
                                       ShouldUseEmulatorFallback(serviceUrl))
            {
                builder.Credential = GoogleCredential.FromAccessToken("owner");
            }
        }
        else
        {
            throw new InvalidOperationException(
                "No Google Cloud credentials available. " +
                "Set GcsStorageProviderOptions.DefaultCredentials, or enable UseDefaultCredentials for Application Default Credentials.");
        }

        // Set service URL for emulator or custom endpoints
        if (serviceUrl is not null)
            builder.BaseUri = serviceUrl.ToString();

        var newClient = builder.Build();

        // Retry layers, one per kind of request:
        //  - Metadata, list, delete and copy calls pass RetryOptions.Never (or are raw requests the SDK does not mark
        //    as retriable), so GcsStorageProviderOptions.Resilience is the only layer that retries them.
        //  - Reads resume under that same policy in ResumingReadStream.
        //  - Writes are a stream that cannot be replayed, so they retry inside the SDK's resumable-upload session: it
        //    queries the session for the committed offset and re-sends from there. That needs the handler's default
        //    NumTries, so it is deliberately left alone.
        // The handler below records Retry-After for the classifier; it never retries.
        newClient.Service.HttpClient.MessageHandler.AddUnsuccessfulResponseHandler(GcsRetryAfter.Handler);

        return newClient;
    }

    private void Evict(GcsClientKey keep)
    {
        lock (_evictionLock)
        {
            while (_clients.Count > _options.ClientCacheSizeLimit)
            {
                GcsClientKey? oldestKey = null;
                var oldest = long.MaxValue;

                foreach (var pair in _clients)
                {
                    if (pair.Key.Equals(keep))
                        continue;

                    var used = pair.Value.LastUsed;

                    if (used < oldest)
                    {
                        oldest = used;
                        oldestKey = pair.Key;
                    }
                }

                if (oldestKey is not { } victim || !_clients.TryRemove(victim, out _))
                    return;
            }
        }
    }

    private static void RejectCredentialParameters(StorageUri uri)
    {
        foreach (var name in new[] { "accessToken", "credentialsPath" })
        {
            if (uri.Parameters.ContainsKey(name))
            {
                throw new ArgumentException(
                    $"The '{name}' URI parameter is not supported: secrets do not belong in URIs. " +
                    "Set GcsStorageProviderOptions.DefaultCredentials instead.",
                    nameof(uri));
            }
        }
    }

    /// <summary>
    ///     Extracts the service URL from the storage URI or returns the default service URL.
    /// </summary>
    private Uri? GetServiceUrl(StorageUri uri)
    {
        if (uri.Parameters.TryGetValue("serviceUrl", out var serviceUrlString) &&
            !string.IsNullOrEmpty(serviceUrlString))
        {
            // StorageUri has already decoded the parameter; decoding again would corrupt values containing '%'.
            if (Uri.TryCreate(serviceUrlString, UriKind.Absolute, out var serviceUrl))
                return serviceUrl;

            throw new ArgumentException($"Invalid service URL: {serviceUrlString}", nameof(uri));
        }

        return _options.ServiceUrl;
    }

    /// <summary>
    ///     Extracts the project ID from the storage URI or returns the default project ID.
    /// </summary>
    private string? GetProjectId(StorageUri uri)
    {
        if (uri.Parameters.TryGetValue("projectId", out var projectId) &&
            !string.IsNullOrWhiteSpace(projectId))
            return projectId;

        return _options.DefaultProjectId;
    }

    private static bool ShouldUseEmulatorFallback(Uri? serviceUrl)
    {
        if (serviceUrl is not null)
            return true;

        var emulatorHost = Environment.GetEnvironmentVariable("STORAGE_EMULATOR_HOST");
        return !string.IsNullOrWhiteSpace(emulatorHost);
    }

    /// <summary>The non-secret data that identifies a client: the endpoint and the project.</summary>
    private readonly record struct GcsClientKey(string? ServiceUrl, string? ProjectId);

    private sealed class CacheEntry(Func<StorageClient> create)
    {
        private long _lastUsed;

        public Lazy<StorageClient> Client { get; } = new(create, LazyThreadSafetyMode.ExecutionAndPublication);

        public long LastUsed
        {
            get => Volatile.Read(ref _lastUsed);
            set => Volatile.Write(ref _lastUsed, value);
        }
    }
}
