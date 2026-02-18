using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.Storage.V1;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.Gcs;

/// <summary>
///     Factory for creating and caching Google Cloud Storage clients with flexible authentication options.
///     Implements credential resolution and client caching with size limits.
/// </summary>
public class GcsClientFactory
{
    private readonly ConcurrentDictionary<string, Lazy<StorageClient>> _clientCache = new();
    private readonly LinkedList<string> _cacheOrder = new();
    private readonly object _cacheLock = new();
    private readonly GcsStorageProviderOptions _options;

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
    ///     Gets or creates a Google Cloud Storage client for the specified storage URI.
    ///     Credentials are resolved in the following order:
    ///     1. URI parameters (accessToken, credentialsPath)
    ///     2. DefaultCredentials from options
    ///     3. Application Default Credentials (ADC) if UseDefaultCredentials is true
    /// </summary>
    /// <param name="uri">The storage URI containing bucket and optional credentials.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a <see cref="StorageClient" />.</returns>
    public virtual Task<StorageClient> GetClientAsync(
        StorageUri uri,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);
        cancellationToken.ThrowIfCancellationRequested();

        var credentials = GetCredentials(uri);
        var serviceUrl = GetServiceUrl(uri);
        var projectId = GetProjectId(uri);

        return GetClientAsync(credentials, serviceUrl, projectId, cancellationToken);
    }

    /// <summary>
    ///     Gets or creates a Google Cloud Storage client with the specified configuration.
    /// </summary>
    /// <param name="credentials">Optional Google credentials. If null and UseDefaultCredentials is true, ADC will be used.</param>
    /// <param name="serviceUrl">Optional service URL for emulator or custom endpoints.</param>
    /// <param name="projectId">Optional project ID for operations that require project context.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing a <see cref="StorageClient" />.</returns>
    public virtual Task<StorageClient> GetClientAsync(
        GoogleCredential? credentials,
        Uri? serviceUrl,
        string? projectId,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var cacheKey = BuildCacheKey(credentials, serviceUrl, projectId);

        var lazyClient = _clientCache.GetOrAdd(cacheKey, key =>
            new Lazy<StorageClient>(
                () => CreateClient(key, credentials, serviceUrl),
                LazyThreadSafetyMode.ExecutionAndPublication));

        try
        {
            return Task.FromResult(lazyClient.Value);
        }
        catch
        {
            _clientCache.TryRemove(new KeyValuePair<string, Lazy<StorageClient>>(cacheKey, lazyClient));

            lock (_cacheLock)
            {
                var node = _cacheOrder.Find(cacheKey);
                if (node is not null)
                {
                    _cacheOrder.Remove(node);
                }
            }

            throw;
        }
    }

    private StorageClient CreateClient(string cacheKey, GoogleCredential? credentials, Uri? serviceUrl)
    {
        var builder = new StorageClientBuilder();

        // Set credentials
        if (credentials is not null)
        {
            builder.Credential = credentials;
        }
        else if (_options.UseDefaultCredentials)
        {
            try
            {
                builder.Credential = GoogleCredential.GetApplicationDefault();
            }
            catch (InvalidOperationException) when (ShouldUseEmulatorFallback(serviceUrl))
            {
                builder.Credential = GoogleCredential.FromAccessToken("owner");
            }
        }
        else
        {
            throw new InvalidOperationException(
                "No Google Cloud credentials available. " +
                "Provide credentials via options, URI parameters, or enable UseDefaultCredentials for Application Default Credentials.");
        }

        // Set service URL for emulator or custom endpoints
        if (serviceUrl is not null)
        {
            builder.BaseUri = serviceUrl.ToString();
        }

        var newClient = builder.Build();
        OnClientCreated(cacheKey);
        return newClient;
    }

    private void OnClientCreated(string cacheKey)
    {
        lock (_cacheLock)
        {
            if (_cacheOrder.Find(cacheKey) is not null)
            {
                return;
            }

            if (_cacheOrder.Count >= _options.ClientCacheSizeLimit)
            {
                // Evict oldest entry
                if (_cacheOrder.First is not null)
                {
                    var oldestKey = _cacheOrder.First.Value;
                    _cacheOrder.RemoveFirst();
                    _clientCache.TryRemove(oldestKey, out _);
                }
            }

            _cacheOrder.AddLast(cacheKey);
        }
    }

    /// <summary>
    ///     Extracts Google credentials from the storage URI or returns default credentials.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>The Google credentials, or null if using default credentials.</returns>
    private GoogleCredential? GetCredentials(StorageUri uri)
    {
        // Check for access token in URI parameters
        if (uri.Parameters.TryGetValue("accessToken", out var accessToken) &&
            !string.IsNullOrWhiteSpace(accessToken))
        {
            return GoogleCredential.FromAccessToken(accessToken);
        }

        // Check for credentials file path in URI parameters
        if (uri.Parameters.TryGetValue("credentialsPath", out var credentialsPath) &&
            !string.IsNullOrWhiteSpace(credentialsPath))
        {
            var expandedPath = Environment.ExpandEnvironmentVariables(credentialsPath);

            if (!File.Exists(expandedPath))
            {
                throw new FileNotFoundException(
                    $"Google Cloud credentials file not found at path: {expandedPath}");
            }

            using var stream = File.OpenRead(expandedPath);
            return GoogleCredential.FromStream(stream);
        }

        // Return default credentials from options if available
        if (_options.DefaultCredentials is not null)
        {
            return _options.DefaultCredentials;
        }

        // Return null to indicate ADC should be used (if enabled)
        return null;
    }

    /// <summary>
    ///     Extracts the service URL from the storage URI or returns the default service URL.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>The service URL, or null if using the default GCS endpoint.</returns>
    private Uri? GetServiceUrl(StorageUri uri)
    {
        if (uri.Parameters.TryGetValue("serviceUrl", out var serviceUrlString) &&
            !string.IsNullOrEmpty(serviceUrlString))
        {
            var decoded = Uri.UnescapeDataString(serviceUrlString);

            if (Uri.TryCreate(decoded, UriKind.Absolute, out var serviceUrl))
            {
                return serviceUrl;
            }

            throw new ArgumentException($"Invalid service URL: {serviceUrlString}", nameof(uri));
        }

        return _options.ServiceUrl;
    }

    /// <summary>
    ///     Extracts the project ID from the storage URI or returns the default project ID.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>The project ID, or null if not specified.</returns>
    private string? GetProjectId(StorageUri uri)
    {
        if (uri.Parameters.TryGetValue("projectId", out var projectId) &&
            !string.IsNullOrWhiteSpace(projectId))
        {
            return projectId;
        }

        return _options.DefaultProjectId;
    }

    /// <summary>
    ///     Builds a cache key for the client configuration.
    /// </summary>
    private static string BuildCacheKey(
        GoogleCredential? credentials,
        Uri? serviceUrl,
        string? projectId)
    {
        var parts = new List<string>
        {
            serviceUrl?.ToString() ?? "default",
            projectId ?? "no-project",
        };

        // Use credential hash to avoid exposing sensitive data
        parts.Add(credentials is null
            ? "adc"
            : $"credential-{RuntimeHelpers.GetHashCode(credentials)}");

        return string.Join("|", parts);
    }

    private static bool ShouldUseEmulatorFallback(Uri? serviceUrl)
    {
        if (serviceUrl is not null)
            return true;

        var emulatorHost = Environment.GetEnvironmentVariable("STORAGE_EMULATOR_HOST");
        return !string.IsNullOrWhiteSpace(emulatorHost);
    }
}
