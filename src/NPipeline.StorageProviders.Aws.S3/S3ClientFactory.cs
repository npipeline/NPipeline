using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using NPipeline.Connectors;
using System.Collections.Concurrent;
using System.Linq;

namespace NPipeline.StorageProviders.Aws.S3;

/// <summary>
/// Factory for creating and caching Amazon S3 clients with flexible authentication options.
/// </summary>
public class S3ClientFactory
{
    private readonly S3StorageProviderOptions _options;
    private readonly ConcurrentDictionary<string, IAmazonS3> _clientCache = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="S3ClientFactory"/> class.
    /// </summary>
    /// <param name="options">The S3 storage provider options.</param>
    public S3ClientFactory(S3StorageProviderOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <summary>
    /// Gets or creates an Amazon S3 client for the specified storage URI.
    /// </summary>
    /// <param name="uri">The storage URI containing bucket, region, and optional credentials.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing an <see cref="IAmazonS3"/> client.</returns>
    public virtual Task<IAmazonS3> GetClientAsync(StorageUri uri, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var credentials = GetCredentials(uri);
        var region = GetRegion(uri);
        var serviceUrl = GetServiceUrl(uri);
        var forcePathStyle = GetForcePathStyle(uri);

        return GetClientAsync(credentials, region, serviceUrl, forcePathStyle, cancellationToken);
    }

    /// <summary>
    /// Gets or creates an Amazon S3 client with the specified configuration.
    /// </summary>
    /// <param name="credentials">The AWS credentials to use.</param>
    /// <param name="region">The AWS region endpoint.</param>
    /// <param name="serviceUrl">Optional service URL for S3-compatible endpoints.</param>
    /// <param name="forcePathStyle">Whether to force path-style addressing.</param>
    /// <param name="cancellationToken">Token to observe while waiting for the task to complete.</param>
    /// <returns>A task producing an <see cref="IAmazonS3"/> client.</returns>
    public virtual Task<IAmazonS3> GetClientAsync(
        AWSCredentials? credentials,
        RegionEndpoint region,
        Uri? serviceUrl,
        bool forcePathStyle,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var cacheKey = BuildCacheKey(credentials, region, serviceUrl, forcePathStyle);

        var client = _clientCache.GetOrAdd(cacheKey, _ =>
        {
            var config = new AmazonS3Config
            {
                RegionEndpoint = region,
                ServiceURL = serviceUrl?.ToString(),
                ForcePathStyle = forcePathStyle
            };

            return credentials is null
                ? new AmazonS3Client(config)
                : new AmazonS3Client(credentials, config);
        });

        return Task.FromResult(client);
    }

    /// <summary>
    /// Extracts AWS credentials from the storage URI or returns default credentials.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>The AWS credentials, or null if using default credential chain.</returns>
    private AWSCredentials? GetCredentials(StorageUri uri)
    {
        var hasAccessKey = uri.Parameters.TryGetValue("accessKey", out var accessKey) && !string.IsNullOrWhiteSpace(accessKey);
        var hasSecretKey = uri.Parameters.TryGetValue("secretKey", out var secretKey) && !string.IsNullOrWhiteSpace(secretKey);
        var hasSessionToken = uri.Parameters.TryGetValue("sessionToken", out var sessionToken) && !string.IsNullOrWhiteSpace(sessionToken);

        if (hasAccessKey || hasSecretKey || hasSessionToken)
        {
            if (!hasAccessKey || !hasSecretKey)
            {
                throw new ArgumentException("Both accessKey and secretKey must be provided for explicit S3 credentials.", nameof(uri));
            }

            return hasSessionToken
                ? new SessionAWSCredentials(accessKey!, secretKey!, sessionToken!)
                : new BasicAWSCredentials(accessKey!, secretKey!);
        }

        if (_options.DefaultCredentials is not null)
        {
            return _options.DefaultCredentials;
        }

        if (!_options.UseDefaultCredentialChain)
        {
            throw new InvalidOperationException(
                "No AWS credentials available. Provide credentials via options, URI parameters, or enable the default credential chain.");
        }

        return null;
    }

    /// <summary>
    /// Extracts the AWS region from the storage URI or returns the default region.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>The AWS region endpoint.</returns>
    private RegionEndpoint GetRegion(StorageUri uri)
    {
        if (uri.Parameters.TryGetValue("region", out var regionString) &&
            !string.IsNullOrEmpty(regionString))
        {
            var normalized = regionString.Trim();
            var match = RegionEndpoint.EnumerableAllRegions
                .FirstOrDefault(r => string.Equals(r.SystemName, normalized, StringComparison.OrdinalIgnoreCase));

            if (match is null)
            {
                throw new ArgumentException($"Invalid AWS region: {regionString}", nameof(uri));
            }

            return match;
        }

        return _options.DefaultRegion ?? RegionEndpoint.USEast1;
    }

    /// <summary>
    /// Extracts the service URL from the storage URI or returns the default service URL.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>The service URL, or null if using AWS S3.</returns>
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
    /// Determines whether to force path-style addressing from the storage URI or options.
    /// </summary>
    /// <param name="uri">The storage URI.</param>
    /// <returns>True if path-style addressing should be forced; otherwise false.</returns>
    private bool GetForcePathStyle(StorageUri uri)
    {
        if (uri.Parameters.TryGetValue("pathStyle", out var pathStyleString) &&
            !string.IsNullOrEmpty(pathStyleString))
        {
            if (bool.TryParse(pathStyleString, out var pathStyle))
            {
                return pathStyle;
            }

            throw new ArgumentException($"Invalid pathStyle value: {pathStyleString}. Must be 'true' or 'false'.", nameof(uri));
        }

        return _options.ForcePathStyle;
    }

    /// <summary>
    /// Builds a cache key for the client configuration.
    /// </summary>
    private static string BuildCacheKey(
        AWSCredentials? credentials,
        RegionEndpoint region,
        Uri? serviceUrl,
        bool forcePathStyle)
    {
        var parts = new List<string>
        {
            region.SystemName,
            forcePathStyle.ToString(),
            serviceUrl?.ToString() ?? "default"
        };

        parts.Add(credentials is null ? "default" : BuildCredentialsKey(credentials));

        return string.Join("|", parts);
    }

    private static string BuildCredentialsKey(AWSCredentials credentials)
    {
        var immutable = credentials.GetCredentials();
        var hash = HashCode.Combine(
            credentials.GetType().FullName,
            immutable.AccessKey,
            immutable.SecretKey,
            immutable.Token);

        return hash.ToString();
    }
}
