using Amazon;
using Amazon.S3;
using NPipeline.StorageProviders.Models;

namespace NPipeline.StorageProviders.S3.Aws;

/// <summary>
///     Factory for creating and caching Amazon S3 clients. The URI selects the endpoint (<c>region</c>, <c>serviceUrl</c>,
///     <c>pathStyle</c>); credentials come from <see cref="AwsS3StorageProviderOptions" />, or from the SDK's default
///     credential chain, and are never read from a URI.
/// </summary>
public class AwsS3ClientFactory : S3ClientFactoryBase
{
    private static readonly string[] CredentialParameters = ["accessKey", "secretKey", "sessionToken"];

    private static readonly Lazy<HashSet<string>> KnownRegions = new(
        () => RegionEndpoint.EnumerableAllRegions.Select(r => r.SystemName).ToHashSet(StringComparer.OrdinalIgnoreCase));

    private readonly AwsS3StorageProviderOptions _options;

    /// <summary>
    ///     Initializes a new instance of the <see cref="AwsS3ClientFactory" /> class.
    /// </summary>
    /// <param name="options">The AWS S3 storage provider options.</param>
    public AwsS3ClientFactory(AwsS3StorageProviderOptions options)
        : base((options ?? throw new ArgumentNullException(nameof(options))).ClientCacheSizeLimit)
    {
        _options = options;
    }

    /// <inheritdoc />
    protected override S3EndpointKey GetEndpoint(StorageUri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        foreach (var parameter in CredentialParameters)
        {
            if (uri.Parameters.ContainsKey(parameter))
            {
                throw new ArgumentException(
                    $"The '{parameter}' URI parameter is no longer supported, because URIs are logged and compared. Set DefaultCredentials in the provider options instead.",
                    nameof(uri));
            }
        }

        return new S3EndpointKey(GetRegion(uri), GetServiceUrl(uri)?.AbsoluteUri, GetForcePathStyle(uri));
    }

    /// <inheritdoc />
    protected override IAmazonS3 CreateClient(S3EndpointKey endpoint)
    {
        var credentials = _options.DefaultCredentials;

        if (credentials is null && !_options.UseDefaultCredentialChain)
        {
            throw new InvalidOperationException(
                "No AWS credentials available. Set DefaultCredentials in the options, or enable the default credential chain.");
        }

        var config = new AmazonS3Config { ForcePathStyle = endpoint.ForcePathStyle };

        // Setting ServiceURL (even to null) clears RegionEndpoint, so set only one of them. A custom endpoint signs
        // with the requested region. With neither, the SDK resolves the region itself (environment, profile, IMDS).
        if (endpoint.ServiceUrl is not null)
        {
            config.ServiceURL = endpoint.ServiceUrl;

            if (endpoint.Region is not null)
                config.AuthenticationRegion = endpoint.Region;
        }
        else if (endpoint.Region is not null)
            config.RegionEndpoint = RegionEndpoint.GetBySystemName(endpoint.Region);

        return credentials is null
            ? new AmazonS3Client(config)
            : new AmazonS3Client(credentials, config);
    }

    private string? GetRegion(StorageUri uri)
    {
        if (uri.Parameters.TryGetValue("region", out var regionString) && !string.IsNullOrEmpty(regionString))
        {
            var normalized = regionString.Trim();

            return KnownRegions.Value.TryGetValue(normalized, out var known)
                ? known
                : throw new ArgumentException($"Invalid AWS region: {regionString}", nameof(uri));
        }

        return _options.DefaultRegion?.SystemName;
    }

    private Uri? GetServiceUrl(StorageUri uri)
    {
        if (uri.Parameters.TryGetValue("serviceUrl", out var serviceUrlString) && !string.IsNullOrEmpty(serviceUrlString))
        {
            return Uri.TryCreate(serviceUrlString, UriKind.Absolute, out var serviceUrl)
                ? serviceUrl
                : throw new ArgumentException($"Invalid service URL: {serviceUrlString}", nameof(uri));
        }

        return _options.ServiceUrl;
    }

    private bool GetForcePathStyle(StorageUri uri)
    {
        if (uri.Parameters.TryGetValue("pathStyle", out var pathStyleString) && !string.IsNullOrEmpty(pathStyleString))
        {
            return bool.TryParse(pathStyleString, out var pathStyle)
                ? pathStyle
                : throw new ArgumentException($"Invalid pathStyle value: {pathStyleString}. Must be 'true' or 'false'.", nameof(uri));
        }

        return _options.ForcePathStyle;
    }
}
