using Amazon;
using Amazon.Runtime.CredentialManagement;
using Amazon.SQS;
using NPipeline.Connectors.Aws.Sqs.Configuration;

namespace NPipeline.Connectors.Aws.Sqs.Internal;

/// <summary>The client a node uses: the one in its options, or one it creates and owns.</summary>
internal static class SqsClientFactory
{
    public static (IAmazonSQS Client, bool Owned) For(SqsNodeOptions options) =>
        options.Client is { } client ? (client, false) : (Create(options), true);

    private static AmazonSQSClient Create(SqsNodeOptions options)
    {
        var config = new AmazonSQSConfig();

        // Setting RegionEndpoint clears ServiceURL, so with a service URL (LocalStack, a VPC endpoint) the region only signs requests.
        if (options.ServiceUrl is { } serviceUrl)
        {
            config.ServiceURL = serviceUrl;

            if (options.Region is { } signingRegion)
                config.AuthenticationRegion = signingRegion;
        }
        else if (options.Region is { } region)
        {
            config.RegionEndpoint = RegionEndpoint.GetBySystemName(region);
        }

        // Left unset, the SDK resolves these itself (AWS_RETRY_MODE, AWS_MAX_ATTEMPTS, or the shared config file).
        if (options.RetryMode is { } retryMode)
            config.RetryMode = retryMode;

        if (options.MaxErrorRetry is { } maxErrorRetry)
            config.MaxErrorRetry = maxErrorRetry;

        if (options.Credentials is { } credentials)
            return new AmazonSQSClient(credentials, config);

        if (options.ProfileName is { } profileName && new CredentialProfileStoreChain().TryGetAWSCredentials(profileName, out var profile))
            return new AmazonSQSClient(profile, config);

        return new AmazonSQSClient(config);
    }
}
