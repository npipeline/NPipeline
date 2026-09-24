using Amazon;
using Amazon.Runtime.CredentialManagement;
using Amazon.SQS;
using NPipeline.Connectors.Aws.Sqs.Configuration;

namespace NPipeline.Connectors.Aws.Sqs.Internal;

/// <summary>
///     Builds the SQS client the nodes use when the caller does not supply one.
/// </summary>
internal static class SqsClientFactory
{
    public static IAmazonSQS Create(SqsConfiguration configuration)
    {
        var config = CreateClientConfig(configuration);

        if (!string.IsNullOrWhiteSpace(configuration.AccessKeyId) &&
            !string.IsNullOrWhiteSpace(configuration.SecretAccessKey))
        {
            return new AmazonSQSClient(
                configuration.AccessKeyId,
                configuration.SecretAccessKey,
                config);
        }

        if (!string.IsNullOrWhiteSpace(configuration.ProfileName))
        {
            var chain = new CredentialProfileStoreChain();

            if (chain.TryGetProfile(configuration.ProfileName, out var profile))
                return new AmazonSQSClient(profile.GetAWSCredentials(chain), config);
        }

        // Use default credential chain
        return new AmazonSQSClient(config);
    }

    /// <summary>
    ///     The client configuration, including the SDK's retry settings. The SDK is the only layer that retries SQS
    ///     calls: its retry knows which errors are throttling, backs off with jitter, and spends from a retry quota.
    /// </summary>
    public static AmazonSQSConfig CreateClientConfig(SqsConfiguration configuration)
    {
        var config = new AmazonSQSConfig
        {
            RegionEndpoint = RegionEndpoint.GetBySystemName(configuration.Region),
        };

        // Left unset, the SDK resolves these itself (AWS_RETRY_MODE, AWS_MAX_ATTEMPTS, or the shared config file).
        if (configuration.RetryMode is { } retryMode)
            config.RetryMode = retryMode;

        if (configuration.MaxErrorRetry is { } maxErrorRetry)
            config.MaxErrorRetry = maxErrorRetry;

        return config;
    }
}
