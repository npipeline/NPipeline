using Amazon;
using Amazon.SQS;

namespace Sample_SqsConnector;

/// <summary>An order received from the input queue as JSON.</summary>
public sealed record Order(string OrderId, string CustomerId, decimal TotalAmount, string Status, DateTime CreatedAt);

/// <summary>The result of processing an order, sent to the output queue.</summary>
public sealed record ProcessedOrder(string OrderId, string CustomerId, decimal TotalAmount, string Status, DateTime ProcessedAt, string? ProcessingNotes);

/// <summary>Where the sample's queues are, read from environment variables.</summary>
/// <param name="InputQueueUrl">The queue orders are received from (<c>SQS_INPUT_QUEUE_URL</c>).</param>
/// <param name="OutputQueueUrl">The queue results are sent to (<c>SQS_OUTPUT_QUEUE_URL</c>).</param>
/// <param name="Region">The AWS region (<c>AWS_REGION</c>, default <c>us-east-1</c>).</param>
/// <param name="ServiceUrl">An endpoint such as LocalStack's (<c>SQS_SERVICE_URL</c>); <c>null</c> uses AWS.</param>
public sealed record SqsSampleSettings(string InputQueueUrl, string OutputQueueUrl, string Region, string? ServiceUrl)
{
    /// <summary>Reads the settings from the environment, with placeholder URLs for anything not set.</summary>
    public static SqsSampleSettings FromEnvironment() =>
        new(
            Environment.GetEnvironmentVariable("SQS_INPUT_QUEUE_URL") ?? "https://sqs.us-east-1.amazonaws.com/123456789012/input-orders-queue",
            Environment.GetEnvironmentVariable("SQS_OUTPUT_QUEUE_URL") ?? "https://sqs.us-east-1.amazonaws.com/123456789012/processed-orders-queue",
            Environment.GetEnvironmentVariable("AWS_REGION") ?? "us-east-1",
            Environment.GetEnvironmentVariable("SQS_SERVICE_URL"));

    /// <summary>A client for the region, or for <see cref="ServiceUrl" /> when it is set.</summary>
    public AmazonSQSClient CreateClient() =>
        new(ServiceUrl is null
            ? new AmazonSQSConfig { RegionEndpoint = RegionEndpoint.GetBySystemName(Region) }
            : new AmazonSQSConfig { ServiceURL = ServiceUrl, AuthenticationRegion = Region });
}
