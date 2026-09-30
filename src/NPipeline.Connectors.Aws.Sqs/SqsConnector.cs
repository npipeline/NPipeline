using NPipeline.Connectors.Aws.Sqs.Configuration;
using NPipeline.Connectors.Aws.Sqs.Nodes;

namespace NPipeline.Connectors.Aws.Sqs;

/// <summary>Creates SQS sources and sinks.</summary>
/// <example>
///     <code>
/// var orders = SqsConnector.Source&lt;Order&gt;("https://sqs.ap-southeast-2.amazonaws.com/123456789012/orders");
/// var invoices = SqsConnector.Sink&lt;Invoice&gt;(invoicesUrl, o => o with { Client = sharedClient });
///     </code>
/// </example>
public static class SqsConnector
{
    internal const string Name = "sqs";

    /// <summary>A source that receives from the queue at <paramref name="queueUrl" />.</summary>
    public static SqsSourceNode<T> Source<T>(string queueUrl, Func<SqsReadOptions, SqsReadOptions>? configure = null)
    {
        var options = new SqsReadOptions { QueueUrl = queueUrl };
        return new SqsSourceNode<T>(configure?.Invoke(options) ?? options);
    }

    /// <summary>A sink that sends to the queue at <paramref name="queueUrl" />.</summary>
    public static SqsSinkNode<T> Sink<T>(string queueUrl, Func<SqsWriteOptions<T>, SqsWriteOptions<T>>? configure = null)
    {
        var options = new SqsWriteOptions<T> { QueueUrl = queueUrl };
        return new SqsSinkNode<T>(configure?.Invoke(options) ?? options);
    }
}
