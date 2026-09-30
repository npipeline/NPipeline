using System.Globalization;
using System.Threading.Channels;
using Amazon.SQS;
using Amazon.SQS.Model;
using Microsoft.Extensions.Logging;
using NPipeline.Connectors.Diagnostics;
using NPipeline.DataFlow;

namespace NPipeline.Connectors.Aws.Sqs.Internal;

/// <summary>
///     Deletes acknowledged messages in batches of up to ten, so acknowledging costs a tenth of a request. A delete that
///     fails leaves the message to be delivered again, which at-least-once delivery allows; it is logged and counted.
/// </summary>
internal sealed class SqsDeleter
{
    private readonly IAmazonSQS _client;
    private readonly ILogger _logger;
    private readonly Task _loop;
    private readonly Channel<string> _pending = Channel.CreateUnbounded<string>(new UnboundedChannelOptions { SingleReader = true });
    private readonly string _queueUrl;

    public SqsDeleter(IAmazonSQS client, string queueUrl, TimeSpan linger, ILogger logger)
    {
        _client = client;
        _queueUrl = queueUrl;
        _logger = logger;
        _loop = Task.Run(() => RunAsync(linger));
    }

    public void Enqueue(string receiptHandle) => _pending.Writer.TryWrite(receiptHandle);

    /// <summary>Sends what is queued and stops.</summary>
    public Task CompleteAsync()
    {
        _pending.Writer.TryComplete();
        return _loop;
    }

    private async Task RunAsync(TimeSpan linger)
    {
        await foreach (var batch in _pending.Reader.ReadAllAsync().BatchAsync(10, linger).ConfigureAwait(false))
        {
            var entries = batch.Select((handle, i) => new DeleteMessageBatchRequestEntry(i.ToString(CultureInfo.InvariantCulture), handle)).ToList();

            try
            {
                var response = await _client.DeleteMessageBatchAsync(_queueUrl, entries).ConfigureAwait(false);

                foreach (var failed in response.Failed ?? [])
                {
                    SqsLogMessages.DeleteFailed(_logger, failed.Code, failed.Message);
                    ConnectorDiagnostics.RecordMessagesSettled(SqsConnector.Name, "delete_failed");
                }
            }
            catch (Exception ex) when (ex is AmazonSQSException or HttpRequestException or TaskCanceledException)
            {
                SqsLogMessages.DeleteBatchFailed(_logger, ex, entries.Count);
                ConnectorDiagnostics.RecordMessagesSettled(SqsConnector.Name, "delete_failed", entries.Count);
            }
        }
    }
}
