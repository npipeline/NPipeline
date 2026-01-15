using System.Runtime.CompilerServices;
using System.Threading.Channels;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_HttpPost.Models;

namespace Sample_HttpPost.Nodes;

/// <summary>
///     Channel-based source node that receives webhook data from HTTP POST requests.
///     This node demonstrates the push-to-pull bridge pattern where:
///     - HTTP POST requests push data into a Channel&lt;WebhookData&gt;
///     - The pipeline pulls data from the channel as IDataPipe&lt;WebhookData&gt;
/// </summary>
/// <remarks>
///     This source must be registered as a singleton in the DI container so that
///     both the HTTP controller (push) and the pipeline (pull) can access the same instance.
/// </remarks>
public class WebhookSource : ISourceNode<WebhookData>
{
    private readonly Channel<WebhookData> _channel;
    private readonly ILogger<WebhookSource> _logger;
    private bool _disposed;
    private int _enqueuedCount;

    /// <summary>
    ///     Initializes a new instance of the <see cref="WebhookSource" /> class.
    /// </summary>
    /// <param name="logger">The logger instance for logging operations.</param>
    public WebhookSource(ILogger<WebhookSource> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;

        // Create an unbounded channel to allow unlimited items to be enqueued
        // In production, consider using a bounded channel with capacity limits
        _channel = Channel.CreateUnbounded<WebhookData>(new UnboundedChannelOptions
        {
            SingleReader = true, // Only the pipeline will read from this channel
            SingleWriter = false, // Multiple HTTP requests can write concurrently
        });
    }

    /// <summary>
    ///     Gets the number of items currently enqueued in the channel.
    /// </summary>
    public int QueuedCount => _channel.Reader.Count;

    /// <summary>
    ///     Gets the total number of items that have been enqueued since creation.
    /// </summary>
    public int TotalEnqueued => _enqueuedCount;

    /// <summary>
    ///     Initializes the source node and returns a data pipe that reads from the channel.
    ///     This method is called by the pipeline when starting execution.
    /// </summary>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A data pipe containing webhook data from the channel.</returns>
    public IDataPipe<WebhookData> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        _logger.LogInformation("WebhookSource initialized and ready to process webhooks");

        async IAsyncEnumerable<WebhookData> ReadFromChannel([EnumeratorCancellation] CancellationToken ct = default)
        {
            _logger.LogInformation("WebhookSource started reading from channel");

            var processedCount = 0;

            await foreach (var item in _channel.Reader.ReadAllAsync(ct))
            {
                ct.ThrowIfCancellationRequested();
                processedCount++;

                try
                {
                    _logger.LogDebug(
                        "Processing webhook {Id} of type {EventType} (Processed: {Count})",
                        item.Id,
                        item.EventType,
                        processedCount
                    );
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("WebhookSource processing canceled");
                    throw;
                }

                yield return item;
            }

            _logger.LogInformation(
                "WebhookSource stopped reading from channel. Total processed: {Count}",
                processedCount
            );
        }

        return new StreamingDataPipe<WebhookData>(ReadFromChannel(cancellationToken), "WebhookSource");
    }

    /// <summary>
    ///     Asynchronously disposes of the webhook source and releases all resources.
    ///     Completes the channel writer and waits for all pending items to be consumed.
    /// </summary>
    /// <returns>A ValueTask that represents the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        // Signal completion to all readers
        _ = _channel.Writer.TryComplete();

        // Wait for all items to be consumed
        try
        {
            await _channel.Reader.Completion;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error during WebhookSource disposal while waiting for channel completion");
        }

        _disposed = true;
        GC.SuppressFinalize(this);
    }

    /// <summary>
    ///     Enqueues a webhook data item into the channel for processing by the pipeline.
    ///     This method is called by the HTTP controller when receiving POST requests.
    /// </summary>
    /// <param name="webhookData">The webhook data to enqueue.</param>
    /// <param name="cancellationToken">Cancellation token to stop the operation.</param>
    /// <returns>A task representing the asynchronous enqueue operation.</returns>
    /// <exception cref="OperationCanceledException">Thrown when the operation is canceled.</exception>
    public async Task EnqueueAsync(WebhookData webhookData, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(webhookData);

        await _channel.Writer.WriteAsync(webhookData, cancellationToken);
        Interlocked.Increment(ref _enqueuedCount);

        _logger.LogInformation(
            "Enqueued webhook {Id} of type {EventType}. Total enqueued: {Count}",
            webhookData.Id,
            webhookData.EventType,
            _enqueuedCount
        );
    }
}
