using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_HttpPost.Models;

namespace Sample_HttpPost.Nodes;

/// <summary>
///     Transform node that processes validated webhook data and generates a summary.
///     This node demonstrates how to transform validated data into a processed format
///     by extracting meaningful information from the payload.
/// </summary>
public class DataProcessingTransform : TransformNode<ValidatedWebhookData, ProcessedData>
{
    private readonly ILogger<DataProcessingTransform> _logger;

    /// <summary>
    ///     Initializes a new instance of the <see cref="DataProcessingTransform" /> class.
    /// </summary>
    /// <param name="logger">The logger instance for logging operations.</param>
    public DataProcessingTransform(ILogger<DataProcessingTransform> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;
    }

    /// <summary>
    ///     Processes the validated webhook data and generates a summary.
    /// </summary>
    /// <param name="item">The validated webhook data to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A task containing the processed data with summary.</returns>
    public override async Task<ProcessedData> ExecuteAsync(
        ValidatedWebhookData item,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Processing validated webhook {Id} of type {EventType}",
            item.Id,
            item.EventType
        );

        // Generate a summary from the payload
        var summary = GenerateSummary(item);

        var processedData = new ProcessedData(
            item.Id,
            item.EventType,
            summary,
            DateTime.UtcNow
        );

        _logger.LogInformation(
            "Processed webhook {Id}: {Summary}",
            processedData.Id,
            processedData.Summary
        );

        return await Task.FromResult(processedData);
    }

    /// <summary>
    ///     Generates a human-readable summary from the validated webhook data.
    /// </summary>
    /// <param name="item">The validated webhook data.</param>
    /// <returns>A summary string describing the webhook data.</returns>
    private string GenerateSummary(ValidatedWebhookData item)
    {
        var payloadSummary = string.Join(", ", item.Payload.Keys.Take(5));

        if (item.Payload.Count > 5)
            payloadSummary += $"... (+{item.Payload.Count - 5} more)";

        var processingDelay = DateTime.UtcNow - item.Timestamp;

        var delayText = processingDelay.TotalMilliseconds < 100
            ? $"{processingDelay.TotalMilliseconds:F0}ms"
            : $"{processingDelay.TotalSeconds:F2}s";

        return $"Event '{item.EventType}' received {delayText} ago with payload keys: [{payloadSummary}]";
    }
}
