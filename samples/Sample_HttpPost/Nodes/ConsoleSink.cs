using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_HttpPost.Models;

namespace Sample_HttpPost.Nodes;

/// <summary>
///     Sink node that outputs processed webhook data to the console.
///     This node demonstrates how to consume processed data and output it
///     with formatted logging.
/// </summary>
public class ConsoleSink : SinkNode<ProcessedData>
{
    private readonly ILogger<ConsoleSink> _logger;
    private int _processedCount;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ConsoleSink" /> class.
    /// </summary>
    /// <param name="logger">The logger instance for logging operations.</param>
    public ConsoleSink(ILogger<ConsoleSink> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        _logger = logger;
    }

    /// <summary>
    ///     Processes the input processed webhook data by writing them to the console with formatting.
    /// </summary>
    /// <param name="input">The data pipe containing input processed webhook data to output.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A task representing the sink execution.</returns>
    public override async Task ExecuteAsync(
        IDataPipe<ProcessedData> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        _logger.LogInformation("ConsoleSink started processing webhook data");

        _processedCount = 0;

        // Print header
        Console.WriteLine();
        Console.WriteLine("=== PROCESSED WEBHOOK DATA ===");
        Console.WriteLine();
        Console.WriteLine("Timestamp           | ID           | Event Type              | Summary");
        Console.WriteLine("---------------------|--------------|-------------------------|--------------------------------------------------");

        // Use await foreach to consume all messages from the input pipe
        await foreach (var data in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            _processedCount++;

            // Format and display the processed data
            var timestamp = data.ProcessedAt.ToString("yyyy-MM-dd HH:mm:ss.fff");
            var id = data.Id.PadRight(12);
            var eventType = data.EventType.PadRight(23);

            Console.WriteLine($"{timestamp} | {id} | {eventType} | {data.Summary}");

            _logger.LogDebug(
                "Output webhook {Id} of type {EventType}",
                data.Id,
                data.EventType
            );
        }

        Console.WriteLine();
        Console.WriteLine("=== SUMMARY ===");
        Console.WriteLine($"Total webhooks processed: {_processedCount}");
        Console.WriteLine();

        _logger.LogInformation(
            "ConsoleSink completed processing. Total processed: {Count}",
            _processedCount
        );
    }
}
