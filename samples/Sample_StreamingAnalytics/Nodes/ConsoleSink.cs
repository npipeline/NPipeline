using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_StreamingAnalytics.Nodes;

/// <summary>
///     Sink node that outputs windowed results to the console with detailed formatting.
///     This node demonstrates how to consume and display streaming analytics results.
/// </summary>
public class ConsoleSink : SinkNode<WindowedResult>
{
    private readonly DateTime _startTime;
    private int _slidingWindowCount;
    private int _totalResultsProcessed;
    private int _tumblingWindowCount;

    /// <summary>
    ///     Initializes a new instance of the ConsoleSink class.
    /// </summary>
    public ConsoleSink()
    {
        _totalResultsProcessed = 0;
        _tumblingWindowCount = 0;
        _slidingWindowCount = 0;
        _startTime = DateTime.UtcNow;
    }

    /// <summary>
    ///     Processes windowed results by writing them to the console with detailed formatting.
    /// </summary>
    /// <param name="input">The data pipe containing windowed results to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task representing the sink execution.</returns>
    public override async Task ExecuteAsync(IDataPipe<WindowedResult> input, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("=== Starting ConsoleSink - Processing Windowed Results ===");
        Console.WriteLine();

        // Use await foreach to consume all windowed results from the input pipe
        await foreach (var result in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            _totalResultsProcessed++;

            // Track window types
            if (result.WindowType == "Tumbling")
                _tumblingWindowCount++;
            else if (result.WindowType == "Sliding")
                _slidingWindowCount++;

            // Display the windowed result with detailed formatting
            DisplayWindowedResult(result);

            // Add separator between results for better readability
            Console.WriteLine(new string('-', 80));
            Console.WriteLine();
        }

        // Display summary statistics
        DisplaySummaryStatistics();
    }

    /// <summary>
    ///     Displays a windowed result with detailed formatting.
    /// </summary>
    private void DisplayWindowedResult(WindowedResult result)
    {
        var processingTime = DateTime.UtcNow - result.WindowStart;

        Console.WriteLine($"📊 WINDOWED RESULT #{_totalResultsProcessed}");
        Console.WriteLine($"   Type:        {result.WindowType}");
        Console.WriteLine($"   Window:      {result.WindowStart:HH:mm:ss.fff} - {result.WindowEnd:HH:mm:ss.fff}");
        Console.WriteLine($"   Duration:    {result.WindowDurationMs}ms");
        Console.WriteLine($"   Data Points: {result.Count}");
        Console.WriteLine();

        Console.WriteLine("📈 STATISTICS:");
        Console.WriteLine($"   Count:       {result.Count:N0}");
        Console.WriteLine($"   Sum:         {result.Sum:N2}");
        Console.WriteLine($"   Average:     {result.Average:N2}");
        Console.WriteLine($"   Min:         {result.Min:N2}");
        Console.WriteLine($"   Max:         {result.Max:N2}");
        Console.WriteLine($"   Range:       {result.Max - result.Min:N2}");
        Console.WriteLine();

        if (result.LateCount > 0)
        {
            var latePercentage = (double)result.LateCount / result.Count * 100;
            Console.WriteLine($"⚠️  LATE DATA:  {result.LateCount} points ({latePercentage:F1}%)");
            Console.WriteLine();
        }

        if (result.Sources.Count > 0)
        {
            Console.WriteLine($"📡 SOURCES ({result.Sources.Count}): {string.Join(", ", result.Sources.OrderBy(s => s))}");
            Console.WriteLine();
        }

        Console.WriteLine($"⏱️  Processing Time: {processingTime.TotalMilliseconds:F2}ms after window end");
    }

    /// <summary>
    ///     Displays summary statistics for the entire processing session.
    /// </summary>
    private void DisplaySummaryStatistics()
    {
        var totalProcessingTime = DateTime.UtcNow - _startTime;

        Console.WriteLine();
        Console.WriteLine("=== CONSOLE SINK SUMMARY ===");
        Console.WriteLine($"Total Processing Time: {totalProcessingTime.TotalSeconds:F2} seconds");
        Console.WriteLine($"Total Results Processed: {_totalResultsProcessed:N0}");
        Console.WriteLine($"  Tumbling Windows: {_tumblingWindowCount:N0}");
        Console.WriteLine($"  Sliding Windows: {_slidingWindowCount:N0}");
        Console.WriteLine($"Average Results per Second: {_totalResultsProcessed / Math.Max(totalProcessingTime.TotalSeconds, 1):F2}");
        Console.WriteLine();

        if (_totalResultsProcessed > 0)
            Console.WriteLine("🎯 Stream Analytics Pipeline Completed Successfully!");
        else
            Console.WriteLine("⚠️  No windowed results were processed");

        Console.WriteLine("===============================");
    }
}
