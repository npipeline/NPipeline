using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_05_ParallelProcessing.Nodes;

/// <summary>
///     Sink node that outputs processed work items and displays performance statistics.
///     This node demonstrates thread-safe result aggregation and performance reporting.
/// </summary>
public class ConsoleSinkWithMetrics : SinkNode<ProcessedWorkItem>
{
    private static readonly object ResultsLock = new();
    private static readonly List<ProcessedWorkItem> Results = new();

    /// <summary>
    ///     Processes the input work items by writing them to the console with formatting
    ///     and collects results for final statistics.
    /// </summary>
    /// <param name="input">The data pipe containing input work items to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task representing the sink execution.</returns>
    public override async Task ExecuteAsync(IDataPipe<ProcessedWorkItem> input, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine("Starting to process work items in ConsoleSinkWithMetrics");
        Console.WriteLine();

        var messageCount = 0;
        var totalProcessingTime = 0L;
        var threadDistribution = new Dictionary<int, int>();

        // Use await foreach to consume all messages from the input pipe
        await foreach (var workItem in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            messageCount++;
            totalProcessingTime += workItem.ProcessingTimeMs;

            // Track thread distribution
            if (!threadDistribution.TryGetValue(workItem.ThreadId, out var value))
            {
                value = 0;
                threadDistribution[workItem.ThreadId] = value;
            }

            threadDistribution[workItem.ThreadId] = ++value;

            // Thread-safe result collection
            lock (ResultsLock)
            {
                Results.Add(workItem);
            }

            // Output individual work item results
            Console.WriteLine($"[Result] {workItem.Id}: {workItem.Result}");
            Console.WriteLine($"[Result] Processed in {workItem.ProcessingTimeMs}ms on thread {workItem.ThreadId} at {workItem.ProcessedAt:HH:mm:ss.fff}");
            Console.WriteLine();
        }

        Console.WriteLine($"ConsoleSinkWithMetrics processed {messageCount} work items");
        Console.WriteLine($"Total processing time: {totalProcessingTime}ms");

        var avgProcessingTime = messageCount > 0
            ? totalProcessingTime / (double)messageCount
            : 0;

        Console.WriteLine($"Average processing time: {avgProcessingTime:F2}ms");
        Console.WriteLine();

        // Display thread utilization
        Console.WriteLine("Thread Utilization:");

        foreach (var kvp in threadDistribution.OrderBy(kvp => kvp.Key))
        {
            var percentage = messageCount > 0
                ? kvp.Value * 100.0 / messageCount
                : 0;

            Console.WriteLine($"  Thread {kvp.Key}: {kvp.Value} items ({percentage:F1}%)");
        }

        Console.WriteLine();

        // Display performance metrics summary
        Console.WriteLine(PerformanceMonitoringTransform.GetPerformanceSummary());

        // Display processing summary by complexity
        await DisplayProcessingSummaryByComplexity();
    }

    /// <summary>
    ///     Displays a summary of processing results grouped by work item complexity.
    /// </summary>
    private static async Task DisplayProcessingSummaryByComplexity()
    {
        await Task.Run(() =>
        {
            Console.WriteLine("=== Processing Summary by Complexity ===");

            lock (ResultsLock)
            {
                if (Results.Count == 0)
                {
                    Console.WriteLine("No results to analyze.");
                    return;
                }

                var complexityGroups = Results
                    .GroupBy(r => GetComplexityFromResult(r.Result))
                    .OrderBy(g => g.Key)
                    .ToList();

                foreach (var group in complexityGroups)
                {
                    var count = group.Count();
                    var avgTime = group.Average(r => r.ProcessingTimeMs);
                    var minTime = group.Min(r => r.ProcessingTimeMs);
                    var maxTime = group.Max(r => r.ProcessingTimeMs);

                    Console.WriteLine($"{group.Key} Complexity ({count} items):");
                    Console.WriteLine($"  Average time: {avgTime:F2}ms");
                    Console.WriteLine($"  Min time: {minTime}ms");
                    Console.WriteLine($"  Max time: {maxTime}ms");
                    Console.WriteLine();
                }
            }
        });
    }

    /// <summary>
    ///     Extracts complexity level from the result string.
    /// </summary>
    private static string GetComplexityFromResult(string result)
    {
        if (result.Contains("Simple computation"))
            return "Simple";

        if (result.Contains("Medium computation"))
            return "Medium";

        if (result.Contains("Complex computation"))
            return "Complex";

        return "Unknown";
    }

    /// <summary>
    ///     Gets all collected results.
    /// </summary>
    /// <returns>A list of all processed work items.</returns>
    public static List<ProcessedWorkItem> GetResults()
    {
        lock (ResultsLock)
        {
            return new List<ProcessedWorkItem>(Results);
        }
    }

    /// <summary>
    ///     Clears all collected results.
    /// </summary>
    public static void ClearResults()
    {
        lock (ResultsLock)
        {
            Results.Clear();
        }
    }
}
