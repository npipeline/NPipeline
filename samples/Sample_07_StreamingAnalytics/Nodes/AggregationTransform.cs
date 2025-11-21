using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_07_StreamingAnalytics.Nodes;

/// <summary>
///     Transform node that performs additional aggregations and calculations on windowed results.
///     This node demonstrates how to enrich windowed data with additional statistics and metrics.
/// </summary>
public class AggregationTransform : TransformNode<WindowedResult, WindowedResult>
{
    private readonly List<WindowedResult> _historicalResults;
    private readonly int _historySize;

    /// <summary>
    ///     Initializes a new instance of the AggregationTransform class.
    /// </summary>
    /// <param name="historySize">The number of previous results to keep for trend analysis.</param>
    public AggregationTransform(int historySize = 10)
    {
        _historySize = historySize;
        _historicalResults = new List<WindowedResult>();
    }

    /// <summary>
    ///     Processes windowed results and enriches them with additional statistics.
    /// </summary>
    /// <param name="item">The windowed result to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A task containing the enriched windowed result.</returns>
    public override async Task<WindowedResult> ExecuteAsync(WindowedResult item, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Aggregating windowed result: {item.WindowType} window {item.WindowStart:O} - {item.WindowEnd:O}");

        // Add the current result to history
        _historicalResults.Add(item);

        // Maintain history size
        if (_historicalResults.Count > _historySize)
            _historicalResults.RemoveAt(0);

        // Calculate additional statistics based on historical data
        var enrichedResult = await CalculateAdditionalStatistics(item, cancellationToken);

        return await Task.FromResult(enrichedResult);
    }

    /// <summary>
    ///     Calculates additional statistics based on historical windowed results.
    /// </summary>
    private async Task<WindowedResult> CalculateAdditionalStatistics(WindowedResult currentResult, CancellationToken cancellationToken)
    {
        // For demonstration, we'll add some trend analysis and anomaly detection
        // In a real implementation, this could include more sophisticated statistical analysis

        if (_historicalResults.Count < 2)
        {
            // Not enough history for trend analysis
            return await Task.FromResult(currentResult);
        }

        // Calculate trend (simple linear regression on average values)
        var recentResults = _historicalResults.TakeLast(Math.Min(5, _historicalResults.Count)).ToList();

        if (recentResults.Count >= 2)
        {
            var trend = CalculateTrend(recentResults);
            var isAnomaly = DetectAnomaly(currentResult, recentResults);

            // Log the analysis results
            Console.WriteLine($"  Trend: {trend:F2} (units per window)");
            Console.WriteLine($"  Anomaly detected: {isAnomaly}");

            // Calculate performance metrics
            var avgProcessingTime = CalculateAverageProcessingTime(recentResults);
            Console.WriteLine($"  Average processing time: {avgProcessingTime:F2}ms");
        }

        // Calculate source diversity metrics
        var sourceDiversity = CalculateSourceDiversity(_historicalResults);
        Console.WriteLine($"  Source diversity: {sourceDiversity:F2} (unique sources per window)");

        return await Task.FromResult(currentResult);
    }

    /// <summary>
    ///     Calculates a simple trend based on average values.
    /// </summary>
    private double CalculateTrend(List<WindowedResult> results)
    {
        if (results.Count < 2)
            return 0.0;

        // Simple linear regression: y = mx + b
        var n = results.Count;
        var sumX = 0.0;
        var sumY = 0.0;
        var sumXY = 0.0;
        var sumX2 = 0.0;

        for (var i = 0; i < n; i++)
        {
            var x = i; // Time index
            var y = results[i].Average;
            sumX += x;
            sumY += y;
            sumXY += x * y;
            sumX2 += x * x;
        }

        // Calculate slope (m)
        var slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
        return slope;
    }

    /// <summary>
    ///     Detects anomalies based on statistical deviation from recent results.
    /// </summary>
    private bool DetectAnomaly(WindowedResult current, List<WindowedResult> recentResults)
    {
        if (recentResults.Count < 3)
            return false;

        var recentAverages = recentResults.Take(recentResults.Count - 1).Select(r => r.Average).ToList();
        var mean = recentAverages.Average();
        var stdDev = Math.Sqrt(recentAverages.Average(x => Math.Pow(x - mean, 2)));

        // Anomaly if current average is more than 2 standard deviations from the mean
        var threshold = 2.0 * stdDev;
        var deviation = Math.Abs(current.Average - mean);

        return deviation > threshold;
    }

    /// <summary>
    ///     Calculates the average processing time for windows.
    /// </summary>
    private double CalculateAverageProcessingTime(List<WindowedResult> results)
    {
        return results.Average(r => r.WindowDurationMs);
    }

    /// <summary>
    ///     Calculates source diversity (average number of unique sources per window).
    /// </summary>
    private double CalculateSourceDiversity(List<WindowedResult> results)
    {
        return results.Average(r => r.Sources.Count);
    }

    /// <summary>
    ///     Gets performance metrics for the aggregation process.
    /// </summary>
    public AggregationMetrics GetMetrics()
    {
        return new AggregationMetrics
        {
            TotalWindowsProcessed = _historicalResults.Count,
            AverageWindowDuration = _historicalResults.Count > 0
                ? _historicalResults.Average(r => r.WindowDurationMs)
                : 0,
            TotalDataPointsProcessed = _historicalResults.Sum(r => r.Count),
            TotalLateDataPoints = _historicalResults.Sum(r => r.LateCount),
            UniqueSources = _historicalResults.SelectMany(r => r.Sources).Distinct().Count(),
        };
    }
}

/// <summary>
///     Represents metrics for the aggregation transform.
/// </summary>
public record AggregationMetrics
{
    /// <summary>
    ///     Gets the total number of windows processed.
    /// </summary>
    public int TotalWindowsProcessed { get; init; }

    /// <summary>
    ///     Gets the average window duration in milliseconds.
    /// </summary>
    public double AverageWindowDuration { get; init; }

    /// <summary>
    ///     Gets the total number of data points processed.
    /// </summary>
    public int TotalDataPointsProcessed { get; init; }

    /// <summary>
    ///     Gets the total number of late data points.
    /// </summary>
    public int TotalLateDataPoints { get; init; }

    /// <summary>
    ///     Gets the number of unique sources.
    /// </summary>
    public int UniqueSources { get; init; }

    /// <summary>
    ///     Gets the late data percentage.
    /// </summary>
    public double LateDataPercentage => TotalDataPointsProcessed > 0
        ? (double)TotalLateDataPoints / TotalDataPointsProcessed * 100
        : 0;

    public override string ToString()
    {
        return $"AggregationMetrics(Windows={TotalWindowsProcessed}, DataPoints={TotalDataPointsProcessed}, " +
               $"Late={LateDataPercentage:F1}%, Sources={UniqueSources})";
    }
}
