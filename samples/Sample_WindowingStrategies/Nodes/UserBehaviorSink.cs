using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_WindowingStrategies.Models;

namespace Sample_WindowingStrategies.Nodes;

/// <summary>
///     Sink node that processes and outputs user behavior analytics results.
///     This node demonstrates comprehensive result processing and reporting.
/// </summary>
public class UserBehaviorSink : SinkNode<object>
{
    private readonly bool _enableDetailedOutput;
    private readonly bool _enablePatternAnalysis;
    private readonly bool _enablePerformanceMetrics;

    /// <summary>
    ///     Initializes a new instance of the <see cref="UserBehaviorSink" /> class.
    /// </summary>
    /// <param name="enableDetailedOutput">Whether to enable detailed output of results.</param>
    /// <param name="enablePatternAnalysis">Whether to enable pattern analysis output.</param>
    /// <param name="enablePerformanceMetrics">Whether to enable performance metrics output.</param>
    public UserBehaviorSink(
        bool enableDetailedOutput = true,
        bool enablePatternAnalysis = true,
        bool enablePerformanceMetrics = true)
    {
        _enableDetailedOutput = enableDetailedOutput;
        _enablePatternAnalysis = enablePatternAnalysis;
        _enablePerformanceMetrics = enablePerformanceMetrics;
    }

    /// <summary>
    ///     Processes and outputs user behavior analytics results.
    /// </summary>
    /// <param name="input">The input data pipe containing analytics results.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A task representing the completion of the sink processing.</returns>
    public override async Task ExecuteAsync(
        IDataPipe<object> input,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine("=== USER BEHAVIOR ANALYTICS RESULTS ===");
        Console.WriteLine();

        var startTime = DateTime.UtcNow;
        var allItems = await input.ToListAsync(cancellationToken);

        // Separate different types of results
        var sessionMetrics = allItems.OfType<SessionMetrics>().ToList();
        var patternMatches = allItems.OfType<PatternMatch>().ToList();

        Console.WriteLine($"Processing {sessionMetrics.Count} session metrics and {patternMatches.Count} pattern matches");
        Console.WriteLine();

        // Process session metrics
        if (sessionMetrics.Count > 0)
            ProcessSessionMetrics(sessionMetrics);

        // Process pattern matches
        if (patternMatches.Count > 0 && _enablePatternAnalysis)
            ProcessPatternMatches(patternMatches);

        // Generate summary report
        GenerateSummaryReport(sessionMetrics, patternMatches, startTime);

        // Performance analysis
        if (_enablePerformanceMetrics)
            GeneratePerformanceAnalysis(sessionMetrics, patternMatches);

        Console.WriteLine();
        Console.WriteLine("=== ANALYTICS PROCESSING COMPLETED ===");
    }

    private void ProcessSessionMetrics(IReadOnlyList<SessionMetrics> metrics)
    {
        Console.WriteLine("=== SESSION METRICS ANALYSIS ===");
        Console.WriteLine();

        // Overall statistics
        var totalSessions = metrics.Sum(m => m.SessionCount);
        var totalUsers = metrics.Sum(m => m.UniqueUserCount);
        var totalEvents = metrics.Sum(m => m.TotalEvents);
        var totalConversionValue = metrics.Sum(m => m.TotalConversionValue);
        var avgEngagementScore = metrics.Average(m => m.EngagementScore);
        var avgConversionRate = metrics.Average(m => m.ConversionRate);
        var avgBounceRate = metrics.Average(m => m.BounceRate);

        Console.WriteLine("Overall Statistics:");
        Console.WriteLine($"  Total Sessions: {totalSessions:N0}");
        Console.WriteLine($"  Total Users: {totalUsers:N0}");
        Console.WriteLine($"  Total Events: {totalEvents:N0}");
        Console.WriteLine($"  Total Conversion Value: {totalConversionValue:C}");
        Console.WriteLine($"  Average Engagement Score: {avgEngagementScore:F3}");
        Console.WriteLine($"  Average Conversion Rate: {avgConversionRate:P1}");
        Console.WriteLine($"  Average Bounce Rate: {avgBounceRate:P1}");
        Console.WriteLine();

        if (_enableDetailedOutput)
        {
            // Window-by-window analysis
            Console.WriteLine("Window-by-Window Analysis:");

            foreach (var metric in metrics.Take(5)) // Limit output for readability
            {
                Console.WriteLine($"  Window {metric.WindowId}:");
                Console.WriteLine($"    Sessions: {metric.SessionCount}, Users: {metric.UniqueUserCount}");
                Console.WriteLine($"    Engagement: {metric.EngagementScore:F3}, Conversion: {metric.ConversionRate:P1}");
                Console.WriteLine($"    Duration: {metric.WindowDuration.TotalMinutes:F1} minutes");
                Console.WriteLine($"    Conversion Value: {metric.TotalConversionValue:C}");
            }

            if (metrics.Count > 5)
                Console.WriteLine($"  ... and {metrics.Count - 5} more windows");

            Console.WriteLine();

            // Top pages analysis
            var allPageMetrics = metrics.SelectMany(m => m.TopPages).ToList();

            if (allPageMetrics.Count > 0)
            {
                Console.WriteLine("Top Pages Across All Windows:");

                var topPages = allPageMetrics
                    .GroupBy(p => p.PageUrl)
                    .Select(g => new
                    {
                        PageUrl = g.Key,
                        TotalViews = g.Sum(p => p.ViewCount),
                        AvgConversionRate = g.Average(p => p.ConversionRate),
                        AvgTimeOnPage = g.Average(p => p.AverageTimeOnPage.TotalMinutes),
                    })
                    .OrderByDescending(p => p.TotalViews)
                    .Take(10);

                foreach (var page in topPages)
                {
                    Console.WriteLine($"    {page.PageUrl}: {page.TotalViews} views, {page.AvgConversionRate:P1} conversion, {page.AvgTimeOnPage:F1} min");
                }

                Console.WriteLine();
            }

            // Device distribution
            var allDeviceDistribution = metrics
                .SelectMany(m => m.DeviceDistribution)
                .GroupBy(kvp => kvp.Key)
                .ToDictionary(g => g.Key, g => g.Sum(kvp => kvp.Value));

            Console.WriteLine("Overall Device Distribution:");

            foreach (var device in allDeviceDistribution.OrderByDescending(kvp => kvp.Value))
            {
                var percentage = (double)device.Value / allDeviceDistribution.Values.Sum() * 100;
                Console.WriteLine($"    {device.Key}: {device.Value} sessions ({percentage:F1}%)");
            }

            Console.WriteLine();
        }
    }

    private void ProcessPatternMatches(IReadOnlyList<PatternMatch> patterns)
    {
        Console.WriteLine("=== PATTERN MATCH ANALYSIS ===");
        Console.WriteLine();

        // Pattern type distribution
        var patternTypeDistribution = patterns
            .GroupBy(p => p.PatternType)
            .ToDictionary(g => g.Key, g => g.Count());

        Console.WriteLine("Pattern Types Detected:");

        foreach (var type in patternTypeDistribution.OrderByDescending(kvp => kvp.Value))
        {
            Console.WriteLine($"    {type.Key}: {type.Value} patterns");
        }

        Console.WriteLine();

        // Severity distribution
        var severityDistribution = patterns
            .GroupBy(p => p.Severity)
            .ToDictionary(g => g.Key, g => g.Count());

        Console.WriteLine("Pattern Severity Distribution:");

        foreach (var severity in severityDistribution.OrderByDescending(kvp => kvp.Value))
        {
            var percentage = (double)severity.Value / patterns.Count * 100;
            Console.WriteLine($"    {severity.Key}: {severity.Value} patterns ({percentage:F1}%)");
        }

        Console.WriteLine();

        if (_enableDetailedOutput)
        {
            // Top patterns by impact
            Console.WriteLine("Top Patterns by Impact Score:");

            var topPatterns = patterns
                .OrderByDescending(p => p.ImpactScore)
                .Take(10);

            foreach (var pattern in topPatterns)
            {
                Console.WriteLine($"    {pattern.PatternName}:");
                Console.WriteLine($"      Impact: {pattern.ImpactScore:F3}, Confidence: {pattern.ConfidenceScore:F3}");
                Console.WriteLine($"      Severity: {pattern.Severity}, Affected Users: {pattern.AffectedUsers.Count}");
                Console.WriteLine($"      Description: {pattern.Description}");
                Console.WriteLine();
            }

            // Recommended actions
            Console.WriteLine("Recommended Actions:");

            var allActions = patterns
                .SelectMany(p => p.RecommendedActions)
                .GroupBy(action => action)
                .ToDictionary(g => g.Key, g => g.Count());

            foreach (var action in allActions.OrderByDescending(kvp => kvp.Value).Take(10))
            {
                Console.WriteLine($"    {action.Key} (mentioned in {action.Value} patterns)");
            }

            Console.WriteLine();
        }
    }

    private void GenerateSummaryReport(IReadOnlyList<SessionMetrics> metrics, IReadOnlyList<PatternMatch> patterns, DateTime startTime)
    {
        Console.WriteLine("=== EXECUTIVE SUMMARY ===");
        Console.WriteLine();

        var processingTime = DateTime.UtcNow - startTime;
        var totalWindows = metrics.Count;
        var totalSessions = metrics.Sum(m => m.SessionCount);
        var totalUsers = metrics.Sum(m => m.UniqueUserCount);
        var totalConversions = metrics.Sum(m => (int)(m.ConversionRate * m.SessionCount));
        var totalValue = metrics.Sum(m => m.TotalConversionValue);
        var highImpactPatterns = patterns.Count(p => p.ImpactScore >= 0.7);

        Console.WriteLine("Key Performance Indicators:");
        Console.WriteLine($"  Analytics Windows Processed: {totalWindows}");
        Console.WriteLine($"  User Sessions Analyzed: {totalSessions:N0}");
        Console.WriteLine($"  Unique Users Identified: {totalUsers:N0}");
        Console.WriteLine($"  Conversions Detected: {totalConversions:N0}");
        Console.WriteLine($"  Total Conversion Value: {totalValue:C}");
        Console.WriteLine($"  High-Impact Patterns Found: {highImpactPatterns}");
        Console.WriteLine($"  Processing Time: {processingTime.TotalMilliseconds:F0}ms");
        Console.WriteLine();

        // Business insights
        var avgSessionDuration = metrics.Count > 0
            ? metrics.Average(m => m.AverageSessionDuration.TotalMinutes)
            : 0;

        var avgConversionRate = metrics.Count > 0
            ? metrics.Average(m => m.ConversionRate)
            : 0;

        var avgEngagementScore = metrics.Count > 0
            ? metrics.Average(m => m.EngagementScore)
            : 0;

        Console.WriteLine("Business Insights:");
        Console.WriteLine($"  Average Session Duration: {avgSessionDuration:F1} minutes");
        Console.WriteLine($"  Average Conversion Rate: {avgConversionRate:P1}");
        Console.WriteLine($"  Average Engagement Score: {avgEngagementScore:F3}");

        if (avgConversionRate > 0.05)
            Console.WriteLine("  ✓ Conversion rate is above industry average (>5%)");
        else
            Console.WriteLine("  ⚠ Conversion rate is below industry average (<5%)");

        if (avgEngagementScore > 0.6)
            Console.WriteLine("  ✓ User engagement is strong (>0.6)");
        else
            Console.WriteLine("  ⚠ User engagement needs improvement (<0.6)");

        if (highImpactPatterns > 0)
            Console.WriteLine($"  ⚠ {highImpactPatterns} high-impact patterns require attention");
        else
            Console.WriteLine("  ✓ No high-impact patterns detected");

        Console.WriteLine();
    }

    private void GeneratePerformanceAnalysis(IReadOnlyList<SessionMetrics> metrics, IReadOnlyList<PatternMatch> patterns)
    {
        Console.WriteLine("=== PERFORMANCE ANALYSIS ===");
        Console.WriteLine();

        // Processing performance
        var avgMetricsProcessingTime = metrics.Count > 0
            ? metrics.Average(m => m.ProcessingTimeMs)
            : 0;

        var avgPatternProcessingTime = patterns.Count > 0
            ? patterns.Average(p => p.ProcessingTimeMs)
            : 0;

        var totalProcessingTime = metrics.Sum(m => m.ProcessingTimeMs) + patterns.Sum(p => p.ProcessingTimeMs);

        Console.WriteLine("Processing Performance:");
        Console.WriteLine($"  Average Metrics Processing Time: {avgMetricsProcessingTime:F2}ms");
        Console.WriteLine($"  Average Pattern Detection Time: {avgPatternProcessingTime:F2}ms");
        Console.WriteLine($"  Total Processing Time: {totalProcessingTime:F0}ms");
        Console.WriteLine();

        // Data quality indicators
        var windowsWithNoData = metrics.Count(m => m.SessionCount == 0);
        var patternsWithLowConfidence = patterns.Count(p => p.ConfidenceScore < 0.5);
        var dataQualityScore = CalculateDataQualityScore(metrics, patterns);

        Console.WriteLine("Data Quality Indicators:");
        Console.WriteLine($"  Windows with No Data: {windowsWithNoData}/{metrics.Count} ({(double)windowsWithNoData / metrics.Count * 100:F1}%)");

        Console.WriteLine(
            $"  Low Confidence Patterns: {patternsWithLowConfidence}/{patterns.Count} ({(double)patternsWithLowConfidence / patterns.Count * 100:F1}%)");

        Console.WriteLine($"  Overall Data Quality Score: {dataQualityScore:F3}/1.0");
        Console.WriteLine();

        // Efficiency metrics
        var eventsPerMs = metrics.Sum(m => m.TotalEvents) / Math.Max(totalProcessingTime, 1);
        var patternsPerMs = patterns.Count / Math.Max(avgPatternProcessingTime * patterns.Count, 1);

        Console.WriteLine("Efficiency Metrics:");
        Console.WriteLine($"  Events Processed per Millisecond: {eventsPerMs:F2}");
        Console.WriteLine($"  Patterns Detected per Millisecond: {patternsPerMs:F4}");
        Console.WriteLine($"  Processing Efficiency: {(dataQualityScore > 0.8 ? "Excellent" : dataQualityScore > 0.6 ? "Good" : "Needs Improvement")}");
        Console.WriteLine();
    }

    private static double CalculateDataQualityScore(IReadOnlyList<SessionMetrics> metrics, IReadOnlyList<PatternMatch> patterns)
    {
        if (metrics.Count == 0)
            return 0;

        var completenessScore = 1.0 - (double)metrics.Count(m => m.SessionCount == 0) / metrics.Count;

        var confidenceScore = patterns.Count > 0
            ? patterns.Average(p => p.ConfidenceScore)
            : 1.0;

        var consistencyScore = CalculateConsistencyScore(metrics);

        return completenessScore * 0.4 + confidenceScore * 0.3 + consistencyScore * 0.3;
    }

    private static double CalculateConsistencyScore(IReadOnlyList<SessionMetrics> metrics)
    {
        if (metrics.Count < 2)
            return 1.0;

        var sessionCounts = metrics.Select(m => m.SessionCount).ToList();
        var avgSessions = sessionCounts.Average();
        var variance = sessionCounts.Sum(s => Math.Pow(s - avgSessions, 2)) / sessionCounts.Count;
        var stdDev = Math.Sqrt(variance);

        // Lower standard deviation indicates more consistent window sizes
        return Math.Max(0, 1.0 - stdDev / avgSessions);
    }
}
