using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_TypeConversionNode.Nodes;

/// <summary>
///     Sink node that captures and reports conversion errors for monitoring and debugging.
///     This node demonstrates error handling and monitoring in type conversion pipelines.
/// </summary>
/// <remarks>
///     This sink showcases error handling patterns:
///     - Error collection and aggregation
///     - Error categorization and analysis
///     - Performance metrics for conversion success rates
///     - Detailed error reporting for debugging
///     - Threshold-based alerting
///     This pattern is essential for production monitoring and troubleshooting.
/// </remarks>
public sealed class ErrorSink : SinkNode<ConversionError>
{
    private readonly Dictionary<string, int> _errorCounts;
    private readonly List<ConversionError> _errors;
    private int _totalProcessed;

    /// <summary>
    ///     Initializes a new instance of <see cref="ErrorSink" /> class.
    /// </summary>
    public ErrorSink()
    {
        _errors = new List<ConversionError>();
        _errorCounts = new Dictionary<string, int>();
        _totalProcessed = 0;
    }

    /// <summary>
    ///     Processes conversion errors and generates monitoring reports.
    /// </summary>
    /// <param name="input">The conversion errors to process.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing error processing operation.</returns>
    public override async Task ExecuteAsync(IDataPipe<ConversionError> input, PipelineContext context, CancellationToken cancellationToken)
    {
        await foreach (var error in input.WithCancellation(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            _errors.Add(error);
            _totalProcessed++;

            // Count errors by source type using TryGetValue for efficiency
            if (_errorCounts.TryGetValue(error.SourceType, out var count))
                _errorCounts[error.SourceType] = count + 1;
            else
                _errorCounts[error.SourceType] = 1;

            // Output individual error for immediate visibility
            OutputError(error, _errors.Count);
        }

        // Generate summary report
        GenerateErrorReport();
    }

    /// <summary>
    ///     Outputs individual error details.
    /// </summary>
    /// <param name="error">The conversion error to output.</param>
    /// <param name="index">The error index.</param>
    private static void OutputError(ConversionError error, int index)
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine($"\n🚨 Conversion Error #{index}");
        Console.WriteLine(new string('=', 50));
        Console.ResetColor();

        Console.WriteLine($"  Source ID: {error.SourceId}");
        Console.WriteLine($"  Source Type: {error.SourceType}");
        Console.WriteLine($"  Error Time: {error.ErrorTime:yyyy-MM-dd HH:mm:ss}");
        Console.WriteLine($"  Error Message: {error.ErrorMessage}");

        if (error.Exception != null)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine($"  Exception: {error.Exception.GetType().Name}");
            Console.WriteLine($"  Exception Message: {error.Exception.Message}");
            Console.ResetColor();
        }
    }

    /// <summary>
    ///     Generates a comprehensive error report with analysis.
    /// </summary>
    private void GenerateErrorReport()
    {
        Console.WriteLine();
        Console.ForegroundColor = ConsoleColor.Cyan;
        Console.WriteLine("📊 Error Analysis Report");
        Console.WriteLine(new string('=', 50));
        Console.ResetColor();

        Console.WriteLine($"  Total Errors Processed: {_errors.Count}");
        Console.WriteLine($"  Total Items Processed: {_totalProcessed}");

        if (_totalProcessed > 0)
        {
            var errorRate = (double)_errors.Count / _totalProcessed * 100;
            var successRate = 100 - errorRate;

            Console.ForegroundColor = errorRate > 10
                ? ConsoleColor.Red
                : errorRate > 5
                    ? ConsoleColor.Yellow
                    : ConsoleColor.Green;

            Console.WriteLine($"  Error Rate: {errorRate:F2}%");
            Console.ResetColor();

            Console.ForegroundColor = successRate < 90
                ? ConsoleColor.Red
                : successRate < 95
                    ? ConsoleColor.Yellow
                    : ConsoleColor.Green;

            Console.WriteLine($"  Success Rate: {successRate:F2}%");
            Console.ResetColor();
        }

        Console.WriteLine();
        Console.ForegroundColor = ConsoleColor.White;
        Console.WriteLine("📈 Errors by Source Type:");
        Console.ResetColor();

        foreach (var kvp in _errorCounts)
        {
            var percentage = _totalProcessed > 0
                ? (double)kvp.Value / _totalProcessed * 100
                : 0;

            var color = percentage > 10
                ? ConsoleColor.Red
                : percentage > 5
                    ? ConsoleColor.Yellow
                    : ConsoleColor.Green;

            Console.ForegroundColor = color;
            Console.WriteLine($"  {kvp.Key}: {kvp.Value} errors ({percentage:F1}%)");
            Console.ResetColor();
        }

        // Generate recommendations
        GenerateRecommendations();

        Console.WriteLine();
        Console.ForegroundColor = ConsoleColor.Green;
        Console.WriteLine("✅ Error processing completed");
        Console.ResetColor();
    }

    /// <summary>
    ///     Generates recommendations based on error analysis.
    /// </summary>
    private void GenerateRecommendations()
    {
        Console.WriteLine();
        Console.ForegroundColor = ConsoleColor.White;
        Console.WriteLine("💡 Recommendations:");
        Console.ResetColor();

        var totalErrors = _errors.Count;
        var totalProcessed = _totalProcessed;

        if (totalErrors == 0)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("  🎉 No errors detected! All conversions successful.");
            Console.ResetColor();
            return;
        }

        var errorRate = (double)totalErrors / totalProcessed * 100;

        if (errorRate > 20)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine("  🔴 High error rate detected (>20%). Review input data quality and validation logic.");
        }
        else if (errorRate > 10)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("  🟡 Moderate error rate detected (>10%). Consider improving error handling.");
        }
        else
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("  🟢 Error rate is acceptable (<10%). Continue monitoring.");
        }

        Console.ResetColor();

        // Check for common error patterns
        var commonErrors = new Dictionary<string, int>();

        foreach (var error in _errors)
        {
            var key = GetErrorCategory(error.ErrorMessage);

            if (commonErrors.TryGetValue(key, out var count))
                commonErrors[key] = count + 1;
            else
                commonErrors[key] = 1;
        }

        var mostCommonError = "";
        var maxCount = 0;

        foreach (var kvp in commonErrors)
        {
            if (kvp.Value > maxCount)
            {
                maxCount = kvp.Value;
                mostCommonError = kvp.Key;
            }
        }

        if (!string.IsNullOrEmpty(mostCommonError))
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine($"  🔍 Most common error category: {mostCommonError} ({maxCount} occurrences)");
            Console.ResetColor();
        }
    }

    /// <summary>
    ///     Categorizes error messages for analysis.
    /// </summary>
    /// <param name="errorMessage">The error message to categorize.</param>
    /// <returns>A category string for error.</returns>
    private static string GetErrorCategory(string errorMessage)
    {
        if (string.IsNullOrEmpty(errorMessage))
            return "Unknown";

        var lowerMessage = errorMessage.ToLowerInvariant();

        if (lowerMessage.Contains("parse") || lowerMessage.Contains("format"))
            return "Parsing/Format";

        if (lowerMessage.Contains("range") || lowerMessage.Contains("invalid"))
            return "Validation";

        if (lowerMessage.Contains("null") || lowerMessage.Contains("reference"))
            return "Null Reference";

        if (lowerMessage.Contains("conversion") || lowerMessage.Contains("cast"))
            return "Type Conversion";

        if (lowerMessage.Contains("json") || lowerMessage.Contains("deserialize"))
            return "JSON Processing";

        if (lowerMessage.Contains("timeout") || lowerMessage.Contains("network"))
            return "Network/Timeout";

        return "Other";
    }
}
