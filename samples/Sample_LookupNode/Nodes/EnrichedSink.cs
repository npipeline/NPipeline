using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_LookupNode.Helpers;
using Sample_LookupNode.Models;

namespace Sample_LookupNode.Nodes;

/// <summary>
///     Sink node that outputs enriched sensor data for analysis.
///     This node demonstrates how to create a sink that consumes enriched sensor data
///     by inheriting from SinkNode&lt;EnrichedSensorReading&gt;.
/// </summary>
public class EnrichedSink : SinkNode<EnrichedSensorReading>
{
    /// <summary>
    ///     Processes the enriched sensor readings by outputting them for analysis.
    /// </summary>
    /// <param name="input">The data pipe containing enriched sensor readings to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task representing the sink execution.</returns>
    public override async Task ExecuteAsync(IDataPipe<EnrichedSensorReading> input, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════");
        Console.WriteLine("           ENRICHED SENSOR DATA ANALYSIS REPORT");
        Console.WriteLine("═══════════════════════════════════════════════════════════════");
        Console.WriteLine();

        var processedCount = 0;
        var riskCounts = new Dictionary<RiskLevel, int>();

        await foreach (var enrichedReading in input.WithCancellation(cancellationToken))
        {
            DisplayEnrichedReading(enrichedReading);

            // Track statistics
            processedCount++;
            riskCounts[enrichedReading.RiskLevel] = riskCounts.GetValueOrDefault(enrichedReading.RiskLevel, 0) + 1;

            // Add small delay for readability
            await Task.Delay(100, cancellationToken);
        }

        DisplaySummaryStatistics(processedCount, riskCounts);
    }

    /// <summary>
    ///     Displays a single enriched sensor reading with color-coded formatting.
    /// </summary>
    private static void DisplayEnrichedReading(EnrichedSensorReading enrichedReading)
    {
        var reading = enrichedReading.OriginalReading;
        var device = enrichedReading.DeviceInfo;
        var riskColor = RiskCalculator.GetRiskColorCode(enrichedReading.RiskLevel);
        var resetColor = RiskCalculator.ResetColorCode;

        Console.WriteLine($"{riskColor}┌─────────────────────────────────────────────────────────────────┐{resetColor}");
        Console.WriteLine($"{riskColor}│ SENSOR READING: {reading.DeviceId,-52} │{resetColor}");
        Console.WriteLine($"{riskColor}├─────────────────────────────────────────────────────────────────┤{resetColor}");
        Console.WriteLine($"│ Reading Type: {reading.ReadingType,-45} │");
        Console.WriteLine($"│ Value: {reading.Value}{reading.Unit,-52} │");
        Console.WriteLine($"│ Timestamp: {reading.Timestamp:yyyy-MM-dd HH:mm:ss UTC,-36} │");

        Console.WriteLine(
            $"│ Risk Level: {riskColor}{enrichedReading.RiskLevel,-13}{resetColor}{RiskCalculator.GetRiskDescription(enrichedReading.RiskLevel),-32} │");

        Console.WriteLine($"{riskColor}├─────────────────────────────────────────────────────────────────┤{resetColor}");
        Console.WriteLine($"│ Device Type: {device.DeviceType,-47} │");
        Console.WriteLine($"│ Location: {device.FactoryLocation,-49} │");
        Console.WriteLine($"│ Status: {device.DeviceStatus,-51} │");
        Console.WriteLine($"│ Calibration: {(enrichedReading.CalibrationValid ? "✓ Valid" : "✗ Invalid"),-46} │");
        Console.WriteLine($"{riskColor}├─────────────────────────────────────────────────────────────────┤{resetColor}");

        // Display risk factors if any
        if (enrichedReading.EnrichmentMetadata.TryGetValue("RiskFactors", out var riskFactorsObj) &&
            riskFactorsObj is List<string> riskFactors && riskFactors.Count > 0)
        {
            Console.WriteLine("│ Risk Factors:                                                    │");

            foreach (var factor in riskFactors.Take(3)) // Limit to first 3 factors for space
            {
                Console.WriteLine($"│ • {factor,-62} │");
            }

            if (riskFactors.Count > 3)
                Console.WriteLine($"│ • ... and {riskFactors.Count - 3} more factor(s) {-54} │");
        }

        Console.WriteLine($"{riskColor}└─────────────────────────────────────────────────────────────────┘{resetColor}");
        Console.WriteLine();
    }

    /// <summary>
    ///     Displays summary statistics for all processed readings.
    /// </summary>
    private static void DisplaySummaryStatistics(int processedCount, Dictionary<RiskLevel, int> riskCounts)
    {
        Console.WriteLine("═══════════════════════════════════════════════════════════════");
        Console.WriteLine("                        SUMMARY STATISTICS");
        Console.WriteLine("═══════════════════════════════════════════════════════════════");
        Console.WriteLine($"Total Readings Processed: {processedCount}");
        Console.WriteLine();
        Console.WriteLine("Risk Level Distribution:");

        foreach (var riskLevel in Enum.GetValues<RiskLevel>())
        {
            var count = riskCounts.GetValueOrDefault(riskLevel, 0);

            var percentage = processedCount > 0
                ? (count * 100.0 / processedCount).ToString("F1")
                : "0.0";

            var color = RiskCalculator.GetRiskColorCode(riskLevel);
            var reset = RiskCalculator.ResetColorCode;

            Console.WriteLine($"  {color}{riskLevel,-10}{reset} : {count,3} readings ({percentage}%)");
        }

        Console.WriteLine();
        Console.WriteLine("═══════════════════════════════════════════════════════════════");
        Console.WriteLine();
    }
}
