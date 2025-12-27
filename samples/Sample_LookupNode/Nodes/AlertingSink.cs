using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_LookupNode.Helpers;
using Sample_LookupNode.Models;

namespace Sample_LookupNode.Nodes;

/// <summary>
///     Sink node that handles high-priority alerts from sensor data.
///     This node demonstrates how to create a sink that processes alerts
///     by inheriting from SinkNode&lt;EnrichedSensorReading&gt;.
/// </summary>
public class AlertingSink : SinkNode<EnrichedSensorReading>
{
    /// <summary>
    ///     Processes the enriched sensor readings and generates alerts for high-priority issues.
    /// </summary>
    /// <param name="input">The data pipe containing enriched sensor readings to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task representing the sink execution.</returns>
    public override async Task ExecuteAsync(IDataPipe<EnrichedSensorReading> input, PipelineContext context, CancellationToken cancellationToken)
    {
        Console.WriteLine();
        Console.WriteLine("🚨 ALERT SYSTEM ACTIVATED 🚨");
        Console.WriteLine("═══════════════════════════════════════════════════════════════");
        Console.WriteLine("           CRITICAL ISSUES REQUIRING IMMEDIATE ATTENTION");
        Console.WriteLine("═══════════════════════════════════════════════════════════════");
        Console.WriteLine();

        var alertCount = 0;
        var criticalCount = 0;
        var calibrationAlerts = 0;

        await foreach (var enrichedReading in input.WithCancellation(cancellationToken))
        {
            if (ShouldGenerateAlert(enrichedReading))
            {
                DisplayAlert(enrichedReading);
                alertCount++;

                if (enrichedReading.RiskLevel == RiskLevel.Critical)
                    criticalCount++;

                if (!enrichedReading.CalibrationValid)
                    calibrationAlerts++;
            }

            // Add small delay for readability
            await Task.Delay(50, cancellationToken);
        }

        DisplayAlertSummary(alertCount, criticalCount, calibrationAlerts);
    }

    /// <summary>
    ///     Determines if an alert should be generated for the given enriched reading.
    /// </summary>
    private static bool ShouldGenerateAlert(EnrichedSensorReading enrichedReading)
    {
        // Generate alerts for high or critical risk levels
        if (enrichedReading.RiskLevel >= RiskLevel.High)
            return true;

        // Generate alerts for calibration issues
        if (!enrichedReading.CalibrationValid)
            return true;

        // Generate alerts for devices with problematic status
        var deviceStatus = enrichedReading.DeviceInfo.DeviceStatus.ToLowerInvariant();

        if (deviceStatus is "offline" or "error" or "calibrationrequired")
            return true;

        return false;
    }

    /// <summary>
    ///     Displays an alert for a single enriched sensor reading.
    /// </summary>
    private static void DisplayAlert(EnrichedSensorReading enrichedReading)
    {
        var reading = enrichedReading.OriginalReading;
        var device = enrichedReading.DeviceInfo;

        // Determine alert type and styling
        var alertType = GetAlertType(enrichedReading);
        var alertColor = GetAlertColor(enrichedReading.RiskLevel);
        var resetColor = RiskCalculator.ResetColorCode;

        Console.WriteLine(
            $"{alertColor}╔══════════════════════════════════════════════════════════════════════════════════════════════════════════════╗{resetColor}");

        Console.WriteLine($"{alertColor}║ {alertType,-128} ║{resetColor}");

        Console.WriteLine(
            $"{alertColor}╠══════════════════════════════════════════════════════════════════════════════════════════════════════════════╣{resetColor}");

        Console.WriteLine($"║ DEVICE:        {device.DeviceId,-115} ║");
        Console.WriteLine($"║ LOCATION:      {device.FactoryLocation,-115} ║");
        Console.WriteLine($"║ TYPE:          {device.DeviceType,-115} ║");
        Console.WriteLine($"║ STATUS:        {device.DeviceStatus,-115} ║");
        Console.WriteLine($"║ READING:       {reading.ReadingType} = {reading.Value}{reading.Unit,-95} ║");
        Console.WriteLine($"║ TIMESTAMP:     {reading.Timestamp:yyyy-MM-dd HH:mm:ss UTC,-95} ║");
        Console.WriteLine($"║ RISK LEVEL:    {enrichedReading.RiskLevel,-115} ║");
        Console.WriteLine($"║ CALIBRATION:   {(enrichedReading.CalibrationValid ? "VALID" : "INVALID"),-115} ║");

        Console.WriteLine(
            $"{alertColor}╠══════════════════════════════════════════════════════════════════════════════════════════════════════════════╣{resetColor}");

        // Display alert details
        if (enrichedReading.EnrichmentMetadata.TryGetValue("RiskFactors", out var riskFactorsObj) &&
            riskFactorsObj is List<string> riskFactors && riskFactors.Count > 0)
        {
            Console.WriteLine("║ RISK FACTORS:                                                                                   ║");

            foreach (var factor in riskFactors)
            {
                Console.WriteLine($"║ • {factor,-123} ║");
            }
        }

        // Display calibration warning if applicable
        if (enrichedReading.EnrichmentMetadata.TryGetValue("CalibrationWarning", out var warningObj) &&
            warningObj is string warning)
            Console.WriteLine($"║ CALIBRATION WARNING: {warning,-103} ║");

        // Display recommended action
        var action = GetRecommendedAction(enrichedReading);
        Console.WriteLine($"║ RECOMMENDED ACTION: {action,-97} ║");

        Console.WriteLine(
            $"{alertColor}╚══════════════════════════════════════════════════════════════════════════════════════════════════════════════╝{resetColor}");

        Console.WriteLine();
        Console.WriteLine();
    }

    /// <summary>
    ///     Gets the alert type header based on the enriched reading.
    /// </summary>
    private static string GetAlertType(EnrichedSensorReading enrichedReading)
    {
        if (enrichedReading.RiskLevel == RiskLevel.Critical)
            return "🚨 CRITICAL ALERT - IMMEDIATE ACTION REQUIRED 🚨";

        if (enrichedReading.RiskLevel == RiskLevel.High)
            return "⚠️  HIGH PRIORITY ALERT - ATTENTION REQUIRED ⚠️";

        if (!enrichedReading.CalibrationValid)
            return "🔧 CALIBRATION ALERT - MAINTENANCE NEEDED 🔧";

        return "📊 MONITORING ALERT - SITUATION AWARENESS 📊";
    }

    /// <summary>
    ///     Gets the color code for the alert based on risk level.
    /// </summary>
    private static string GetAlertColor(RiskLevel riskLevel)
    {
        return riskLevel switch
        {
            RiskLevel.Critical => "\u001b[41;97m", // Red background with white text
            RiskLevel.High => "\u001b[31m", // Red text
            RiskLevel.Medium => "\u001b[33m", // Yellow text
            _ => "\u001b[0m", // Default
        };
    }

    /// <summary>
    ///     Gets the recommended action based on the enriched reading.
    /// </summary>
    private static string GetRecommendedAction(EnrichedSensorReading enrichedReading)
    {
        var deviceStatus = enrichedReading.DeviceInfo.DeviceStatus.ToLowerInvariant();

        if (deviceStatus == "offline")
            return "Check device connectivity and power supply";

        if (deviceStatus == "error")
            return "Investigate device error logs and perform diagnostics";

        if (deviceStatus == "calibrationrequired" || !enrichedReading.CalibrationValid)
            return "Schedule immediate calibration maintenance";

        if (enrichedReading.RiskLevel == RiskLevel.Critical)
            return "Evacuate area if necessary and contact emergency services";

        if (enrichedReading.RiskLevel == RiskLevel.High)
            return "Dispatch maintenance team immediately";

        return "Monitor closely and prepare maintenance plan";
    }

    /// <summary>
    ///     Displays a summary of all alerts generated.
    /// </summary>
    private static void DisplayAlertSummary(int alertCount, int criticalCount, int calibrationAlerts)
    {
        Console.WriteLine("═══════════════════════════════════════════════════════════════");
        Console.WriteLine("                        ALERT SUMMARY");
        Console.WriteLine("═══════════════════════════════════════════════════════════════");
        Console.WriteLine($"Total Alerts Generated: {alertCount}");
        Console.WriteLine($"Critical Alerts: {criticalCount}");
        Console.WriteLine($"Calibration Alerts: {calibrationAlerts}");

        if (alertCount == 0)
        {
            Console.WriteLine();
            Console.WriteLine("✅ No alerts generated - All systems operating within normal parameters.");
        }
        else
        {
            Console.WriteLine();
            Console.WriteLine("⚠️  Please review all alerts and take appropriate action.");
            Console.WriteLine("   Contact maintenance team for critical issues.");
        }

        Console.WriteLine("═══════════════════════════════════════════════════════════════");
        Console.WriteLine();
    }
}
