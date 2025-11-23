using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_LookupNode.Helpers;
using Sample_LookupNode.Models;

namespace Sample_LookupNode.Nodes;

/// <summary>
///     Transform node that calculates risk levels based on enriched sensor data.
///     This node demonstrates how to create a transform that performs risk assessment
///     by inheriting from TransformNode&lt;SensorReadingWithCalibration, EnrichedSensorReading&gt;.
/// </summary>
public class RiskAssessmentNode : TransformNode<SensorReadingWithCalibration, EnrichedSensorReading>
{
    /// <summary>
    ///     Calculates the risk level for the sensor reading.
    /// </summary>
    /// <param name="item">The input item to assess.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the enriched sensor reading with risk assessment.</returns>
    public override async Task<EnrichedSensorReading> ExecuteAsync(SensorReadingWithCalibration item, PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Simulate processing delay
        await Task.Delay(Random.Shared.Next(5, 15), cancellationToken);

        // Calculate risk level using the RiskCalculator helper
        var riskLevel = RiskCalculator.CalculateRisk(item);

        // Create enrichment metadata with additional analysis
        var enrichmentMetadata = new Dictionary<string, object>
        {
            ["RiskDescription"] = RiskCalculator.GetRiskDescription(riskLevel),
            ["CalibrationWarning"] = item.CalibrationWarning,
            ["DaysUntilCalibration"] = item.DaysUntilCalibration,
            ["DeviceAgeDays"] = (DateTime.UtcNow - item.DeviceInfo.InstallationDate).TotalDays,
            ["LastCalibrationDaysAgo"] = (DateTime.UtcNow - item.DeviceInfo.LastCalibration).TotalDays,
            ["ProcessingTimestamp"] = DateTime.UtcNow,
            ["RiskFactors"] = GetRiskFactors(item, riskLevel),
        };

        Console.WriteLine($"Risk assessment for {item.OriginalReading.DeviceId}: {riskLevel} - {RiskCalculator.GetRiskDescription(riskLevel)}");

        return new EnrichedSensorReading(
            item.OriginalReading,
            item.DeviceInfo,
            item.CalibrationValid,
            riskLevel,
            enrichmentMetadata
        );
    }

    /// <summary>
    ///     Identifies specific risk factors that contributed to the risk assessment.
    /// </summary>
    private static List<string> GetRiskFactors(SensorReadingWithCalibration item, RiskLevel riskLevel)
    {
        var factors = new List<string>();
        var device = item.DeviceInfo;
        var reading = item.OriginalReading;

        // Device status factors
        if (device.DeviceStatus.Equals("Offline", StringComparison.OrdinalIgnoreCase))
            factors.Add("Device is offline");

        if (device.DeviceStatus.Equals("Error", StringComparison.OrdinalIgnoreCase))
            factors.Add("Device reported error");

        if (device.DeviceStatus.Equals("CalibrationRequired", StringComparison.OrdinalIgnoreCase))
            factors.Add("Device requires calibration");

        if (device.DeviceStatus.Equals("Maintenance", StringComparison.OrdinalIgnoreCase))
            factors.Add("Device under maintenance");

        // Calibration factors
        if (!item.CalibrationValid)
        {
            if (item.DaysUntilCalibration < 0)
                factors.Add($"Calibration overdue by {Math.Abs(item.DaysUntilCalibration)} days");
            else
                factors.Add("Reading outside calibration period");
        }
        else if (item.DaysUntilCalibration <= 30)
            factors.Add($"Calibration expiring in {item.DaysUntilCalibration} days");

        // Reading value factors
        var readingRisk = GetReadingValueRisk(reading.ReadingType, reading.Value);

        if (readingRisk >= RiskLevel.High)
            factors.Add($"Abnormal {reading.ReadingType.ToLowerInvariant()} reading: {reading.Value}{reading.Unit}");

        // Device age factors
        var deviceAgeDays = (DateTime.UtcNow - device.InstallationDate).TotalDays;

        if (deviceAgeDays > 365 * 5) // 5 years
            factors.Add("Device is older than 5 years");

        // If no specific factors but risk is elevated, add general factor
        if (factors.Count == 0 && riskLevel > RiskLevel.Low)
            factors.Add("General risk factors detected");

        return factors;
    }

    /// <summary>
    ///     Gets risk level based on sensor reading values.
    /// </summary>
    private static RiskLevel GetReadingValueRisk(string readingType, double value)
    {
        return readingType.ToLowerInvariant() switch
        {
            "temperature" => GetTemperatureRisk(value),
            "pressure" => GetPressureRisk(value),
            "humidity" => GetHumidityRisk(value),
            "vibration" => GetVibrationRisk(value),
            _ => RiskLevel.Low,
        };
    }

    /// <summary>
    ///     Calculates risk level for temperature readings.
    /// </summary>
    private static RiskLevel GetTemperatureRisk(double temperature)
    {
        // Temperature in Celsius
        if (temperature < -30 || temperature > 80)
            return RiskLevel.Critical;

        if (temperature < -20 || temperature > 60)
            return RiskLevel.High;

        if (temperature < -10 || temperature > 45)
            return RiskLevel.Medium;

        return RiskLevel.Low;
    }

    /// <summary>
    ///     Calculates risk level for pressure readings.
    /// </summary>
    private static RiskLevel GetPressureRisk(double pressure)
    {
        // Pressure in PSI
        if (pressure > 500 || pressure < 0)
            return RiskLevel.Critical;

        if (pressure > 400 || pressure < 10)
            return RiskLevel.High;

        if (pressure > 300 || pressure < 20)
            return RiskLevel.Medium;

        return RiskLevel.Low;
    }

    /// <summary>
    ///     Calculates risk level for humidity readings.
    /// </summary>
    private static RiskLevel GetHumidityRisk(double humidity)
    {
        // Humidity in percentage
        if (humidity > 95 || humidity < 5)
            return RiskLevel.Critical;

        if (humidity > 85 || humidity < 15)
            return RiskLevel.High;

        if (humidity > 75 || humidity < 25)
            return RiskLevel.Medium;

        return RiskLevel.Low;
    }

    /// <summary>
    ///     Calculates risk level for vibration readings.
    /// </summary>
    private static RiskLevel GetVibrationRisk(double vibration)
    {
        // Vibration in mm/s
        if (vibration > 50)
            return RiskLevel.Critical;

        if (vibration > 30)
            return RiskLevel.High;

        if (vibration > 15)
            return RiskLevel.Medium;

        return RiskLevel.Low;
    }
}
