using Sample_LookupNode.Models;

namespace Sample_LookupNode.Helpers;

/// <summary>
///     Helper class for calculating risk levels based on sensor readings and device metadata.
///     This class implements business logic for IoT risk assessment scenarios.
/// </summary>
public static class RiskCalculator
{
    /// <summary>
    ///     Gets the reset color code for console output.
    /// </summary>
    public static string ResetColorCode => "\u001b[0m";

    /// <summary>
    ///     Calculates the risk level for a sensor reading with calibration validation.
    /// </summary>
    /// <param name="readingWithCalibration">The sensor reading with calibration information.</param>
    /// <returns>The calculated risk level.</returns>
    public static RiskLevel CalculateRisk(SensorReadingWithCalibration readingWithCalibration)
    {
        var reading = readingWithCalibration.OriginalReading;
        var device = readingWithCalibration.DeviceInfo;

        // Check for critical device status issues first
        var deviceStatusRisk = GetDeviceStatusRisk(device.DeviceStatus);

        if (deviceStatusRisk == RiskLevel.Critical)
            return RiskLevel.Critical;

        // Check calibration validity
        if (!readingWithCalibration.CalibrationValid)
        {
            // Overdue calibration is high risk
            if (readingWithCalibration.DaysUntilCalibration < 0)
                return RiskLevel.High;

            // Calibration expiring soon is medium risk
            if (readingWithCalibration.DaysUntilCalibration <= 30)
                return RiskLevel.Medium;
        }

        // Check reading value ranges for each sensor type
        var readingRisk = GetReadingValueRisk(reading.ReadingType, reading.Value);

        // Combine risks - take the higher risk
        return CombineRisks(deviceStatusRisk, readingRisk);
    }

    /// <summary>
    ///     Gets risk level based on device status.
    /// </summary>
    private static RiskLevel GetDeviceStatusRisk(string deviceStatus)
    {
        return deviceStatus.ToLowerInvariant() switch
        {
            "offline" => RiskLevel.High,
            "error" => RiskLevel.Critical,
            "calibrationrequired" => RiskLevel.High,
            "maintenance" => RiskLevel.Medium,
            "online" => RiskLevel.Low,
            _ => RiskLevel.Medium,
        };
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

    /// <summary>
    ///     Combines multiple risk levels by returning the highest risk.
    /// </summary>
    private static RiskLevel CombineRisks(params RiskLevel[] risks)
    {
        return risks.Max();
    }

    /// <summary>
    ///     Gets a human-readable description of the risk level.
    /// </summary>
    public static string GetRiskDescription(RiskLevel riskLevel)
    {
        return riskLevel switch
        {
            RiskLevel.Low => "Normal operating conditions",
            RiskLevel.Medium => "Monitor closely - potential issues developing",
            RiskLevel.High => "Attention required - significant risk detected",
            RiskLevel.Critical => "Immediate action required - critical risk",
            _ => "Unknown risk level",
        };
    }

    /// <summary>
    ///     Gets color code for console output based on risk level.
    /// </summary>
    public static string GetRiskColorCode(RiskLevel riskLevel)
    {
        return riskLevel switch
        {
            RiskLevel.Low => "\u001b[32m", // Green
            RiskLevel.Medium => "\u001b[33m", // Yellow
            RiskLevel.High => "\u001b[31m", // Red
            RiskLevel.Critical => "\u001b[41m", // Red background
            _ => "\u001b[0m", // Reset
        };
    }
}
