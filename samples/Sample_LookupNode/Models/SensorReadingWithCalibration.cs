namespace Sample_LookupNode.Models;

/// <summary>
///     Represents a sensor reading that has been enriched with device metadata and calibration validation.
///     This record is an intermediate model between device lookup and risk assessment.
/// </summary>
/// <param name="OriginalReading">The original raw sensor reading before enrichment.</param>
/// <param name="DeviceInfo">Device metadata retrieved during the enrichment process.</param>
/// <param name="CalibrationValid">Indicates whether the device calibration is currently valid.</param>
/// <param name="CalibrationWarning">Warning message if calibration is invalid or expiring soon.</param>
/// <param name="DaysUntilCalibration">Number of days until next calibration is due.</param>
public record SensorReadingWithCalibration(
    SensorReading OriginalReading,
    DeviceMetadata DeviceInfo,
    bool CalibrationValid,
    string CalibrationWarning,
    int DaysUntilCalibration
);
