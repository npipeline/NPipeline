namespace Sample_LookupNode.Models;

/// <summary>
///     Represents an enriched sensor reading with device metadata and analysis.
///     This record combines the original sensor reading with device information and calculated metrics.
/// </summary>
/// <param name="OriginalReading">The original raw sensor reading before enrichment.</param>
/// <param name="DeviceInfo">Device metadata retrieved during the enrichment process.</param>
/// <param name="CalibrationValid">Indicates whether the device calibration is currently valid.</param>
/// <param name="RiskLevel">Calculated risk level based on sensor value and device status.</param>
/// <param name="EnrichmentMetadata">Additional metadata generated during the enrichment process.</param>
public record EnrichedSensorReading(
    SensorReading OriginalReading,
    DeviceMetadata DeviceInfo,
    bool CalibrationValid,
    RiskLevel RiskLevel,
    Dictionary<string, object> EnrichmentMetadata
);
