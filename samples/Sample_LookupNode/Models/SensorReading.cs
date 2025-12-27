namespace Sample_LookupNode.Models;

/// <summary>
///     Represents a raw sensor reading from an IoT device.
///     This record contains the basic data captured by sensors before enrichment.
/// </summary>
/// <param name="DeviceId">Unique identifier for the IoT device that generated the reading.</param>
/// <param name="Timestamp">UTC timestamp when the reading was captured.</param>
/// <param name="Value">The numerical value of the sensor reading.</param>
/// <param name="Unit">The unit of measurement for the sensor value (e.g., "°C", "kPa", "%").</param>
/// <param name="ReadingType">The type of sensor reading (e.g., "Temperature", "Pressure", "Humidity").</param>
public record SensorReading(
    string DeviceId,
    DateTime Timestamp,
    double Value,
    string Unit,
    string ReadingType
);
