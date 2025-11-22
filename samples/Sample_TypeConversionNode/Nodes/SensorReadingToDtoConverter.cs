using NPipeline.Nodes;

namespace Sample_TypeConversionNode.Nodes;

/// <summary>
///     Type conversion node that transforms SensorReading to SensorDto for API responses.
///     This node demonstrates API response formatting with different naming conventions.
/// </summary>
/// <remarks>
///     This converter showcases API integration patterns:
///     - PascalCase to snake_case naming convention conversion
///     - String formatting for API responses
///     - Data type conversion for serialization compatibility
///     - Boolean and numeric formatting
///     - DateTime formatting for API standards
///     This pattern is essential for creating API responses from domain objects.
/// </remarks>
public sealed class SensorReadingToDtoConverter : TypeConversionNode<SensorReading, SensorDto>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="SensorReadingToDtoConverter" /> class.
    /// </summary>
    public SensorReadingToDtoConverter()
    {
        // No AutoMap() due to completely different naming conventions
        // All mappings are custom to handle PascalCase to snake_case conversion

        Map(
            src => src.Id,
            dst => dst.sensor_id,
            id => id.ToString("N") // Format as lowercase hex without hyphens
        );

        Map(
            src => src.Timestamp,
            dst => dst.timestamp,
            timestamp => timestamp.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")
        );

        Map(
            src => src.Temperature,
            dst => dst.temperature_celsius,
            temp => temp.ToString("F2")
        );

        Map(
            src => src.TemperatureFahrenheit,
            dst => dst.temperature_fahrenheit,
            temp => temp.ToString("F2")
        );

        Map(
            src => src.Humidity,
            dst => dst.humidity_percent,
            humidity => humidity.ToString("F1")
        );

        Map(
            src => src.Pressure,
            dst => dst.pressure_hpa,
            pressure => pressure.ToString("F1")
        );

        Map(
            src => src.SensorType,
            dst => dst.sensor_type,
            sensorType => sensorType.ToString().ToLowerInvariant()
        );

        Map(
            src => src.Status,
            dst => dst.status,
            status => status.ToString().ToLowerInvariant()
        );

        Map(
            src => src.Location,
            dst => dst.location,
            location => location
        );

        Map(
            src => src.IsValid,
            dst => dst.is_valid,
            isValid => isValid
        );
    }
}
