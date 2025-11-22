using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_16_TypeConversionNode.Nodes;

/// <summary>
///     Transform node that enriches SensorData with additional business logic and validation.
///     This node demonstrates working with converted data and adding business value.
/// </summary>
/// <remarks>
///     This transform showcases post-conversion processing patterns:
///     - Data validation and quality assessment
///     - Business rule application
///     - Computed field generation
///     - Location assignment based on sensor type
///     - Processing timestamp addition
///     - Error handling and validation messages
///     This pattern is common for adding business intelligence after type conversion.
/// </remarks>
public sealed class SensorDataEnrichmentTransform : TransformNode<SensorData, SensorReading>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="SensorDataEnrichmentTransform" /> class.
    /// </summary>
    public SensorDataEnrichmentTransform()
    {
    }

    /// <summary>
    ///     Enriches sensor data with validation, business logic, and computed fields.
    /// </summary>
    /// <param name="item">The sensor data to enrich.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The enriched sensor reading.</returns>
    public override Task<SensorReading> ExecuteAsync(SensorData item, PipelineContext context, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Validate sensor data
        var (isValid, validationMessage) = ValidateSensorData(item);

        // Generate location based on sensor type
        var location = GenerateLocation(item.SensorType);

        // Calculate Fahrenheit temperature
        var temperatureFahrenheit = ConvertCelsiusToFahrenheit(item.Temperature);

        // Create enriched sensor reading
        var enrichedReading = new SensorReading(
            item.Id,
            item.Timestamp,
            item.Temperature,
            item.Humidity,
            item.Pressure,
            item.SensorType,
            item.Status,
            isValid,
            validationMessage,
            temperatureFahrenheit,
            location,
            DateTime.UtcNow
        );

        return Task.FromResult(enrichedReading);
    }

    /// <summary>
    ///     Validates sensor data and returns validation results.
    /// </summary>
    /// <param name="data">The sensor data to validate.</param>
    /// <returns>A tuple indicating validity and validation message.</returns>
    private static (bool IsValid, string Message) ValidateSensorData(SensorData data)
    {
        var issues = new List<string>();

        // Temperature validation
        if (data.Temperature < -50.0 || data.Temperature > 100.0)
            issues.Add($"Temperature {data.Temperature:F2}°C is outside valid range (-50°C to 100°C)");

        // Humidity validation
        if (data.Humidity < 0.0 || data.Humidity > 100.0)
            issues.Add($"Humidity {data.Humidity:F1}% is outside valid range (0% to 100%)");

        // Pressure validation
        if (data.Pressure < 800.0 || data.Pressure > 1200.0)
            issues.Add($"Pressure {data.Pressure:F1} hPa is outside valid range (800 to 1200 hPa)");

        // Timestamp validation
        if (data.Timestamp > DateTime.UtcNow.AddMinutes(5))
            issues.Add($"Timestamp {data.Timestamp:yyyy-MM-dd HH:mm:ss} is in the future");

        if (data.Timestamp < DateTime.UtcNow.AddDays(30))
            issues.Add($"Timestamp {data.Timestamp:yyyy-MM-dd HH:mm:ss} is more than 30 days old");

        // Status-specific validation
        if (data.Status == SensorStatus.Error && issues.Count == 0)
            issues.Add("Sensor status is Error but all values appear valid");

        if (issues.Count == 0)
            return (true, "Validation passed");

        return (false, string.Join("; ", issues));
    }

    /// <summary>
    ///     Generates location information based on sensor type.
    /// </summary>
    /// <param name="sensorType">The type of sensor.</param>
    /// <returns>A location string for the sensor type.</returns>
    private static string GenerateLocation(SensorType sensorType)
    {
        return sensorType switch
        {
            SensorType.Temperature => "Temperature Control Room A",
            SensorType.Humidity => "Humidity Monitoring Station B",
            SensorType.Pressure => "Pressure Sensor Lab C",
            SensorType.Multi => "Multi-Sensor Array D",
            SensorType.Environmental => "Environmental Monitoring Station E",
            SensorType.Industrial => "Industrial Facility F",
            _ => "Unknown Location",
        };
    }

    /// <summary>
    ///     Converts Celsius temperature to Fahrenheit.
    /// </summary>
    /// <param name="celsius">Temperature in Celsius.</param>
    /// <returns>Temperature in Fahrenheit.</returns>
    private static double ConvertCelsiusToFahrenheit(double celsius)
    {
        return Math.Round(celsius * 9.0 / 5.0 + 32.0, 2);
    }
}
