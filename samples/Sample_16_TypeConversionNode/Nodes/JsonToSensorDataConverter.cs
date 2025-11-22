using System;
using System.Text.Json;
using NPipeline.Nodes;

namespace Sample_16_TypeConversionNode.Nodes;

/// <summary>
///     Type conversion node that transforms JsonStringData to SensorData.
///     This node demonstrates JSON deserialization with complex nested structure extraction.
/// </summary>
/// <remarks>
///     This converter showcases advanced TypeConversionNode capabilities:
///     - JSON deserialization and nested object extraction
///     - Complex data transformation using whole input object
///     - Type conversion between different numeric formats
///     - Handling optional fields and null values
///     - Business logic during transformation
///     This pattern is common when integrating with modern APIs and microservices.
/// </remarks>
public sealed class JsonToSensorDataConverter : TypeConversionNode<JsonStringData, SensorData>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="JsonToSensorDataConverter" /> class.
    /// </summary>
    public JsonToSensorDataConverter() : base(CreateSensorDataFromJson)
    {
        // No AutoMap() since we need to extract data from nested JSON structure
        // All mappings will be custom to handle the complex JSON structure

        Map(
            dst => dst.Id,
            input => ExtractSensorId(input.JsonContent)
        );

        Map(
            dst => dst.Timestamp,
            input => ExtractTimestamp(input.JsonContent)
        );

        Map(
            dst => dst.Temperature,
            input => ExtractReading(input.JsonContent, "temperature")
        );

        Map(
            dst => dst.Humidity,
            input => ExtractReading(input.JsonContent, "humidity")
        );

        Map(
            dst => dst.Pressure,
            input => ExtractReading(input.JsonContent, "pressure")
        );

        Map(
            dst => dst.SensorType,
            input => DetermineSensorType(input.JsonContent, input.Source)
        );

        Map(
            dst => dst.Status,
            input => DetermineSensorStatus(input.JsonContent)
        );
    }

    /// <summary>
    ///     Custom factory function that creates SensorData from JSON content.
    /// </summary>
    /// <param name="input">The JSON string data input.</param>
    /// <returns>A new SensorData instance with default values.</returns>
    private static SensorData CreateSensorDataFromJson(JsonStringData input)
    {
        // Create a default instance that will be populated by mappings
        return new SensorData(
            Guid.NewGuid(), // Will be overridden by mapping
            DateTime.UtcNow, // Will be overridden by mapping
            0.0, // Will be overridden by mapping
            0.0, // Will be overridden by mapping
            0.0, // Will be overridden by mapping
            SensorType.Multi, // Will be overridden by mapping
            SensorStatus.Active // Will be overridden by mapping
        );
    }

    /// <summary>
    ///     Extracts sensor ID from JSON content.
    /// </summary>
    /// <param name="jsonContent">The JSON content to parse.</param>
    /// <returns>The extracted sensor ID or a new GUID.</returns>
    private static Guid ExtractSensorId(string jsonContent)
    {
        try
        {
            using var document = JsonDocument.Parse(jsonContent);

            if (document.RootElement.TryGetProperty("sensorId", out var sensorIdElement))
                return sensorIdElement.GetGuid();
        }
        catch (JsonException)
        {
            // Invalid JSON format
        }
        catch (Exception)
        {
            // Other parsing errors
        }

        return Guid.NewGuid(); // Fallback to new GUID
    }

    /// <summary>
    ///     Extracts timestamp from JSON content.
    /// </summary>
    /// <param name="jsonContent">The JSON content to parse.</param>
    /// <returns>The extracted timestamp or current time.</returns>
    private static DateTime ExtractTimestamp(string jsonContent)
    {
        try
        {
            using var document = JsonDocument.Parse(jsonContent);

            if (document.RootElement.TryGetProperty("timestamp", out var timestampElement))
                return timestampElement.GetDateTime();
        }
        catch (JsonException)
        {
            // Invalid JSON format
        }
        catch (Exception)
        {
            // Other parsing errors
        }

        return DateTime.UtcNow; // Fallback to current time
    }

    /// <summary>
    ///     Extracts a specific reading value from the nested readings object.
    /// </summary>
    /// <param name="jsonContent">The JSON content to parse.</param>
    /// <param name="readingType">The type of reading to extract (temperature, humidity, pressure).</param>
    /// <returns>The extracted reading value or 0.0 as fallback.</returns>
    private static double ExtractReading(string jsonContent, string readingType)
    {
        try
        {
            using var document = JsonDocument.Parse(jsonContent);

            if (document.RootElement.TryGetProperty("readings", out var readingsElement) &&
                readingsElement.TryGetProperty(readingType, out var readingElement))
                return readingElement.GetDouble();
        }
        catch (JsonException)
        {
            // Invalid JSON format or missing properties
        }
        catch (Exception)
        {
            // Other parsing errors
        }

        return 0.0; // Fallback value
    }

    /// <summary>
    ///     Determines sensor type based on JSON content and source information.
    /// </summary>
    /// <param name="jsonContent">The JSON content to analyze.</param>
    /// <param name="source">The source system identifier.</param>
    /// <returns>The determined sensor type.</returns>
    private static SensorType DetermineSensorType(string jsonContent, string source)
    {
        // Try to determine from metadata if available
        try
        {
            using var document = JsonDocument.Parse(jsonContent);

            if (document.RootElement.TryGetProperty("metadata", out var metadataElement) &&
                metadataElement.TryGetProperty("tags", out var tagsElement))
            {
                var tags = new string[tagsElement.GetArrayLength()];

                for (var i = 0; i < tagsElement.GetArrayLength(); i++)
                {
                    tags[i] = tagsElement[i].GetString() ?? string.Empty;
                }

                // Determine sensor type based on tags
                if (Array.Exists(tags, tag => tag.Contains("temperature")))
                    return SensorType.Temperature;

                if (Array.Exists(tags, tag => tag.Contains("humidity")))
                    return SensorType.Humidity;

                if (Array.Exists(tags, tag => tag.Contains("pressure")))
                    return SensorType.Pressure;

                if (Array.Exists(tags, tag => tag.Contains("environmental")))
                    return SensorType.Environmental;

                if (Array.Exists(tags, tag => tag.Contains("industrial")))
                    return SensorType.Industrial;
            }
        }
        catch (JsonException)
        {
            // Invalid JSON format
        }

        // Fallback based on source
        return source switch
        {
            var s when s.Contains("API") => SensorType.Multi,
            var s when s.Contains("MQTT") => SensorType.Environmental,
            var s when s.Contains("WebSocket") => SensorType.Multi,
            var s when s.Contains("Database") => SensorType.Multi,
            var s when s.Contains("IoT") => SensorType.Environmental,
            _ => SensorType.Multi,
        };
    }

    /// <summary>
    ///     Determines sensor status based on quality information in JSON.
    /// </summary>
    /// <param name="jsonContent">The JSON content to analyze.</param>
    /// <returns>The determined sensor status.</returns>
    private static SensorStatus DetermineSensorStatus(string jsonContent)
    {
        try
        {
            using var document = JsonDocument.Parse(jsonContent);

            // Check quality section
            if (document.RootElement.TryGetProperty("quality", out var qualityElement))
            {
                if (qualityElement.TryGetProperty("isValid", out var isValidElement) &&
                    !isValidElement.GetBoolean())
                    return SensorStatus.Error;

                if (qualityElement.TryGetProperty("calibrationDue", out var calibrationDueElement))
                {
                    var calibrationDue = calibrationDueElement.GetDateTime();

                    if (calibrationDue <= DateTime.UtcNow)
                        return SensorStatus.Calibration;
                }
            }

            // Check for alerts
            if (document.RootElement.TryGetProperty("alerts", out var alertsElement) &&
                alertsElement.GetArrayLength() > 0)
            {
                var firstAlert = alertsElement[0];

                if (firstAlert.TryGetProperty("severity", out var severityElement))
                {
                    var severity = severityElement.GetString();

                    return severity?.ToLowerInvariant() switch
                    {
                        "error" => SensorStatus.Error,
                        "warning" => SensorStatus.Maintenance,
                        _ => SensorStatus.Active,
                    };
                }
            }
        }
        catch (JsonException)
        {
            // Invalid JSON format
        }

        return SensorStatus.Active; // Default status
    }
}
