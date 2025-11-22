using System;
using System.Globalization;
using NPipeline.Nodes;

namespace Sample_16_TypeConversionNode.Nodes;

/// <summary>
///     Type conversion node that transforms RawStringData to SensorData.
///     This node demonstrates string parsing with error handling and validation.
/// </summary>
/// <remarks>
///     This converter showcases several TypeConversionNode capabilities:
///     - String to primitive type conversion with error handling
///     - String to enum conversion
///     - Custom validation logic during conversion
///     - Using the whole input object for complex transformations
///     - Graceful handling of invalid data with fallback values
///     This is a common pattern when integrating with legacy systems or text-based data sources.
/// </remarks>
public sealed class StringToSensorDataConverter : TypeConversionNode<RawStringData, SensorData>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="StringToSensorDataConverter" /> class.
    /// </summary>
    public StringToSensorDataConverter() : base(CreateSensorData)
    {
        // Manual mappings for string parsing with validation
        // Note: Not using AutoMap() due to reflection issues in the current implementation

        Map(
            src => src.Timestamp,
            dst => dst.Timestamp,
            timestampStr => ParseTimestamp(timestampStr)
        );

        Map(
            src => src.Temperature,
            dst => dst.Temperature,
            tempStr => ParseDouble(tempStr, "Temperature", 0.0)
        );

        Map(
            src => src.Humidity,
            dst => dst.Humidity,
            humidityStr => ParseDouble(humidityStr, "Humidity", 50.0)
        );

        Map(
            src => src.Pressure,
            dst => dst.Pressure,
            pressureStr => ParseDouble(pressureStr, "Pressure", 1013.25)
        );

        Map(
            src => src.SensorType,
            dst => dst.SensorType,
            sensorTypeStr => ParseEnum(sensorTypeStr, SensorType.Multi)
        );

        Map(
            src => src.Status,
            dst => dst.Status,
            statusStr => ParseEnum(statusStr, SensorStatus.Active)
        );
    }

    /// <summary>
    ///     Custom factory function for creating SensorData instances.
    /// </summary>
    /// <param name="input">The raw string data input.</param>
    /// <returns>A new SensorData instance with default values.</returns>
    private static SensorData CreateSensorData(RawStringData input)
    {
        // Try to parse the ID, fallback to a new GUID if invalid
        var id = Guid.TryParse(input.Id, out var guidId)
            ? guidId
            : Guid.NewGuid();

        return new SensorData(
            id,
            DateTime.UtcNow, // Will be overridden by mapping
            0.0, // Will be overridden by mapping
            0.0, // Will be overridden by mapping
            0.0, // Will be overridden by mapping
            SensorType.Multi, // Will be overridden by mapping
            SensorStatus.Error // Will be overridden by mapping
        );
    }

    /// <summary>
    ///     Parses timestamp string with multiple format support.
    /// </summary>
    /// <param name="timestampStr">The timestamp string to parse.</param>
    /// <returns>The parsed DateTime or current time as fallback.</returns>
    private static DateTime ParseTimestamp(string timestampStr)
    {
        if (string.IsNullOrWhiteSpace(timestampStr))
            return DateTime.UtcNow;

        var formats = new[]
        {
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss.fff",
            "yyyy/MM/dd HH:mm:ss",
            "MM/dd/yyyy HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
        };

        foreach (var format in formats)
        {
            if (DateTime.TryParseExact(timestampStr, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out var result))
                return result;
        }

        // Try general parsing as last resort
        return DateTime.TryParse(timestampStr, CultureInfo.InvariantCulture, DateTimeStyles.None, out var generalResult)
            ? generalResult
            : DateTime.UtcNow;
    }

    /// <summary>
    ///     Parses double values with error handling and validation.
    /// </summary>
    /// <param name="valueStr">The string value to parse.</param>
    /// <param name="fieldName">The field name for error reporting.</param>
    /// <param name="fallback">The fallback value if parsing fails.</param>
    /// <returns>The parsed double value or fallback.</returns>
    private static double ParseDouble(string valueStr, string fieldName, double fallback)
    {
        if (string.IsNullOrWhiteSpace(valueStr))
            return fallback;

        if (double.TryParse(valueStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var result))
        {
            // Apply reasonable range validation
            return fieldName switch
            {
                "Temperature" => result is >= -50.0 and <= 100.0
                    ? result
                    : fallback,
                "Humidity" => result is >= 0.0 and <= 100.0
                    ? result
                    : fallback,
                "Pressure" => result is >= 800.0 and <= 1200.0
                    ? result
                    : fallback,
                _ => result,
            };
        }

        return fallback;
    }

    /// <summary>
    ///     Parses enum values with case-insensitive matching.
    /// </summary>
    /// <typeparam name="T">The enum type.</typeparam>
    /// <param name="valueStr">The string value to parse.</param>
    /// <param name="fallback">The fallback value if parsing fails.</param>
    /// <returns>The parsed enum value or fallback.</returns>
    private static T ParseEnum<T>(string valueStr, T fallback) where T : struct, Enum
    {
        if (string.IsNullOrWhiteSpace(valueStr))
            return fallback;

        return Enum.TryParse<T>(valueStr, true, out var result)
            ? result
            : fallback;
    }
}
