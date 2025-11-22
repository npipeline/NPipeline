using System;
using System.Collections.Generic;
using System.Globalization;
using NPipeline.Nodes;

namespace Sample_TypeConversionNode.Nodes;

/// <summary>
///     Type conversion node that transforms LegacySensorFormat to CanonicalSensorData.
///     This node demonstrates legacy system integration with naming convention changes and business logic.
/// </summary>
/// <remarks>
///     This converter showcases enterprise integration patterns:
///     - Naming convention transformation (UPPER_CASE to PascalCase)
///     - Legacy format to modern canonical format conversion
///     - Business logic for data quality assessment
///     - Complex object creation with nested structures
///     - Unit conversion and normalization
///     - Metadata enrichment during transformation
///     This pattern is essential for modernizing legacy systems and enterprise integration.
/// </remarks>
public sealed class LegacyToCanonicalConverter : TypeConversionNode<LegacySensorFormat, CanonicalSensorData>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="LegacyToCanonicalConverter" /> class.
    /// </summary>
    public LegacyToCanonicalConverter() : base(CreateCanonicalSensorData)
    {
        // No AutoMap() due to completely different naming conventions
        // All mappings are custom to handle legacy format transformation

        Map(
            dst => dst.SensorId,
            input => ParseLegacySensorId(input.SENSOR_ID)
        );

        Map(
            dst => dst.ReadingTimestamp,
            input => ParseLegacyTimestamp(input.READING_TIME)
        );

        Map(
            dst => dst.TemperatureCelsius,
            input => NormalizeTemperature(input.TEMP_VAL)
        );

        Map(
            dst => dst.TemperatureFahrenheit,
            input => ConvertToFahrenheit(NormalizeTemperature(input.TEMP_VAL))
        );

        Map(
            dst => dst.RelativeHumidity,
            input => ParseDouble(input.HUMIDITY_VAL, 0.0)
        );

        Map(
            dst => dst.AtmosphericPressure,
            input => ParseDouble(input.PRESS_VAL, 1013.25)
        );

        Map(
            dst => dst.Category,
            input => ParseEnum(input.SENSOR_CATEGORY, SensorCategory.Environmental)
        );

        Map(
            dst => dst.State,
            input => ParseEnum(input.OPERATIONAL_STATE, OperationalState.Offline)
        );

        Map(
            dst => dst.Quality,
            input => AssessDataQuality(input)
        );

        Map(
            dst => dst.Location,
            input => GenerateLocation(input.SENSOR_CATEGORY)
        );

        Map(
            dst => dst.Metadata,
            input => EnrichMetadata(input)
        );
    }

    /// <summary>
    ///     Custom factory function for creating CanonicalSensorData instances.
    /// </summary>
    /// <param name="input">The legacy sensor format input.</param>
    /// <returns>A new CanonicalSensorData instance with default values.</returns>
    private static CanonicalSensorData CreateCanonicalSensorData(LegacySensorFormat input)
    {
        return new CanonicalSensorData(
            Guid.NewGuid(), // Will be overridden by mapping
            DateTimeOffset.UtcNow, // Will be overridden by mapping
            0.0, // Will be overridden by mapping
            0.0, // Will be overridden by mapping
            0.0, // Will be overridden by mapping
            0.0, // Will be overridden by mapping
            SensorCategory.Environmental, // Will be overridden by mapping
            OperationalState.Offline, // Will be overridden by mapping
            DataQuality.Unknown, // Will be overridden by mapping
            new GeoLocation(0.0, 0.0, null, null), // Will be overridden by mapping
            new Dictionary<string, object>() // Will be overridden by mapping
        );
    }

    /// <summary>
    ///     Parses legacy sensor ID and converts to GUID.
    /// </summary>
    /// <param name="legacyId">The legacy sensor ID string.</param>
    /// <returns>A parsed GUID or new GUID if parsing fails.</returns>
    private static Guid ParseLegacySensorId(string legacyId)
    {
        if (string.IsNullOrWhiteSpace(legacyId))
            return Guid.NewGuid();

        // Try to extract numeric part and create deterministic GUID
        var parts = legacyId.Split('-');

        if (parts.Length >= 2 && int.TryParse(parts[^1], out var numericPart))
        {
            // Create deterministic GUID based on numeric part
            var bytes = BitConverter.GetBytes(numericPart);
            var guidBytes = new byte[16];
            Array.Copy(bytes, 0, guidBytes, 0, Math.Min(bytes.Length, 16));
            return new Guid(guidBytes);
        }

        return Guid.NewGuid();
    }

    /// <summary>
    ///     Parses legacy timestamp format and converts to DateTimeOffset.
    /// </summary>
    /// <param name="legacyTimestamp">The legacy timestamp string.</param>
    /// <returns>A parsed DateTimeOffset or current time as fallback.</returns>
    private static DateTimeOffset ParseLegacyTimestamp(string legacyTimestamp)
    {
        if (string.IsNullOrWhiteSpace(legacyTimestamp))
            return DateTimeOffset.UtcNow;

        // Legacy format: yyyyMMddHHmmss
        if (DateTime.TryParseExact(legacyTimestamp, "yyyyMMddHHmmss",
                CultureInfo.InvariantCulture, DateTimeStyles.None, out var dateTime))
            return new DateTimeOffset(dateTime, TimeSpan.Zero);

        // Try other common legacy formats
        var formats = new[]
        {
            "yyyy-MM-dd HH:mm:ss",
            "dd/MM/yyyy HH:mm:ss",
            "MM/dd/yyyy HH:mm:ss",
        };

        foreach (var format in formats)
        {
            if (DateTime.TryParseExact(legacyTimestamp, format,
                    CultureInfo.InvariantCulture, DateTimeStyles.None, out var altDateTime))
                return new DateTimeOffset(altDateTime, TimeSpan.Zero);
        }

        return DateTimeOffset.UtcNow;
    }

    /// <summary>
    ///     Normalizes temperature values from legacy format.
    /// </summary>
    /// <param name="legacyTemp">The legacy temperature value.</param>
    /// <returns>Normalized temperature in Celsius.</returns>
    private static double NormalizeTemperature(string legacyTemp)
    {
        var temp = ParseDouble(legacyTemp, 20.0);

        // If temperature seems to be in Fahrenheit (>50), convert to Celsius
        if (temp > 50.0)
            return (temp - 32.0) * 5.0 / 9.0;

        return temp;
    }

    /// <summary>
    ///     Converts Celsius to Fahrenheit.
    /// </summary>
    /// <param name="celsius">Temperature in Celsius.</param>
    /// <returns>Temperature in Fahrenheit.</returns>
    private static double ConvertToFahrenheit(double celsius)
    {
        return celsius * 9.0 / 5.0 + 32.0;
    }

    /// <summary>
    ///     Parses double values with fallback.
    /// </summary>
    /// <param name="value">The string value to parse.</param>
    /// <param name="fallback">The fallback value if parsing fails.</param>
    /// <returns>The parsed double value or fallback.</returns>
    private static double ParseDouble(string value, double fallback)
    {
        if (double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var result))
            return result;

        return fallback;
    }

    /// <summary>
    ///     Parses enum values with case-insensitive matching.
    /// </summary>
    /// <typeparam name="T">The enum type.</typeparam>
    /// <param name="value">The string value to parse.</param>
    /// <param name="fallback">The fallback value if parsing fails.</param>
    /// <returns>The parsed enum value or fallback.</returns>
    private static T ParseEnum<T>(string value, T fallback) where T : struct, Enum
    {
        if (Enum.TryParse<T>(value, true, out var result))
            return result;

        return fallback;
    }

    /// <summary>
    ///     Assesses data quality based on legacy data characteristics.
    /// </summary>
    /// <param name="input">The legacy sensor data.</param>
    /// <returns>The assessed data quality.</returns>
    private static DataQuality AssessDataQuality(LegacySensorFormat input)
    {
        var qualityScore = 0;

        // Check for valid temperature range
        var temp = ParseDouble(input.TEMP_VAL, 0.0);

        if (temp is >= -50.0 and <= 100.0)
            qualityScore += 25;

        // Check for valid humidity range
        var humidity = ParseDouble(input.HUMIDITY_VAL, 0.0);

        if (humidity is >= 0.0 and <= 100.0)
            qualityScore += 25;

        // Check for valid pressure range
        var pressure = ParseDouble(input.PRESS_VAL, 0.0);

        if (pressure is >= 800.0 and <= 1200.0)
            qualityScore += 25;

        // Check operational state
        if (input.OPERATIONAL_STATE == "ONLINE")
            qualityScore += 25;

        return qualityScore switch
        {
            >= 75 => DataQuality.High,
            >= 50 => DataQuality.Medium,
            >= 25 => DataQuality.Low,
            _ => DataQuality.Unknown,
        };
    }

    /// <summary>
    ///     Generates location information based on sensor category.
    /// </summary>
    /// <param name="category">The sensor category.</param>
    /// <returns>A GeoLocation based on the category.</returns>
    private static GeoLocation GenerateLocation(string category)
    {
        return category switch
        {
            "CLIMATE" => new GeoLocation(40.7128, -74.0060, 10.0, "Climate Control Room"),
            "INDUSTRIAL" => new GeoLocation(41.8781, -87.6298, 15.0, "Factory Floor A"),
            "ENVIRONMENTAL" => new GeoLocation(37.7749, -122.4194, 5.0, "Environmental Station"),
            "QUALITY" => new GeoLocation(42.3601, -71.0589, 20.0, "Quality Assurance Lab"),
            "SAFETY" => new GeoLocation(39.9526, -75.1652, 8.0, "Safety Monitoring Point"),
            _ => new GeoLocation(0.0, 0.0, null, "Unknown Location"),
        };
    }

    /// <summary>
    ///     Enriches metadata with additional information from legacy data.
    /// </summary>
    /// <param name="input">The legacy sensor data.</param>
    /// <returns>A dictionary with enriched metadata.</returns>
    private static Dictionary<string, object> EnrichMetadata(LegacySensorFormat input)
    {
        var metadata = new Dictionary<string, object>
        {
            ["legacySensorId"] = input.SENSOR_ID,
            ["legacyCategory"] = input.SENSOR_CATEGORY,
            ["legacyState"] = input.OPERATIONAL_STATE,
            ["legacyTimestamp"] = input.READING_TIME,
            ["conversionTimestamp"] = DateTime.UtcNow,
            ["conversionVersion"] = "1.0",
            ["sourceSystem"] = "LegacyIntegration",
        };

        // Add data quality indicators
        var temp = ParseDouble(input.TEMP_VAL, 0.0);
        var humidity = ParseDouble(input.HUMIDITY_VAL, 0.0);
        var pressure = ParseDouble(input.PRESS_VAL, 0.0);

        metadata["temperatureValidation"] = temp is >= -50.0 and <= 100.0;
        metadata["humidityValidation"] = humidity is >= 0.0 and <= 100.0;
        metadata["pressureValidation"] = pressure is >= 800.0 and <= 1200.0;
        metadata["isOnline"] = input.OPERATIONAL_STATE == "ONLINE";

        return metadata;
    }
}
