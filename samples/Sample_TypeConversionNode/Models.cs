using System;
using System.Collections.Generic;

namespace Sample_TypeConversionNode;

/// <summary>
///     Raw string data received from external systems (e.g., CSV, log files, legacy APIs).
///     Represents unstructured input that needs parsing and validation.
/// </summary>
public sealed record RawStringData(
    string Id,
    string Timestamp,
    string Temperature,
    string Humidity,
    string Pressure,
    string SensorType,
    string Status
);

/// <summary>
///     JSON string data representing structured information from APIs or message queues.
///     Contains nested data that requires deserialization and transformation.
/// </summary>
public sealed record JsonStringData(
    string JsonContent,
    string Source,
    string ReceivedAt
);

/// <summary>
///     Parsed sensor data with strongly-typed properties.
///     Represents the result of parsing raw string data into proper types.
/// </summary>
public sealed record SensorData(
    Guid Id,
    DateTime Timestamp,
    double Temperature,
    double Humidity,
    double Pressure,
    SensorType SensorType,
    SensorStatus Status
);

/// <summary>
///     Domain object representing enriched sensor information with business logic.
///     Contains additional computed fields and validation status.
/// </summary>
public sealed record SensorReading(
    Guid Id,
    DateTime Timestamp,
    double Temperature,
    double Humidity,
    double Pressure,
    SensorType SensorType,
    SensorStatus Status,
    bool IsValid,
    string ValidationMessage,
    double TemperatureFahrenheit,
    string Location,
    DateTime ProcessedAt
);

/// <summary>
///     Data Transfer Object (DTO) for API responses.
///     Optimized for serialization with specific naming conventions.
/// </summary>
public sealed record SensorDto(
    string sensor_id,
    string timestamp,
    string temperature_celsius,
    string temperature_fahrenheit,
    string humidity_percent,
    string pressure_hpa,
    string sensor_type,
    string status,
    string location,
    bool is_valid
);

/// <summary>
///     Aggregated sensor statistics for reporting.
///     Represents processed and aggregated data from multiple readings.
/// </summary>
public sealed record SensorStatistics(
    DateTime WindowStart,
    DateTime WindowEnd,
    SensorType SensorType,
    int ReadingCount,
    double AverageTemperature,
    double MinTemperature,
    double MaxTemperature,
    double AverageHumidity,
    double AveragePressure,
    int ValidReadings,
    int InvalidReadings
);

/// <summary>
///     Legacy data format from old systems requiring transformation.
///     Represents data from legacy integration scenarios.
/// </summary>
public sealed record LegacySensorFormat(
    string SENSOR_ID,
    string READING_TIME,
    string TEMP_VAL,
    string HUMIDITY_VAL,
    string PRESS_VAL,
    string SENSOR_CATEGORY,
    string OPERATIONAL_STATE
);

/// <summary>
///     Final output format for downstream processing.
///     Represents the canonical data format for the system.
/// </summary>
public sealed record CanonicalSensorData(
    Guid SensorId,
    DateTimeOffset ReadingTimestamp,
    double TemperatureCelsius,
    double TemperatureFahrenheit,
    double RelativeHumidity,
    double AtmosphericPressure,
    SensorCategory Category,
    OperationalState State,
    DataQuality Quality,
    GeoLocation Location,
    Dictionary<string, object> Metadata
);

/// <summary>
///     Enumeration of sensor types with proper typing.
/// </summary>
public enum SensorType
{
    Temperature,
    Humidity,
    Pressure,
    Multi,
    Environmental,
    Industrial,
}

/// <summary>
///     Enumeration of sensor status values.
/// </summary>
public enum SensorStatus
{
    Active,
    Inactive,
    Maintenance,
    Error,
    Calibration,
}

/// <summary>
///     Enumeration of sensor categories for legacy format conversion.
/// </summary>
public enum SensorCategory
{
    Climate,
    Industrial,
    Environmental,
    Quality,
    Safety,
}

/// <summary>
///     Enumeration of operational states for legacy format conversion.
/// </summary>
public enum OperationalState
{
    Online,
    Offline,
    Maintenance,
    Fault,
    Calibration,
}

/// <summary>
///     Enumeration of data quality indicators.
/// </summary>
public enum DataQuality
{
    High,
    Medium,
    Low,
    Unknown,
}

/// <summary>
///     Geographic location information.
/// </summary>
public sealed record GeoLocation(
    double Latitude,
    double Longitude,
    double? Altitude,
    string? Name
);

/// <summary>
///     Error information for conversion failures.
/// </summary>
public sealed record ConversionError(
    string SourceId,
    string ErrorMessage,
    Exception? Exception,
    DateTime ErrorTime,
    string SourceType
);

/// <summary>
///     Conversion result with success/failure information.
/// </summary>
public sealed record ConversionResult<T>(
    bool IsSuccess,
    T? Result,
    ConversionError? Error,
    DateTime ProcessedAt
);
