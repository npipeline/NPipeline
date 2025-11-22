using System;

namespace Sample_12_BatchingNode;

/// <summary>
///     Represents an individual sensor reading from an IoT device.
///     This model demonstrates individual items that benefit from batched processing.
/// </summary>
/// <param name="DeviceId">The unique identifier of the sensor device.</param>
/// <param name="Timestamp">When the reading was taken.</param>
/// <param name="Temperature">Temperature reading in Celsius.</param>
/// <param name="Humidity">Humidity reading as a percentage.</param>
/// <param name="Pressure">Atmospheric pressure reading in hPa.</param>
/// <param name="BatteryLevel">Battery level as a percentage.</param>
public record SensorReading(
    string DeviceId,
    DateTime Timestamp,
    double Temperature,
    double Humidity,
    double Pressure,
    double BatteryLevel);

/// <summary>
///     Represents the result of batch processing sensor readings.
///     This model shows aggregated data that results from batch processing.
/// </summary>
/// <param name="BatchId">Unique identifier for this batch.</param>
/// <param name="BatchTimestamp">When this batch was processed.</param>
/// <param name="DeviceId">The device ID for these readings.</param>
/// <param name="ReadingCount">Number of readings in this batch.</param>
/// <param name="AverageTemperature">Average temperature across all readings.</param>
/// <param name="AverageHumidity">Average humidity across all readings.</param>
/// <param name="AveragePressure">Average pressure across all readings.</param>
/// <param name="MinBatteryLevel">Minimum battery level in the batch.</param>
/// <param name="TemperatureRange">Temperature range (max - min) in the batch.</param>
/// <param name="ProcessingTimeMs">Time taken to process this batch in milliseconds.</param>
public record BatchProcessingResult(
    string BatchId,
    DateTime BatchTimestamp,
    string DeviceId,
    int ReadingCount,
    double AverageTemperature,
    double AverageHumidity,
    double AveragePressure,
    double MinBatteryLevel,
    double TemperatureRange,
    long ProcessingTimeMs);

/// <summary>
///     Represents a database insert result for a batch of sensor readings.
///     This model demonstrates the result of bulk database operations.
/// </summary>
/// <param name="BatchId">The batch ID that was inserted.</param>
/// <param name="RecordsInserted">Number of records successfully inserted.</param>
/// <param name="InsertTimeMs">Time taken for the database insert operation.</param>
/// <param name="Success">Whether the insert operation was successful.</param>
/// <param name="ErrorMessage">Error message if the operation failed.</param>
public record DatabaseInsertResult(
    string BatchId,
    int RecordsInserted,
    long InsertTimeMs,
    bool Success,
    string? ErrorMessage = null);
