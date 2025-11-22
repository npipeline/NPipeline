using System;
using NPipeline.Attributes.Nodes;
using NPipeline.Nodes;

namespace Sample_14_TimeWindowedJoinNode.Nodes;

/// <summary>
///     Time-windowed join node that correlates sensor readings with maintenance events.
///     This node demonstrates how to join two data streams based on both key matching (DeviceId)
///     and time window constraints to find sensor readings that occurred within a specified
///     time range around maintenance events.
/// </summary>
[KeySelector(typeof(SensorReading), "DeviceId")]
[KeySelector(typeof(MaintenanceEvent), "DeviceId")]
public class SensorMaintenanceJoinNode : KeyedJoinNode<string, SensorReading, MaintenanceEvent, SensorMaintenanceJoin>
{
    private readonly TimeSpan _windowSize;

    public SensorMaintenanceJoinNode()
    {
        _windowSize = TimeSpan.FromMinutes(5); // Default 5-minute window
    }

    public SensorMaintenanceJoinNode(string name, TimeSpan windowSize)
    {
        _windowSize = windowSize;
    }

    /// <summary>
    ///     Creates output item by combining sensor reading with maintenance event.
    /// </summary>
    /// <param name="sensorReading">The sensor reading from the first input stream.</param>
    /// <param name="maintenanceEvent">The maintenance event from the second input stream.</param>
    /// <returns>A combined SensorMaintenanceJoin record.</returns>
    public override SensorMaintenanceJoin CreateOutput(SensorReading sensorReading, MaintenanceEvent maintenanceEvent)
    {
        var timeDifference = sensorReading.Timestamp - maintenanceEvent.Timestamp;
        var windowStart = maintenanceEvent.Timestamp - _windowSize;
        var windowEnd = maintenanceEvent.Timestamp + _windowSize;

        return new SensorMaintenanceJoin(
            sensorReading,
            maintenanceEvent,
            timeDifference,
            windowStart,
            windowEnd
        );
    }

    /// <summary>
    ///     Creates output for unmatched sensor readings (left outer join).
    /// </summary>
    /// <param name="sensorReading">The unmatched sensor reading.</param>
    /// <returns>A SensorMaintenanceJoin with null maintenance event.</returns>
    public override SensorMaintenanceJoin CreateOutputFromLeft(SensorReading sensorReading)
    {
        Console.WriteLine(
            $"⚠ Unmatched Sensor Reading - Device: {sensorReading.DeviceId}, Temp: {sensorReading.Temperature}°C, Humidity: {sensorReading.Humidity}%, Time: {sensorReading.Timestamp:yyyy-MM-dd HH:mm:ss}");

        // Create a placeholder maintenance event for unmatched sensor readings
        var placeholderMaintenanceEvent = new MaintenanceEvent(
            "NO-MAINTENANCE",
            sensorReading.DeviceId,
            DateTime.MinValue,
            "No Maintenance",
            "Unknown",
            "No maintenance event found for this sensor reading"
        );

        return new SensorMaintenanceJoin(
            sensorReading,
            placeholderMaintenanceEvent,
            TimeSpan.Zero,
            DateTime.MinValue,
            DateTime.MaxValue
        );
    }

    /// <summary>
    ///     Creates output for unmatched maintenance events (right outer join).
    /// </summary>
    /// <param name="maintenanceEvent">The unmatched maintenance event.</param>
    /// <returns>A SensorMaintenanceJoin with null sensor reading.</returns>
    public override SensorMaintenanceJoin CreateOutputFromRight(MaintenanceEvent maintenanceEvent)
    {
        Console.WriteLine(
            $"ℹ Unmatched Maintenance Event - Device: {maintenanceEvent.DeviceId}, Type: {maintenanceEvent.MaintenanceType}, Time: {maintenanceEvent.Timestamp:yyyy-MM-dd HH:mm:ss}");

        // Create a placeholder sensor reading for unmatched maintenance events
        var placeholderSensorReading = new SensorReading(
            "NO-SENSOR",
            maintenanceEvent.DeviceId,
            DateTime.MinValue,
            0.0,
            0.0,
            "Unknown"
        );

        return new SensorMaintenanceJoin(
            placeholderSensorReading,
            maintenanceEvent,
            TimeSpan.Zero,
            DateTime.MinValue,
            DateTime.MaxValue
        );
    }
}
