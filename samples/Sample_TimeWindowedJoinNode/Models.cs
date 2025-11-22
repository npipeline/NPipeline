using System;
using NPipeline.DataFlow;

namespace Sample_TimeWindowedJoinNode;

/// <summary>
///     Data models for time-windowed join sample demonstrating temporal data correlation.
/// </summary>
public record SensorReading(
    string SensorId,
    string DeviceId,
    DateTime Timestamp,
    double Temperature,
    double Humidity,
    string Location
);

public record MaintenanceEvent(
    string MaintenanceId,
    string DeviceId,
    DateTime Timestamp,
    string MaintenanceType,
    string TechnicianId,
    string Description
);

public record SensorMaintenanceJoin(
    SensorReading SensorReading,
    MaintenanceEvent MaintenanceEvent,
    TimeSpan TimeDifference,
    DateTime WindowStart,
    DateTime WindowEnd
);

/// <summary>
///     Represents enriched sensor data with maintenance impact analysis.
/// </summary>
public record EnrichedSensorData(
    SensorMaintenanceJoin JoinData,
    double? PreMaintenanceTemp,
    double? PostMaintenanceTemp,
    double? TempChange,
    string MaintenanceImpact
) : ITimestamped
{
    /// <summary>
    ///     Gets the timestamp of the enriched sensor data.
    /// </summary>
    public DateTimeOffset Timestamp => new(JoinData.SensorReading.Timestamp);
}

public record MaintenanceEffectivenessReport(
    string DeviceId,
    int MaintenanceCount,
    double AverageTempImprovement,
    int TotalSensorReadings,
    string[] MaintenanceTypes,
    DateTime LastMaintenanceDate
);

public record TimeWindowedAnalysis(
    DateTime WindowStart,
    DateTime WindowEnd,
    int JoinCount,
    double AverageTempChange,
    string MostCommonMaintenanceType,
    int AffectedDevices
);

public enum WindowType
{
    Tumbling,
    Sliding,
    Session,
}
