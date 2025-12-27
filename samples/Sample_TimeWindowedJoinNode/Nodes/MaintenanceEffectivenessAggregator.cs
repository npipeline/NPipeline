using System;
using System.Linq;
using Microsoft.Extensions.Logging;
using NPipeline.Configuration;
using NPipeline.DataFlow.Windowing;
using NPipeline.Nodes;

namespace Sample_TimeWindowedJoinNode.Nodes;

public class MaintenanceEffectivenessAggregator : AdvancedAggregateNode<EnrichedSensorData, string, MaintenanceEffectivenessReport,
    MaintenanceEffectivenessReport>
{
    private static readonly Action<ILogger, string, int, double, int, Exception?> _effectivenessReport =
        LoggerMessage.Define<string, int, double, int>(LogLevel.Information, new EventId(1, "EffectivenessReport"),
            "Maintenance Effectiveness Report for Device {DeviceId}: {MaintenanceCount} maintenance events, {AverageTempImprovement:F2}°C average improvement, {TotalSensorReadings} sensor readings analyzed");

    private readonly ILogger<MaintenanceEffectivenessAggregator>? _logger;

    public MaintenanceEffectivenessAggregator(ILogger<MaintenanceEffectivenessAggregator> logger)
        : base(new AggregateNodeConfiguration<EnrichedSensorData>(
            WindowAssigner.Tumbling(TimeSpan.FromMinutes(5))))
    {
        _logger = logger;
    }

    public override string GetKey(EnrichedSensorData item)
    {
        return item.JoinData.SensorReading.DeviceId;
    }

    public override MaintenanceEffectivenessReport CreateAccumulator()
    {
        return new MaintenanceEffectivenessReport(
            string.Empty,
            0,
            0.0,
            0,
            Array.Empty<string>(),
            DateTime.MinValue
        );
    }

    public override MaintenanceEffectivenessReport Accumulate(MaintenanceEffectivenessReport accumulator, EnrichedSensorData item)
    {
        var deviceId = accumulator.DeviceId == string.Empty
            ? item.JoinData.SensorReading.DeviceId
            : accumulator.DeviceId;

        var maintenanceCount = accumulator.MaintenanceCount;
        var averageTempImprovement = accumulator.AverageTempImprovement;
        var totalSensorReadings = accumulator.TotalSensorReadings;
        var maintenanceTypes = accumulator.MaintenanceTypes.ToList();

        var lastMaintenanceDate = accumulator.LastMaintenanceDate == DateTime.MinValue
            ? item.JoinData.MaintenanceEvent.Timestamp
            : accumulator.LastMaintenanceDate;

        totalSensorReadings++;

        // Add maintenance type if not already present
        if (!maintenanceTypes.Contains(item.JoinData.MaintenanceEvent.MaintenanceType))
            maintenanceTypes.Add(item.JoinData.MaintenanceEvent.MaintenanceType);

        // Calculate temperature improvement if we have both pre and post maintenance temperatures
        if (item.PreMaintenanceTemp.HasValue && item.PostMaintenanceTemp.HasValue)
        {
            var tempImprovement = item.PostMaintenanceTemp.Value - item.PreMaintenanceTemp.Value;
            averageTempImprovement = (averageTempImprovement * maintenanceCount + tempImprovement) / (maintenanceCount + 1);
            maintenanceCount++;
        }

        return new MaintenanceEffectivenessReport(
            deviceId,
            maintenanceCount,
            averageTempImprovement,
            totalSensorReadings,
            maintenanceTypes.ToArray(),
            lastMaintenanceDate
        );
    }

    public override MaintenanceEffectivenessReport GetResult(MaintenanceEffectivenessReport accumulator)
    {
        if (_logger != null)
        {
            _effectivenessReport(_logger, accumulator.DeviceId, accumulator.MaintenanceCount, accumulator.AverageTempImprovement,
                accumulator.TotalSensorReadings, null);
        }

        return accumulator;
    }
}
