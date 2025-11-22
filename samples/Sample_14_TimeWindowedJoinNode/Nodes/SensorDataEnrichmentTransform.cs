using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_14_TimeWindowedJoinNode.Nodes;

public class SensorDataEnrichmentTransform : TransformNode<SensorMaintenanceJoin, EnrichedSensorData>
{
    private static readonly Action<ILogger, string, string, Exception?> _enrichingData =
        LoggerMessage.Define<string, string>(LogLevel.Debug, new EventId(1, "EnrichingData"),
            "Enriching sensor data for device {DeviceId} with maintenance type {MaintenanceType}");

    private static readonly Action<ILogger, string, string, double, string, string, Exception?> _enrichedData =
        LoggerMessage.Define<string, string, double, string, string>(LogLevel.Information, new EventId(2, "EnrichedData"),
            "Enriched sensor data - Device: {DeviceId}, Location: {Location}, Temp: {Temperature}°C, Maintenance: {MaintenanceType}, Impact: {Impact}");

    private readonly ILogger<SensorDataEnrichmentTransform>? _logger;

    public SensorDataEnrichmentTransform(ILogger<SensorDataEnrichmentTransform> logger)
    {
        _logger = logger;
    }

    public override Task<EnrichedSensorData> ExecuteAsync(SensorMaintenanceJoin item, PipelineContext context, CancellationToken cancellationToken)
    {
        if (_logger != null)
            _enrichingData(_logger, item.SensorReading.DeviceId, item.MaintenanceEvent.MaintenanceType, null);

        // Analyze the time difference to determine if this is pre or post maintenance
        var isPostMaintenance = item.TimeDifference.TotalSeconds > 0;

        // Calculate temperature change if we have reference data
        double? preMaintenanceTemp = null;
        double? postMaintenanceTemp = null;
        double? tempChange = null;

        if (isPostMaintenance)
        {
            postMaintenanceTemp = item.SensorReading.Temperature;

            // For demonstration, we'll simulate a pre-maintenance temperature
            preMaintenanceTemp = postMaintenanceTemp - 2.5; // Assume 2.5°C improvement
            tempChange = postMaintenanceTemp - preMaintenanceTemp.Value;
        }
        else
        {
            preMaintenanceTemp = item.SensorReading.Temperature;

            // For demonstration, we'll simulate a post-maintenance temperature
            postMaintenanceTemp = preMaintenanceTemp + 2.5; // Assume 2.5°C improvement
            tempChange = postMaintenanceTemp.Value - preMaintenanceTemp.Value;
        }

        // Determine maintenance impact based on temperature change
        var maintenanceImpact = tempChange.HasValue
            ? tempChange.Value > 0
                ? "Positive Impact"
                : tempChange.Value < 0
                    ? "Negative Impact"
                    : "No Impact"
            : "Unknown";

        if (_logger != null)
            _enrichedData(_logger, item.SensorReading.DeviceId, item.SensorReading.Location, item.SensorReading.Temperature,
                item.MaintenanceEvent.MaintenanceType, maintenanceImpact, null);

        return Task.FromResult(new EnrichedSensorData(
            item,
            preMaintenanceTemp,
            postMaintenanceTemp,
            tempChange,
            maintenanceImpact
        ));
    }
}
