using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_14_TimeWindowedJoinNode.Nodes;

/// <summary>
///     Generic sink node that outputs results to the console.
///     This sink can handle multiple types of outputs for demonstration purposes.
/// </summary>
public class ConsoleSink<T> : SinkNode<T>
{
    private static readonly Action<ILogger, Exception?> _startingOutput =
        LoggerMessage.Define(LogLevel.Information, new EventId(1, "StartingOutput"), "ConsoleSink: Starting to output results to console");

    private static readonly Action<ILogger, Exception?> _completedOutput =
        LoggerMessage.Define(LogLevel.Information, new EventId(2, "CompletedOutput"), "ConsoleSink: Completed outputting results");

    private readonly ILogger<ConsoleSink<T>>? _logger;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ConsoleSink{T}" /> class.
    /// </summary>
    /// <param name="logger">The logger for diagnostic information.</param>
    public ConsoleSink(ILogger<ConsoleSink<T>>? logger = null)
    {
        _logger = logger;
    }

    /// <inheritdoc />
    public override async Task ExecuteAsync(IDataPipe<T> input, PipelineContext context, CancellationToken cancellationToken)
    {
        if (_logger != null)
            _startingOutput(_logger, null);

        await foreach (var item in input.WithCancellation(cancellationToken))
        {
            await OutputItem(item);
        }

        if (_logger != null)
            _completedOutput(_logger, null);
    }

    private Task OutputItem(T item)
    {
        return item switch
        {
            SensorMaintenanceJoin join => OutputJoinResult(join),
            EnrichedSensorData enriched => OutputEnrichedData(enriched),
            MaintenanceEffectivenessReport report => OutputEffectivenessReport(report),
            TimeWindowedAnalysis analysis => OutputTimeWindowedAnalysis(analysis),
            _ => Task.Run(() => Console.WriteLine($"Unknown item type: {item}")),
        };
    }

    private Task OutputJoinResult(SensorMaintenanceJoin join)
    {
        Console.WriteLine($"[JOIN] Device: {join.SensorReading.DeviceId}, Sensor: {join.SensorReading.SensorId}, " +
                          $"Maintenance: {join.MaintenanceEvent.MaintenanceType}, Time Diff: {join.TimeDifference.TotalMinutes:F1}min");

        return Task.CompletedTask;
    }

    private Task OutputEnrichedData(EnrichedSensorData enriched)
    {
        Console.WriteLine($"[ENRICHED] Device: {enriched.JoinData.SensorReading.DeviceId}, " +
                          $"Temp: {enriched.JoinData.SensorReading.Temperature:F1}°C, " +
                          $"Humidity: {enriched.JoinData.SensorReading.Humidity:F1}%, " +
                          $"Temp Change: {enriched.TempChange:F1}°C, " +
                          $"Impact: {enriched.MaintenanceImpact}");

        return Task.CompletedTask;
    }

    private Task OutputEffectivenessReport(MaintenanceEffectivenessReport report)
    {
        var maintenanceTypes = string.Join(", ", report.MaintenanceTypes);

        Console.WriteLine($"[REPORT] Device: {report.DeviceId}, " +
                          $"Maintenance Count: {report.MaintenanceCount}, " +
                          $"Avg Temp Improvement: {report.AverageTempImprovement:F2}°C, " +
                          $"Total Readings: {report.TotalSensorReadings}, " +
                          $"Types: [{maintenanceTypes}], " +
                          $"Last Maintenance: {report.LastMaintenanceDate:yyyy-MM-dd HH:mm}");

        return Task.CompletedTask;
    }

    private Task OutputTimeWindowedAnalysis(TimeWindowedAnalysis analysis)
    {
        Console.WriteLine($"[ANALYSIS] Window: {analysis.WindowStart:HH:mm:ss} - {analysis.WindowEnd:HH:mm:ss}, " +
                          $"Joins: {analysis.JoinCount}, " +
                          $"Avg Temp Change: {analysis.AverageTempChange:F2}°C, " +
                          $"Common Maintenance: {analysis.MostCommonMaintenanceType}, " +
                          $"Affected Devices: {analysis.AffectedDevices}");

        return Task.CompletedTask;
    }
}
