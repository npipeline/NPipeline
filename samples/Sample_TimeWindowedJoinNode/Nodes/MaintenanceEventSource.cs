using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using NPipeline.DataFlow;
using NPipeline.DataFlow.DataPipes;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_TimeWindowedJoinNode.Nodes;

/// <summary>
///     Source node that generates maintenance events with timestamps for demonstrating time-windowed joins.
///     This node creates a stream of maintenance events that can be correlated with sensor readings.
/// </summary>
public class MaintenanceEventSource : SourceNode<MaintenanceEvent>
{
    private static readonly Action<ILogger, int, Exception?> _startingGeneration =
        LoggerMessage.Define<int>(LogLevel.Information, new EventId(1, "StartingGeneration"),
            "MaintenanceEventSource: Starting to generate {MaxEvents} maintenance events");

    private static readonly Action<ILogger, string, string, DateTime, Exception?> _generatedEvent =
        LoggerMessage.Define<string, string, DateTime>(LogLevel.Debug, new EventId(2, "GeneratedEvent"),
            "MaintenanceEventSource: Generated event {MaintenanceId} for device {DeviceId} at {Timestamp}");

    private static readonly Action<ILogger, int, Exception?> _finishedGeneration =
        LoggerMessage.Define<int>(LogLevel.Information, new EventId(3, "FinishedGeneration"),
            "MaintenanceEventSource: Finished generating {TotalEvents} events");

    private readonly TimeSpan _delayBetweenEvents;
    private readonly string[] _deviceIds;
    private readonly ILogger<MaintenanceEventSource>? _logger;
    private readonly int _maxEvents;

    /// <summary>
    ///     Initializes a new instance of the <see cref="MaintenanceEventSource" /> class.
    /// </summary>
    /// <param name="logger">The logger for diagnostic information.</param>
    /// <param name="delayBetweenEvents">Delay between generating events to simulate real-time data.</param>
    /// <param name="maxEvents">Maximum number of events to generate.</param>
    /// <param name="deviceIds">List of device IDs to generate events for.</param>
    public MaintenanceEventSource(
        ILogger<MaintenanceEventSource>? logger = null,
        TimeSpan? delayBetweenEvents = null,
        int maxEvents = 20,
        string[]? deviceIds = null)
    {
        _logger = logger;
        _delayBetweenEvents = delayBetweenEvents ?? TimeSpan.FromMilliseconds(800);
        _maxEvents = maxEvents;
        _deviceIds = deviceIds ?? new[] { "TEMP-001", "PRESS-001", "FLOW-001", "VIB-001" };
    }

    /// <inheritdoc />
    public override IDataPipe<MaintenanceEvent> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        if (_logger != null)
            _startingGeneration(_logger, _maxEvents, null);

        return new StreamingDataPipe<MaintenanceEvent>(GenerateEventsAsync(cancellationToken), "MaintenanceEventSource");
    }

    private async IAsyncEnumerable<MaintenanceEvent> GenerateEventsAsync([EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var random = new Random(123); // Fixed seed for reproducible results
        var eventId = 1;

        for (var i = 0; i < _maxEvents; i++)
        {
            var deviceId = _deviceIds[random.Next(_deviceIds.Length)];

            var eventType = random.NextDouble() > 0.7
                ? "Scheduled"
                : "Corrective";

            var technicianId = $"TECH-{random.Next(1, 5):D3}";

            var baseTime = DateTime.UtcNow.AddMinutes(-random.Next(60));
            var timestamp = baseTime.AddMinutes(random.Next(-30, 0));

            var maintenanceEvent = new MaintenanceEvent(
                $"MAINT-{eventId:D4}",
                deviceId,
                timestamp,
                eventType,
                technicianId,
                GenerateDescriptionForEvent(eventType, deviceId)
            );

            if (_logger != null)
                _generatedEvent(_logger, maintenanceEvent.MaintenanceId, maintenanceEvent.DeviceId, maintenanceEvent.Timestamp, null);

            yield return maintenanceEvent;

            eventId++;

            if (i < _maxEvents - 1)
                await Task.Delay(_delayBetweenEvents, cancellationToken);
        }

        if (_logger != null)
            _finishedGeneration(_logger, eventId - 1, null);
    }

    private static string GenerateDescriptionForEvent(string eventType, string deviceId)
    {
        return eventType switch
        {
            "Scheduled" => $"Routine maintenance for {deviceId}",
            "Corrective" => $"Fix reported issue with {deviceId}",
            "Emergency" => $"Emergency repair for {deviceId}",
            _ => $"Maintenance activity for {deviceId}",
        };
    }
}
