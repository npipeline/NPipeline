using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_08_CustomNodeImplementation.Models;

namespace Sample_08_CustomNodeImplementation.Nodes;

/// <summary>
///     Transform node that monitors and tracks lifecycle events for observability.
///     This node demonstrates how to implement a monitoring transform that tracks
///     node lifecycle events while passing through the original data.
/// </summary>
/// <remarks>
///     This implementation demonstrates:
///     - Lifecycle event tracking for observability
///     - Pass-through transform pattern
///     - Performance monitoring capabilities
///     - Structured code for testability
/// </remarks>
public class LifecycleMonitorNode : TransformNode<SensorData, SensorData>
{
    private readonly List<LifecycleEvent> _lifecycleEvents = new();
    private bool _disposed;
    private int _processedCount;
    private readonly DateTime _startTime;

    /// <summary>
    ///     Initializes a new instance of the LifecycleMonitorNode class.
    /// </summary>
    public LifecycleMonitorNode()
    {
        _startTime = DateTime.UtcNow;
        RecordLifecycleEvent(LifecycleEventType.NodeInitializing, "Lifecycle monitor initialization started");
        Console.WriteLine("Initializing LifecycleMonitorNode...");
        RecordLifecycleEvent(LifecycleEventType.NodeInitialized, "Lifecycle monitor initialization completed");
        Console.WriteLine("LifecycleMonitorNode initialized successfully");
    }

    /// <summary>
    ///     Processes sensor data while tracking lifecycle events and performance metrics.
    /// </summary>
    /// <param name="item">The sensor data to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the processed sensor data (unchanged).</returns>
    public override async Task<SensorData> ExecuteAsync(SensorData item, PipelineContext context, CancellationToken cancellationToken)
    {
        if (_processedCount == 0)
            RecordLifecycleEvent(LifecycleEventType.NodeExecutionStarted, "Started processing sensor data");

        _processedCount++;

        // Log every 10th item to avoid spamming the console
        if (_processedCount % 10 == 0)
        {
            var elapsed = DateTime.UtcNow - _startTime;
            Console.WriteLine($"LifecycleMonitor: Processed {_processedCount} items in {elapsed.TotalSeconds:F2} seconds");
        }

        // Add monitoring metadata to the sensor data
        var monitoredItem = item with
        {
            Metadata = new Dictionary<string, object>(item.Metadata)
            {
                ["MonitoredAt"] = DateTime.UtcNow,
                ["ProcessingOrder"] = _processedCount,
                ["MonitorNode"] = "LifecycleMonitorNode",
            },
        };

        // Simulate some processing work
        await Task.Delay(1, cancellationToken);

        return monitoredItem;
    }

    /// <summary>
    ///     Records a lifecycle event.
    /// </summary>
    /// <param name="eventType">The type of lifecycle event.</param>
    /// <param name="message">Optional message for the event.</param>
    private void RecordLifecycleEvent(LifecycleEventType eventType, string? message = null)
    {
        var lifecycleEvent = new LifecycleEvent
        {
            EventType = eventType,
            NodeName = nameof(LifecycleMonitorNode),
            Timestamp = DateTime.UtcNow,
            Message = message,
            Metadata = new Dictionary<string, object>
            {
                ["ProcessedCount"] = _processedCount,
                ["ElapsedTime"] = DateTime.UtcNow - _startTime,
            },
        };

        _lifecycleEvents.Add(lifecycleEvent);
    }

    /// <summary>
    ///     Gets the recorded lifecycle events.
    /// </summary>
    /// <returns>A read-only list of lifecycle events.</returns>
    public IReadOnlyList<LifecycleEvent> GetLifecycleEvents()
    {
        return _lifecycleEvents.AsReadOnly();
    }

    /// <summary>
    ///     Asynchronously disposes of the node and releases all resources.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that represents the asynchronous dispose operation.</returns>
    public override async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            RecordLifecycleEvent(LifecycleEventType.NodeDisposing, "Lifecycle monitor disposal started");

            // Dispose managed resources here if needed
            _disposed = true;

            RecordLifecycleEvent(LifecycleEventType.NodeDisposed, "Lifecycle monitor disposal completed");
            Console.WriteLine("LifecycleMonitorNode disposed successfully");
        }

        GC.SuppressFinalize(this);
        await base.DisposeAsync();
    }
}
