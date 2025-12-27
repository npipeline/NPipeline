namespace Sample_WatermarkHandling.Models;

/// <summary>
///     Represents overall pipeline statistics and health indicators for the IoT manufacturing platform.
///     This class provides comprehensive monitoring of pipeline performance and system health.
/// </summary>
public class ProcessingStats
{
    /// <summary>
    ///     Initializes a new instance of the ProcessingStats class.
    /// </summary>
    public ProcessingStats()
    {
        TotalEventsProcessed = 0;
        AverageProcessingLatency = TimeSpan.Zero;
        WatermarkAccuracy = new WatermarkAccuracy();
        SystemThroughput = 0.0;
        ErrorRate = 0.0;
        ResourceUtilization = new ResourceUtilization();
        LastUpdated = DateTime.UtcNow;
        ProcessingStartTime = DateTime.UtcNow;
        DeviceStats = new Dictionary<string, DeviceProcessingStats>();
        WindowStats = new WindowProcessingStats();
        AlertStats = new AlertStatistics();
    }

    /// <summary>
    ///     Gets or sets the total count of processed events.
    /// </summary>
    public long TotalEventsProcessed { get; set; }

    /// <summary>
    ///     Gets or sets the average end-to-end processing latency.
    /// </summary>
    public TimeSpan AverageProcessingLatency { get; set; }

    /// <summary>
    ///     Gets or sets the watermark precision metrics.
    /// </summary>
    public WatermarkAccuracy WatermarkAccuracy { get; set; }

    /// <summary>
    ///     Gets or sets the events processed per second.
    /// </summary>
    public double SystemThroughput { get; set; }

    /// <summary>
    ///     Gets or sets the error and exception rates.
    /// </summary>
    public double ErrorRate { get; set; }

    /// <summary>
    ///     Gets or sets the CPU, memory, and network usage.
    /// </summary>
    public ResourceUtilization ResourceUtilization { get; set; }

    /// <summary>
    ///     Gets or sets the timestamp when statistics were last updated.
    /// </summary>
    public DateTime LastUpdated { get; set; }

    /// <summary>
    ///     Gets or sets the timestamp when processing started.
    /// </summary>
    public DateTime ProcessingStartTime { get; set; }

    /// <summary>
    ///     Gets or sets the per-device processing statistics.
    /// </summary>
    public Dictionary<string, DeviceProcessingStats> DeviceStats { get; set; }

    /// <summary>
    ///     Gets or sets the window processing statistics.
    /// </summary>
    public WindowProcessingStats WindowStats { get; set; }

    /// <summary>
    ///     Gets or sets the alert statistics.
    /// </summary>
    public AlertStatistics AlertStats { get; set; }

    /// <summary>
    ///     Gets the total processing duration.
    /// </summary>
    public TimeSpan ProcessingDuration => LastUpdated - ProcessingStartTime;

    /// <summary>
    ///     Gets a value indicating whether the pipeline is healthy.
    /// </summary>
    public bool IsHealthy =>
        ErrorRate < 0.05 &&
        ResourceUtilization.CpuUsage < 0.9 &&
        ResourceUtilization.MemoryUsage < 0.9 &&
        WatermarkAccuracy.OverallAccuracy > 0.8 &&
        SystemThroughput > 0;

    /// <summary>
    ///     Gets a value indicating whether the pipeline performance is optimal.
    /// </summary>
    public bool IsOptimal =>
        ErrorRate < 0.01 &&
        ResourceUtilization.CpuUsage < 0.7 &&
        ResourceUtilization.MemoryUsage < 0.7 &&
        WatermarkAccuracy.OverallAccuracy > 0.95 &&
        AverageProcessingLatency < TimeSpan.FromMilliseconds(100);

    /// <summary>
    ///     Returns a string representation of the processing statistics.
    /// </summary>
    /// <returns>String representation of the processing statistics.</returns>
    public override string ToString()
    {
        var healthStatus = IsOptimal
            ? "OPTIMAL"
            : IsHealthy
                ? "HEALTHY"
                : "DEGRADED";

        return $"Pipeline Stats: Events={TotalEventsProcessed:N0} | " +
               $"Throughput={SystemThroughput:F1}/s | " +
               $"Latency={AverageProcessingLatency.TotalMilliseconds:F0}ms | " +
               $"Error Rate={ErrorRate:P2} | " +
               $"Health={healthStatus} | " +
               $"Duration={ProcessingDuration:hh\\:mm\\:ss}";
    }
}

/// <summary>
///     Represents resource utilization metrics.
/// </summary>
public class ResourceUtilization
{
    /// <summary>
    ///     Initializes a new instance of the ResourceUtilization class.
    /// </summary>
    public ResourceUtilization()
    {
        CpuUsage = 0.0;
        MemoryUsage = 0.0;
        NetworkUsage = 0.0;
        DiskUsage = 0.0;
    }

    /// <summary>
    ///     Gets or sets the CPU usage (0.0 to 1.0).
    /// </summary>
    public double CpuUsage { get; set; }

    /// <summary>
    ///     Gets or sets the memory usage (0.0 to 1.0).
    /// </summary>
    public double MemoryUsage { get; set; }

    /// <summary>
    ///     Gets or sets the network usage (0.0 to 1.0).
    /// </summary>
    public double NetworkUsage { get; set; }

    /// <summary>
    ///     Gets or sets the disk usage (0.0 to 1.0).
    /// </summary>
    public double DiskUsage { get; set; }

    /// <summary>
    ///     Returns a string representation of the resource utilization.
    /// </summary>
    /// <returns>String representation of the resource utilization.</returns>
    public override string ToString()
    {
        return $"Resources: CPU={CpuUsage:P0} | Memory={MemoryUsage:P0} | Network={NetworkUsage:P0} | Disk={DiskUsage:P0}";
    }
}

/// <summary>
///     Represents per-device processing statistics.
/// </summary>
public class DeviceProcessingStats
{
    /// <summary>
    ///     Initializes a new instance of the DeviceProcessingStats class.
    /// </summary>
    /// <param name="deviceId">The device identifier.</param>
    public DeviceProcessingStats(string deviceId)
    {
        DeviceId = deviceId;
        EventsProcessed = 0;
        AverageLatency = TimeSpan.Zero;
        LateDataCount = 0;
        ErrorCount = 0;
        LastEventTime = DateTime.MinValue;
    }

    /// <summary>
    ///     Gets the device identifier.
    /// </summary>
    public string DeviceId { get; }

    /// <summary>
    ///     Gets or sets the number of events processed from this device.
    /// </summary>
    public long EventsProcessed { get; set; }

    /// <summary>
    ///     Gets or sets the average latency for this device.
    /// </summary>
    public TimeSpan AverageLatency { get; set; }

    /// <summary>
    ///     Gets or sets the count of late data from this device.
    /// </summary>
    public long LateDataCount { get; set; }

    /// <summary>
    ///     Gets or sets the error count for this device.
    /// </summary>
    public long ErrorCount { get; set; }

    /// <summary>
    ///     Gets or sets the timestamp of the last event from this device.
    /// </summary>
    public DateTime LastEventTime { get; set; }

    /// <summary>
    ///     Gets the error rate for this device (0.0 to 1.0).
    /// </summary>
    public double ErrorRate => EventsProcessed > 0
        ? (double)ErrorCount / EventsProcessed
        : 0.0;

    /// <summary>
    ///     Gets the late data rate for this device (0.0 to 1.0).
    /// </summary>
    public double LateDataRate => EventsProcessed > 0
        ? (double)LateDataCount / EventsProcessed
        : 0.0;

    /// <summary>
    ///     Returns a string representation of the device processing statistics.
    /// </summary>
    /// <returns>String representation of the device processing statistics.</returns>
    public override string ToString()
    {
        return $"Device {DeviceId}: Events={EventsProcessed} | " +
               $"Latency={AverageLatency.TotalMilliseconds:F0}ms | " +
               $"Late Data={LateDataRate:P2} | " +
               $"Errors={ErrorRate:P2}";
    }
}

/// <summary>
///     Represents window processing statistics.
/// </summary>
public class WindowProcessingStats
{
    /// <summary>
    ///     Initializes a new instance of the WindowProcessingStats class.
    /// </summary>
    public WindowProcessingStats()
    {
        TotalWindows = 0;
        CompletedWindows = 0;
        AverageWindowSize = TimeSpan.Zero;
        LateWindows = 0;
        DroppedWindows = 0;
    }

    /// <summary>
    ///     Gets or sets the total number of windows created.
    /// </summary>
    public long TotalWindows { get; set; }

    /// <summary>
    ///     Gets or sets the number of completed windows.
    /// </summary>
    public long CompletedWindows { get; set; }

    /// <summary>
    ///     Gets or sets the average window size.
    /// </summary>
    public TimeSpan AverageWindowSize { get; set; }

    /// <summary>
    ///     Gets or sets the number of late windows.
    /// </summary>
    public long LateWindows { get; set; }

    /// <summary>
    ///     Gets or sets the number of dropped windows.
    /// </summary>
    public long DroppedWindows { get; set; }

    /// <summary>
    ///     Gets the window completion rate (0.0 to 1.0).
    /// </summary>
    public double CompletionRate => TotalWindows > 0
        ? (double)CompletedWindows / TotalWindows
        : 0.0;

    /// <summary>
    ///     Returns a string representation of the window processing statistics.
    /// </summary>
    /// <returns>String representation of the window processing statistics.</returns>
    public override string ToString()
    {
        return $"Windows: Total={TotalWindows} | " +
               $"Completed={CompletedWindows} ({CompletionRate:P0}) | " +
               $"Late={LateWindows} | " +
               $"Dropped={DroppedWindows} | " +
               $"Avg Size={AverageWindowSize.TotalSeconds:F1}s";
    }
}

/// <summary>
///     Represents alert statistics.
/// </summary>
public class AlertStatistics
{
    /// <summary>
    ///     Initializes a new instance of the AlertStatistics class.
    /// </summary>
    public AlertStatistics()
    {
        TotalAlerts = 0;
        CriticalAlerts = 0;
        WarningAlerts = 0;
        InfoAlerts = 0;
        LastAlertTime = DateTime.MinValue;
    }

    /// <summary>
    ///     Gets or sets the total number of alerts.
    /// </summary>
    public long TotalAlerts { get; set; }

    /// <summary>
    ///     Gets or sets the number of critical alerts.
    /// </summary>
    public long CriticalAlerts { get; set; }

    /// <summary>
    ///     Gets or sets the number of warning alerts.
    /// </summary>
    public long WarningAlerts { get; set; }

    /// <summary>
    ///     Gets or sets the number of info alerts.
    /// </summary>
    public long InfoAlerts { get; set; }

    /// <summary>
    ///     Gets or sets the timestamp of the last alert.
    /// </summary>
    public DateTime LastAlertTime { get; set; }

    /// <summary>
    ///     Returns a string representation of the alert statistics.
    /// </summary>
    /// <returns>String representation of the alert statistics.</returns>
    public override string ToString()
    {
        return $"Alerts: Total={TotalAlerts} | " +
               $"Critical={CriticalAlerts} | " +
               $"Warning={WarningAlerts} | " +
               $"Info={InfoAlerts}";
    }
}
