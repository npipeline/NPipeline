using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_BatchingNode.Nodes;

/// <summary>
///     Transform node that processes batches of sensor readings efficiently.
///     This node demonstrates the benefits of batch processing for computational operations.
/// </summary>
public class BatchProcessingTransform : TransformNode<IReadOnlyCollection<SensorReading>, BatchProcessingResult>
{
    private readonly bool _simulateProcessingDelay;

    /// <summary>
    ///     Initializes a new instance of the <see cref="BatchProcessingTransform" /> class.
    /// </summary>
    /// <param name="simulateProcessingDelay">Whether to simulate processing delay to demonstrate batch efficiency.</param>
    public BatchProcessingTransform(bool simulateProcessingDelay = true)
    {
        _simulateProcessingDelay = simulateProcessingDelay;
    }

    /// <summary>
    ///     Processes a batch of sensor readings to compute aggregated statistics.
    ///     This demonstrates how batch processing can be more efficient than individual processing.
    /// </summary>
    /// <param name="batch">The batch of sensor readings to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A batch processing result with aggregated statistics.</returns>
    public override async Task<BatchProcessingResult> ExecuteAsync(
        IReadOnlyCollection<SensorReading> batch,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        var processingStartTime = DateTime.UtcNow;
        var batchId = Guid.NewGuid().ToString("N")[..8];

        Console.WriteLine($"Processing batch {batchId} with {batch.Count} sensor readings");

        if (batch.Count == 0)
        {
            Console.WriteLine($"Batch {batchId} is empty, skipping processing");

            return new BatchProcessingResult(
                batchId,
                processingStartTime,
                "Unknown",
                0,
                0,
                0,
                0,
                0,
                0,
                0);
        }

        // Group by device to process each device's readings separately within the batch
        var deviceGroups = batch.GroupBy(r => r.DeviceId).ToList();

        // For this example, we'll focus on the primary device (first one in the batch)
        // In a real scenario, you might process each device separately or create multiple results
        var primaryDeviceGroup = deviceGroups.First();
        var deviceId = primaryDeviceGroup.Key;
        var readings = primaryDeviceGroup.ToList();

        // Simulate processing delay to demonstrate the efficiency of batch processing
        if (_simulateProcessingDelay)
        {
            // Simulate 10ms per reading, but batch processing is more efficient
            var delay = Math.Max(50, readings.Count * 2); // Minimum 50ms, but much less than individual processing
            await Task.Delay(delay, cancellationToken);
        }

        // Calculate aggregated statistics
        var averageTemperature = readings.Average(r => r.Temperature);
        var averageHumidity = readings.Average(r => r.Humidity);
        var averagePressure = readings.Average(r => r.Pressure);
        var minBatteryLevel = readings.Min(r => r.BatteryLevel);
        var temperatureRange = readings.Max(r => r.Temperature) - readings.Min(r => r.Temperature);

        var processingEndTime = DateTime.UtcNow;
        var processingTimeMs = (long)(processingEndTime - processingStartTime).TotalMilliseconds;

        var result = new BatchProcessingResult(
            batchId,
            processingEndTime,
            deviceId,
            readings.Count,
            Math.Round(averageTemperature, 2),
            Math.Round(averageHumidity, 2),
            Math.Round(averagePressure, 2),
            Math.Round(minBatteryLevel, 2),
            Math.Round(temperatureRange, 2),
            processingTimeMs);

        Console.WriteLine($"Batch {batchId} processed successfully in {processingTimeMs}ms:");
        Console.WriteLine($"  Device: {deviceId}");
        Console.WriteLine($"  Readings: {readings.Count}");
        Console.WriteLine($"  Avg Temp: {result.AverageTemperature}°C");
        Console.WriteLine($"  Avg Humidity: {result.AverageHumidity}%");
        Console.WriteLine($"  Min Battery: {result.MinBatteryLevel}%");
        Console.WriteLine($"  Temp Range: {result.TemperatureRange}°C");

        // If there are multiple devices in the batch, log that information
        if (deviceGroups.Count > 1)
        {
            Console.WriteLine($"  Note: Batch contained readings from {deviceGroups.Count} devices, processed primary device {deviceId}");

            foreach (var group in deviceGroups.Skip(1))
            {
                Console.WriteLine($"    {group.Key}: {group.Count()} readings (not processed in this result)");
            }
        }

        return result;
    }
}
