using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_LookupNode.Helpers;
using Sample_LookupNode.Models;

namespace Sample_LookupNode.Nodes;

/// <summary>
///     Custom LookupNode that enriches sensor readings with device metadata.
///     This node demonstrates how to use LookupNode for external data enrichment
///     by inheriting from LookupNode&lt;SensorReading, string, DeviceMetadata, SensorReadingWithMetadata&gt;.
/// </summary>
public class DeviceMetadataLookup : LookupNode<SensorReading, string, DeviceMetadata, SensorReadingWithMetadata>
{
    /// <summary>
    ///     Extracts the lookup key from the input sensor reading.
    /// </summary>
    /// <param name="input">The sensor reading input item.</param>
    /// <param name="context">The pipeline context.</param>
    /// <returns>The DeviceId to use for the lookup.</returns>
    protected override string ExtractKey(SensorReading input, PipelineContext context)
    {
        return input.DeviceId;
    }

    /// <summary>
    ///     Performs the asynchronous lookup operation to retrieve device metadata.
    /// </summary>
    /// <param name="key">The DeviceId to look up.</param>
    /// <param name="context">The current pipeline context.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous lookup operation.</returns>
    protected override async Task<DeviceMetadata?> LookupAsync(string key, PipelineContext context, CancellationToken cancellationToken)
    {
        // Simulate async lookup delay (e.g., database or API call)
        await Task.Delay(Random.Shared.Next(10, 50), cancellationToken);

        // Look up device metadata from the registry
        var device = DeviceRegistry.GetDevice(key);

        if (device != null)
            Console.WriteLine($"✓ Found device metadata for {key}: {device.DeviceType} at {device.FactoryLocation}");
        else
            Console.WriteLine($"⚠ Device not found: {key} - using default metadata");

        return device;
    }

    /// <summary>
    ///     Creates the final enriched output by combining sensor reading with device metadata.
    /// </summary>
    /// <param name="input">The original sensor reading input item.</param>
    /// <param name="lookupValue">The device metadata retrieved from the lookup, or null if not found.</param>
    /// <param name="context">The pipeline context.</param>
    /// <returns>The enriched sensor reading with device information.</returns>
    protected override SensorReadingWithMetadata CreateOutput(SensorReading input, DeviceMetadata? lookupValue, PipelineContext context)
    {
        if (lookupValue != null)
            return new SensorReadingWithMetadata(input, lookupValue);

        // Handle missing devices gracefully with default metadata
        var defaultMetadata = new DeviceMetadata(
            input.DeviceId,
            "Unknown Location",
            "Unknown Device Type",
            DateTime.UtcNow.AddYears(-1),
            DateTime.UtcNow.AddDays(-30),
            DateTime.UtcNow.AddDays(335),
            "Unknown",
            new Dictionary<string, object> { ["Status"] = "Device not registered" }
        );

        return new SensorReadingWithMetadata(input, defaultMetadata);
    }
}

/// <summary>
///     Represents a sensor reading that has been enriched with device metadata.
///     This record is an intermediate model between lookup and calibration validation.
/// </summary>
/// <param name="OriginalReading">The original raw sensor reading before enrichment.</param>
/// <param name="DeviceInfo">Device metadata retrieved during the enrichment process.</param>
public record SensorReadingWithMetadata(
    SensorReading OriginalReading,
    DeviceMetadata DeviceInfo
);
