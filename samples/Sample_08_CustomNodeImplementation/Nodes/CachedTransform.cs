using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_08_CustomNodeImplementation.Models;

namespace Sample_08_CustomNodeImplementation.Nodes;

/// <summary>
///     Advanced transform node with caching capabilities for performance optimization.
///     This node demonstrates how to implement a transform with intelligent caching
///     to improve performance for expensive calculations.
/// </summary>
/// <remarks>
///     This implementation demonstrates:
///     - Advanced transform node implementation
///     - Performance optimization through caching
///     - Cache invalidation strategies
///     - Structured code for testability
/// </remarks>
public class CachedTransform : TransformNode<SensorData, ProcessedSensorData>
{
    private readonly Dictionary<string, ProcessedSensorData> _cache;
    private readonly TimeSpan _cacheExpiry = TimeSpan.FromMinutes(5);
    private readonly Dictionary<string, DateTime> _cacheTimestamps;
    private int _cacheHits;
    private int _cacheMisses;
    private bool _disposed;
    private int _processedCount;

    /// <summary>
    ///     Initializes a new instance of the CachedTransform class.
    /// </summary>
    public CachedTransform()
    {
        _cache = new Dictionary<string, ProcessedSensorData>();
        _cacheTimestamps = new Dictionary<string, DateTime>();
        Console.WriteLine("Initializing CachedTransform with performance optimization...");
        Console.WriteLine("CachedTransform initialized successfully");
        Console.WriteLine($"Cache configuration: Expiry = {_cacheExpiry.TotalMinutes} minutes");
    }

    /// <summary>
    ///     Processes sensor data with caching for performance optimization.
    /// </summary>
    /// <param name="item">The sensor data to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the processed sensor data.</returns>
    public override async Task<ProcessedSensorData> ExecuteAsync(SensorData item, PipelineContext context, CancellationToken cancellationToken)
    {
        _processedCount++;

        // Generate cache key based on sensor data
        var cacheKey = GenerateCacheKey(item);

        // Check if we have a cached result
        if (_cache.TryGetValue(cacheKey, out var cachedResult) &&
            _cacheTimestamps.TryGetValue(cacheKey, out var cacheTime) &&
            DateTime.UtcNow - cacheTime < _cacheExpiry)
        {
            _cacheHits++;

            // Log cache hits periodically
            if (_cacheHits % 5 == 0)
                Console.WriteLine($"CachedTransform: Cache hit #{_cacheHits} for sensor {item.SensorId}");

            return cachedResult;
        }

        // Cache miss - perform the expensive calculation
        _cacheMisses++;
        var processedResult = await PerformExpensiveCalculation(item, cancellationToken);

        // Store in cache
        _cache[cacheKey] = processedResult;
        _cacheTimestamps[cacheKey] = DateTime.UtcNow;

        // Log cache misses periodically
        if (_cacheMisses % 5 == 0)
            Console.WriteLine($"CachedTransform: Cache miss #{_cacheMisses} for sensor {item.SensorId} - performed expensive calculation");

        return processedResult;
    }

    /// <summary>
    ///     Generates a cache key for sensor data.
    /// </summary>
    /// <param name="sensorData">The sensor data to generate a key for.</param>
    /// <returns>A cache key string.</returns>
    private string GenerateCacheKey(SensorData sensorData)
    {
        // Create a cache key based on sensor ID, value rounded to 2 decimals, and unit
        // This ensures similar readings from the same sensor use cached results
        return $"{sensorData.SensorId}_{Math.Round(sensorData.Value, 2)}_{sensorData.Unit}";
    }

    /// <summary>
    ///     Performs an expensive calculation on sensor data.
    /// </summary>
    /// <param name="sensorData">The sensor data to process.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the processed sensor data.</returns>
    private async Task<ProcessedSensorData> PerformExpensiveCalculation(SensorData sensorData, CancellationToken cancellationToken)
    {
        // Simulate an expensive calculation (e.g., complex mathematical operations, external API calls, etc.)
        await Task.Delay(50, cancellationToken); // Simulate 50ms of processing time

        // Perform some complex calculations
        var processedValue = sensorData.Value;

        // Apply different transformations based on unit
        if (sensorData.Unit == "°C")
        {
            // Temperature processing: apply calibration and convert to Fahrenheit
            processedValue = processedValue * 1.8 + 32; // Convert to Fahrenheit
            processedValue = Math.Round(processedValue + 0.5, 2); // Add calibration offset
        }
        else if (sensorData.Unit == "kPa")
        {
            // Pressure processing: apply normalization
            processedValue = Math.Round(processedValue * 0.145, 2); // Convert to PSI
        }

        // Determine processing status
        var status = ProcessingStatus.Success;

        if (sensorData.Metadata.TryGetValue("Quality", out var qualityObj) &&
            qualityObj.ToString() == "Questionable")
            status = ProcessingStatus.ValidationError;

        return new ProcessedSensorData
        {
            OriginalData = sensorData,
            ProcessedValue = processedValue,
            Status = status,
            ProcessedAt = DateTime.UtcNow,
            ProcessingMetadata = new Dictionary<string, object>
            {
                ["ProcessingTimeMs"] = 50,
                ["CacheEnabled"] = true,
                ["CalibrationApplied"] = true,
                ["UnitConversion"] = sensorData.Unit == "°C"
                    ? "°F"
                    : "PSI",
            },
        };
    }

    /// <summary>
    ///     Cleans up expired cache entries.
    /// </summary>
    private void CleanupExpiredCacheEntries()
    {
        var expiredKeys = _cacheTimestamps
            .Where(kvp => DateTime.UtcNow - kvp.Value >= _cacheExpiry)
            .Select(kvp => kvp.Key)
            .ToList();

        foreach (var key in expiredKeys)
        {
            _cache.Remove(key);
            _cacheTimestamps.Remove(key);
        }

        if (expiredKeys.Count > 0)
            Console.WriteLine($"CachedTransform: Cleaned up {expiredKeys.Count} expired cache entries");
    }

    /// <summary>
    ///     Gets cache statistics for monitoring.
    /// </summary>
    /// <returns>A dictionary containing cache statistics.</returns>
    public Dictionary<string, object> GetCacheStatistics()
    {
        return new Dictionary<string, object>
        {
            ["CacheHits"] = _cacheHits,
            ["CacheMisses"] = _cacheMisses,
            ["ProcessedCount"] = _processedCount,
            ["HitRate"] = _processedCount > 0
                ? _cacheHits * 100.0 / _processedCount
                : 0,
            ["CacheSize"] = _cache.Count,
            ["CacheExpiryMinutes"] = _cacheExpiry.TotalMinutes,
        };
    }

    /// <summary>
    ///     Asynchronously disposes of resources used by the cached transform node.
    /// </summary>
    /// <returns>A ValueTask representing the asynchronous dispose operation.</returns>
    public override async ValueTask DisposeAsync()
    {
        if (!_disposed)
        {
            // Clean up expired cache entries
            CleanupExpiredCacheEntries();

            var hitRate = _processedCount > 0
                ? _cacheHits * 100.0 / _processedCount
                : 0;

            Console.WriteLine($"CachedTransform: Processing complete - {_processedCount} items processed");
            Console.WriteLine($"CachedTransform: Cache performance - {_cacheHits} hits, {_cacheMisses} misses, {hitRate:F1}% hit rate");
            Console.WriteLine($"CachedTransform: Cache contains {_cache.Count} valid entries");
            Console.WriteLine("Disposing CachedTransform...");
            Console.WriteLine($"Final cache statistics: {string.Join(", ", GetCacheStatistics().Select(kvp => $"{kvp.Key}={kvp.Value}"))}");

            _cache.Clear();
            _cacheTimestamps.Clear();

            _disposed = true;
            GC.SuppressFinalize(this);
            await base.DisposeAsync();
        }
    }
}
