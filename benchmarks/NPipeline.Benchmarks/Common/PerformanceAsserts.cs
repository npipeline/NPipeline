namespace NPipeline.Benchmarks.Common;

/// <summary>
///     Helper class for automated validation of performance claims in benchmarks.
/// </summary>
public static class PerformanceAsserts
{
    /// <summary>
    ///     Asserts that throughput meets or exceeds the expected minimum.
    /// </summary>
    public static void AssertThroughput(double actualThroughput, double expectedMinThroughput, string scenario)
    {
        if (actualThroughput < expectedMinThroughput)
        {
            throw new PerformanceAssertionException(
                $"Throughput assertion failed for {scenario}: " +
                $"Expected >= {expectedMinThroughput:F2} items/sec, " +
                $"Actual: {actualThroughput:F2} items/sec");
        }
    }

    /// <summary>
    ///     Asserts that latency is within acceptable bounds.
    /// </summary>
    public static void AssertLatency(TimeSpan actualLatency, TimeSpan maxLatency, string scenario)
    {
        if (actualLatency > maxLatency)
        {
            throw new PerformanceAssertionException(
                $"Latency assertion failed for {scenario}: " +
                $"Expected <= {maxLatency.TotalMilliseconds:F2}ms, " +
                $"Actual: {actualLatency.TotalMilliseconds:F2}ms");
        }
    }

    /// <summary>
    ///     Asserts that memory allocation is within acceptable bounds.
    /// </summary>
    public static void AssertMemoryAllocation(long actualBytes, long maxExpectedBytes, string scenario)
    {
        if (actualBytes > maxExpectedBytes)
        {
            throw new PerformanceAssertionException(
                $"Memory allocation assertion failed for {scenario}: " +
                $"Expected <= {maxExpectedBytes / (1024.0 * 1024.0):F2}MB, " +
                $"Actual: {actualBytes / (1024.0 * 1024.0):F2}MB");
        }
    }

    /// <summary>
    ///     Asserts that zero allocation is achieved (or very close to it).
    /// </summary>
    public static void AssertZeroAllocation(long actualBytes, string scenario, long toleranceBytes = 1024)
    {
        if (actualBytes > toleranceBytes)
        {
            throw new PerformanceAssertionException(
                $"Zero allocation assertion failed for {scenario}: " +
                $"Expected <= {toleranceBytes} bytes, " +
                $"Actual: {actualBytes} bytes");
        }
    }

    /// <summary>
    ///     Asserts that GC pressure is within acceptable limits.
    /// </summary>
    public static void AssertGCCollections(
        int gen0Collections, int gen1Collections, int gen2Collections,
        int maxGen0, int maxGen1, int maxGen2, string scenario)
    {
        var failures = new List<string>();

        if (gen0Collections > maxGen0)
            failures.Add($"Gen0: Expected <= {maxGen0}, Actual: {gen0Collections}");

        if (gen1Collections > maxGen1)
            failures.Add($"Gen1: Expected <= {maxGen1}, Actual: {gen1Collections}");

        if (gen2Collections > maxGen2)
            failures.Add($"Gen2: Expected <= {maxGen2}, Actual: {gen2Collections}");

        if (failures.Count > 0)
        {
            throw new PerformanceAssertionException(
                $"GC pressure assertion failed for {scenario}: " +
                string.Join(", ", failures));
        }
    }

    /// <summary>
    ///     Asserts that performance scaling is linear or better.
    /// </summary>
    public static void AssertLinearScaling(
        double baselineThroughput, double scaledThroughput,
        double scalingFactor, string scenario, double tolerance = 0.1)
    {
        var expectedThroughput = baselineThroughput * scalingFactor;
        var minAcceptable = expectedThroughput * (1.0 - tolerance);

        if (scaledThroughput < minAcceptable)
        {
            throw new PerformanceAssertionException(
                $"Linear scaling assertion failed for {scenario}: " +
                $"Expected >= {expectedThroughput:F2} items/sec (with {tolerance:P0} tolerance), " +
                $"Actual: {scaledThroughput:F2} items/sec " +
                $"(baseline: {baselineThroughput:F2}, scaling factor: {scalingFactor:F2})");
        }
    }

    /// <summary>
    ///     Asserts that performance is consistent across multiple runs.
    /// </summary>
    public static void AssertPerformanceConsistency(
        IEnumerable<double> measurements, string scenario, double maxVariabilityPercent = 0.1)
    {
        var measurementsList = measurements.ToList();

        if (measurementsList.Count < 2)
            return; // Can't measure consistency with less than 2 measurements

        var mean = measurementsList.Average();
        var standardDeviation = CalculateStandardDeviation(measurementsList);
        var coefficientOfVariation = standardDeviation / mean;

        if (coefficientOfVariation > maxVariabilityPercent)
        {
            throw new PerformanceAssertionException(
                $"Performance consistency assertion failed for {scenario}: " +
                $"Coefficient of variation: {coefficientOfVariation:P2}, " +
                $"Maximum allowed: {maxVariabilityPercent:P2}");
        }
    }

    /// <summary>
    ///     Asserts that cache hit rate meets expectations.
    /// </summary>
    public static void AssertCacheHitRate(double actualHitRate, double minExpectedHitRate, string scenario)
    {
        if (actualHitRate < minExpectedHitRate)
        {
            throw new PerformanceAssertionException(
                $"Cache hit rate assertion failed for {scenario}: " +
                $"Expected >= {minExpectedHitRate:P2}, " +
                $"Actual: {actualHitRate:P2}");
        }
    }

    /// <summary>
    ///     Asserts that parallel efficiency is acceptable.
    /// </summary>
    public static void AssertParallelEfficiency(
        double singleThreadThroughput, double multiThreadThroughput,
        int threadCount, string scenario, double minEfficiencyPercent = 0.7)
    {
        var theoreticalMax = singleThreadThroughput * threadCount;
        var actualEfficiency = multiThreadThroughput / theoreticalMax;

        if (actualEfficiency < minEfficiencyPercent)
        {
            throw new PerformanceAssertionException(
                $"Parallel efficiency assertion failed for {scenario}: " +
                $"Efficiency: {actualEfficiency:P2}, " +
                $"Minimum required: {minEfficiencyPercent:P2} " +
                $"(single-thread: {singleThreadThroughput:F2}, " +
                $"multi-thread: {multiThreadThroughput:F2}, threads: {threadCount})");
        }
    }

    /// <summary>
    ///     Validates that streaming approach is more memory efficient than materialization.
    /// </summary>
    public static void AssertStreamingMemoryEfficiency(
        long streamingMemory, long materializationMemory,
        string scenario, double minImprovementRatio = 0.5)
    {
        if (materializationMemory <= 0)
            return; // Avoid division by zero

        var improvementRatio = 1.0 - (double)streamingMemory / materializationMemory;

        if (improvementRatio < minImprovementRatio)
        {
            throw new PerformanceAssertionException(
                $"Streaming memory efficiency assertion failed for {scenario}: " +
                $"Memory improvement: {improvementRatio:P2}, " +
                $"Minimum required: {minImprovementRatio:P2} " +
                $"(streaming: {streamingMemory / (1024.0 * 1024.0):F2}MB, " +
                $"materialization: {materializationMemory / (1024.0 * 1024.0):F2}MB)");
        }
    }

    /// <summary>
    ///     Asserts that ValueTask fast path provides expected performance benefits.
    /// </summary>
    public static void AssertValueTaskFastPath(
        TimeSpan valueTaskTime, TimeSpan taskTime,
        string scenario, double minImprovementPercent = 0.1)
    {
        if (taskTime <= TimeSpan.Zero)
            return; // Avoid division by zero

        var improvementRatio = 1.0 - valueTaskTime.TotalNanoseconds / taskTime.TotalNanoseconds;

        if (improvementRatio < minImprovementPercent)
        {
            throw new PerformanceAssertionException(
                $"ValueTask fast path assertion failed for {scenario}: " +
                $"Performance improvement: {improvementRatio:P2}, " +
                $"Minimum required: {minImprovementPercent:P2} " +
                $"(ValueTask: {valueTaskTime.TotalNanoseconds:F0}ns, " +
                $"Task: {taskTime.TotalNanoseconds:F0}ns)");
        }
    }

    private static double CalculateStandardDeviation(IEnumerable<double> values)
    {
        var valuesList = values.ToList();

        if (valuesList.Count <= 1)
            return 0;

        var mean = valuesList.Average();
        var sumOfSquares = valuesList.Sum(x => Math.Pow(x - mean, 2));
        return Math.Sqrt(sumOfSquares / (valuesList.Count - 1));
    }
}

/// <summary>
///     Exception thrown when a performance assertion fails.
/// </summary>
public class PerformanceAssertionException : Exception
{
    public PerformanceAssertionException(string message) : base(message)
    {
    }

    public PerformanceAssertionException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
