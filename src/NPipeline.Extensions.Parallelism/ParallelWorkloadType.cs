namespace NPipeline.Extensions.Parallelism;

/// <summary>
///     Workload type hint for automatic parallel configuration with sensible defaults.
///     Use this to configure parallelism based on your workload characteristics,
///     avoiding manual calculation of degree of parallelism and queue sizes.
/// </summary>
public enum ParallelWorkloadType
{
    /// <summary>
    ///     General-purpose workload with balanced CPU and I/O characteristics.
    ///     Uses ProcessorCount * 2 parallelism with moderate queue sizes.
    ///     Suitable for most common scenarios that don't fit other categories.
    /// </summary>
    General,

    /// <summary>
    ///     CPU-intensive workload (calculations, compression, encryption, data processing).
    ///     Uses ProcessorCount parallelism to avoid oversubscription and context-switching overhead.
    ///     Queue size: ProcessorCount * 2.
    ///     Best for: Heavy computation, no I/O waiting.
    /// </summary>
    CpuBound,

    /// <summary>
    ///     I/O-intensive workload (file access, database queries, local operations).
    ///     Uses ProcessorCount * 4 parallelism to hide I/O latency.
    ///     Queue size: ProcessorCount * 8.
    ///     Best for: File reads/writes, database operations, disk access.
    /// </summary>
    IoBound,

    /// <summary>
    ///     Network-intensive workload (HTTP calls, API requests, remote operations).
    ///     Uses higher parallelism (up to 100) to maximize throughput under high-latency conditions.
    ///     Queue size: 200.
    ///     Best for: Web service calls, REST APIs, remote API access.
    /// </summary>
    NetworkBound,
}
