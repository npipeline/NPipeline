using NPipeline.Nodes;
using NPipeline.Pipeline;

namespace Sample_ParallelProcessing.Nodes;

/// <summary>
///     Transform node that performs CPU-intensive operations on work items.
///     This node demonstrates parallel processing of CPU-bound workloads with thread safety.
/// </summary>
public class ParallelCpuTransform : TransformNode<CpuIntensiveWorkItem, ProcessedWorkItem>
{
    /// <summary>
    ///     Performs CPU-intensive processing on the input work item.
    /// </summary>
    /// <param name="item">The work item to process.</param>
    /// <param name="context">The pipeline execution context.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>A Task containing the processed work item result.</returns>
    public override async Task<ProcessedWorkItem> ExecuteAsync(CpuIntensiveWorkItem item, PipelineContext context, CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        var threadId = Environment.CurrentManagedThreadId;

        Console.WriteLine($"[Thread {threadId}] Processing work item {item.Id} (Size: {item.DataSize}, Complexity: {item.Complexity})");

        // Simulate CPU-intensive work based on data size and complexity
        var result = await PerformCpuIntensiveWork(item, cancellationToken);

        var endTime = DateTime.UtcNow;
        var processingTimeMs = (long)(endTime - startTime).TotalMilliseconds;

        Console.WriteLine($"[Thread {threadId}] Completed work item {item.Id} in {processingTimeMs}ms");

        return new ProcessedWorkItem(
            item.Id,
            result,
            processingTimeMs,
            endTime,
            threadId
        );
    }

    /// <summary>
    ///     Simulates CPU-intensive work based on the work item's characteristics.
    /// </summary>
    /// <param name="item">The work item to process.</param>
    /// <param name="cancellationToken">Cancellation token to stop processing.</param>
    /// <returns>The computed result string.</returns>
    private static async Task<string> PerformCpuIntensiveWork(CpuIntensiveWorkItem item, CancellationToken cancellationToken)
    {
        // Simulate different types of CPU-intensive operations based on complexity
        var result = item.Complexity switch
        {
            <= 3 => await SimpleComputation(item, cancellationToken),
            <= 6 => await MediumComputation(item, cancellationToken),
            _ => await ComplexComputation(item, cancellationToken),
        };

        return result;
    }

    /// <summary>
    ///     Performs simple computation (low complexity).
    /// </summary>
    private static async Task<string> SimpleComputation(CpuIntensiveWorkItem item, CancellationToken cancellationToken)
    {
        // Simple mathematical operations
        var result = 0L;

        for (var i = 0; i < item.DataSize * 100; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            result += i * item.Complexity;
        }

        // Small delay to simulate processing time
        await Task.Delay(10, cancellationToken);

        return $"Simple computation result: {result}";
    }

    /// <summary>
    ///     Performs medium complexity computation.
    /// </summary>
    private static async Task<string> MediumComputation(CpuIntensiveWorkItem item, CancellationToken cancellationToken)
    {
        // More complex mathematical operations with prime number calculation
        var primes = new List<int>();
        var candidate = 2;

        while (primes.Count < item.DataSize / 10)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var isPrime = true;

            for (var i = 2; i <= Math.Sqrt(candidate); i++)
            {
                if (candidate % i == 0)
                {
                    isPrime = false;
                    break;
                }
            }

            if (isPrime)
                primes.Add(candidate);

            candidate++;
        }

        // Medium delay to simulate processing time
        await Task.Delay(50, cancellationToken);

        return $"Medium computation found {primes.Count} primes, last prime: {primes.LastOrDefault()}";
    }

    /// <summary>
    ///     Performs complex computation (high complexity).
    /// </summary>
    private static async Task<string> ComplexComputation(CpuIntensiveWorkItem item, CancellationToken cancellationToken)
    {
        // Complex operations: Fibonacci sequence calculation
        var fibonacciNumbers = new List<long>();
        var n = Math.Min(item.DataSize / 20, 40); // Limit to prevent excessive computation

        for (var i = 0; i < n; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            fibonacciNumbers.Add(Fibonacci(i));
        }

        // Larger delay to simulate complex processing time
        await Task.Delay(100, cancellationToken);

        return $"Complex computation: Fibonacci({n - 1}) = {fibonacciNumbers.LastOrDefault()}, sum: {fibonacciNumbers.Sum()}";
    }

    /// <summary>
    ///     Calculates the nth Fibonacci number using iterative approach.
    /// </summary>
    private static long Fibonacci(int n)
    {
        if (n <= 1)
            return n;

        var a = 0L;
        var b = 1L;

        for (var i = 2; i <= n; i++)
        {
            var temp = a + b;
            a = b;
            b = temp;
        }

        return b;
    }
}
