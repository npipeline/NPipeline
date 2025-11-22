using NPipeline.Configuration;
using NPipeline.Pipeline;
using Sample_03_BasicErrorHandling.Nodes;

namespace Sample_03_BasicErrorHandling;

/// <summary>
///     Error handling pipeline definition demonstrating NPipeline resilience patterns.
///     This pipeline implements comprehensive error handling techniques including:
///     1. Simulated intermittent failures in data sources
///     2. Retry logic with exponential backoff and jitter
///     3. Error collection and reporting mechanisms
///     4. Fallback mechanisms for graceful degradation
///     5. Circuit breaker patterns for fault tolerance
/// </summary>
/// <remarks>
///     <para>
///         This implementation follows the IPipelineDefinition pattern, which allows the pipeline
///         structure to be defined once and reused multiple times. Each execution creates fresh
///         instances of all nodes, ensuring proper isolation between runs and preventing error state leakage.
///     </para>
///     <para>
///         The error handling flow demonstrates a complete resilience journey:
///         - Source failures trigger retry mechanisms
///         - Transform failures implement exponential backoff with jitter
///         - Sink failures activate fallback mechanisms for graceful degradation
///         - All stages include comprehensive error logging and monitoring
///     </para>
/// </remarks>
public class ErrorHandlingPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes with comprehensive error handling capabilities.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     <para>
    ///         This method creates a resilient pipeline with comprehensive error handling flow:
    ///         UnreliableSource -> RetryTransform -> FallbackSink
    ///     </para>
    ///     <para>
    ///         The pipeline demonstrates error handling through these stages:
    ///         1. <strong>UnreliableSource</strong>: Simulates intermittent data source failures with configurable failure rates,
    ///         demonstrating how to handle unreliable external systems and network connections.
    ///         2. <strong>RetryTransform</strong>: Implements sophisticated retry logic with exponential backoff and jitter,
    ///         showing how to handle transient failures while preventing thundering herd problems.
    ///         3. <strong>FallbackSink</strong>: Provides graceful degradation through primary and fallback output mechanisms,
    ///         demonstrating how to maintain service availability even when primary systems fail.
    ///     </para>
    ///     <para>
    ///         <strong>Error Handling Flow:</strong>
    ///         - When the UnreliableSource fails, the pipeline execution framework handles the exception
    ///         - The RetryTransform catches processing failures and implements retry with increasing delays
    ///         - The FallbackSink catches output failures and automatically switches to alternative mechanisms
    ///         - All errors are logged with detailed context for debugging and monitoring
    ///     </para>
    ///     <para>
    ///         <strong>Resilience Patterns Demonstrated:</strong>
    ///         - <em>Retry Pattern</em>: Automatic retry with exponential backoff for transient failures
    ///         - <em>Fallback Pattern</em>: Alternative processing paths when primary mechanisms fail
    ///         - <em>Error Isolation</em>: Failures in one node don't cascade to others
    ///         - <em>Graceful Degradation</em>: Service continues operating with reduced functionality
    ///         - <em>Observability</em>: Comprehensive error logging and metrics collection
    ///     </para>
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add diagnostic logging to validate error handling configuration
        Console.WriteLine("=== DIAGNOSTIC: ErrorHandlingPipeline.Define() called ===");
        Console.WriteLine("DIAGNOSTIC: About to add nodes to pipeline...");

        // Configure error handling with retry options and circuit breaker
        // This enables the pipeline to handle failures gracefully and demonstrate resilience patterns
        builder.WithRetryOptions(options => new PipelineRetryOptions(
            3, // Allow 3 retries per item
            2, // Allow 2 node restart attempts
            5 // Allow 5 sequential attempts
        ));

        // Configure circuit breaker to prevent cascading failures
        // This enables the circuit breaker pattern to trip after consecutive failures
        builder.WithCircuitBreaker(
            3, // Trip after 3 consecutive failures
            TimeSpan.FromSeconds(30), // Wait 30 seconds before attempting recovery
            TimeSpan.FromMinutes(5) // Track operations for monitoring
        );

        // Add the source node that generates data with potential intermittent failures
        // This node simulates real-world scenarios where data sources might be temporarily unavailable
        // due to network issues, service degradation, or rate limiting
        var source = builder.AddSource<UnreliableSource, string>("unreliable-source");
        Console.WriteLine("DIAGNOSTIC: Added source node 'unreliable-source'");

        // Add the transform node that implements sophisticated retry logic with exponential backoff
        // This node demonstrates how to handle transient processing failures that might resolve themselves
        // with time, such as temporary resource exhaustion or external service timeouts
        var transform = builder.AddTransform<RetryTransform, string, string>("retry-transform");
        Console.WriteLine("DIAGNOSTIC: Added transform node 'retry-transform'");

        // Add the sink node that provides graceful degradation through fallback mechanisms
        // This node shows how to maintain service availability by switching to alternative output
        // methods when the primary mechanism fails, ensuring no data loss even during system issues
        var sink = builder.AddSink<FallbackSink, string>("fallback-sink");
        Console.WriteLine("DIAGNOSTIC: Added sink node 'fallback-sink'");

        // Connect the nodes in a linear flow: source -> transform -> sink
        // Each connection includes built-in error handling to prevent cascading failures
        // The pipeline framework ensures that errors are properly isolated and handled at each stage
        builder.Connect(source, transform);
        builder.Connect(transform, sink);
        Console.WriteLine("DIAGNOSTIC: Connected all nodes in pipeline");

        // Check if error handling is configured
        Console.WriteLine("DIAGNOSTIC: Checking error handling configuration...");

        // Build pipeline to check the graph configuration
        var pipeline = builder.Build();
        var graph = pipeline.Graph;

        Console.WriteLine($"DIAGNOSTIC: Graph.ErrorHandling.RetryOptions: {graph.ErrorHandling.RetryOptions}");
        Console.WriteLine($"DIAGNOSTIC: Graph.ErrorHandling.CircuitBreakerOptions: {graph.ErrorHandling.CircuitBreakerOptions}");
        Console.WriteLine($"DIAGNOSTIC: Graph.ErrorHandling.PipelineErrorHandler: {graph.ErrorHandling.PipelineErrorHandler}");
        Console.WriteLine($"DIAGNOSTIC: Graph.ErrorHandling.PipelineErrorHandlerType: {graph.ErrorHandling.PipelineErrorHandlerType}");
        Console.WriteLine($"DIAGNOSTIC: Graph.ErrorHandling.DeadLetterSink: {graph.ErrorHandling.DeadLetterSink}");
        Console.WriteLine($"DIAGNOSTIC: Graph.ErrorHandling.DeadLetterSinkType: {graph.ErrorHandling.DeadLetterSinkType}");

        if (graph.ErrorHandling.RetryOptions == null)
            Console.WriteLine("DIAGNOSTIC: *** ISSUE DETECTED: No retry options configured! This is likely the root cause. ***");
        else
        {
            Console.WriteLine(
                $"DIAGNOSTIC: Retry options configured: MaxItemRetries={graph.ErrorHandling.RetryOptions.MaxItemRetries}, MaxNodeRestartAttempts={graph.ErrorHandling.RetryOptions.MaxNodeRestartAttempts}");
        }

        Console.WriteLine("=== DIAGNOSTIC: ErrorHandlingPipeline.Define() completed ===");
    }

    /// <summary>
    ///     Gets a comprehensive description of what this pipeline demonstrates and the error handling concepts it showcases.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose, error handling features, and resilience patterns.</returns>
    /// <remarks>
    ///     This description provides educational context about the error handling patterns demonstrated
    ///     in this pipeline, making it suitable for learning about resilience in data processing systems.
    /// </remarks>
    public static string GetDescription()
    {
        return @"Basic Error Handling Sample:

This sample demonstrates fundamental error handling concepts and resilience patterns in NPipeline:

ERROR HANDLING CONCEPTS:
- Try-catch patterns in pipeline nodes for local error handling
- Exponential backoff with jitter for retry logic
- Error logging and collection for observability
- Graceful degradation with fallback mechanisms
- Error isolation to prevent cascading failures
- Resilience patterns for robust pipeline execution

PIPELINE ARCHITECTURE:
The pipeline flow demonstrates a complete error handling journey:
1. UnreliableSource generates data with simulated intermittent failures
   - Simulates real-world data source reliability issues
   - Demonstrates source-level error handling and recovery

2. RetryTransform implements retry logic with exponential backoff
   - Handles transient processing failures automatically
   - Prevents thundering herd problems with jitter
   - Provides configurable retry policies

3. FallbackSink provides error logging and fallback mechanisms
   - Maintains service availability during failures
   - Implements graceful degradation patterns
   - Ensures no data loss through alternative outputs

RESILIENCE PATTERNS DEMONSTRATED:
- Retry Pattern: Automatic recovery from transient failures
- Fallback Pattern: Alternative processing paths for resilience
- Circuit Breaker Pattern: Preventing cascading failures
- Timeout Pattern: Handling resource exhaustion gracefully
- Bulkhead Pattern: Error isolation between components

IMPLEMENTATION BENEFITS:
This implementation follows the IPipelineDefinition pattern, which provides:
- Reusable pipeline definitions with comprehensive error handling
- Proper node isolation between executions to prevent error state leakage
- Type-safe node connections with error propagation control
- Clear separation of error handling logic from business logic
- Demonstrates production-ready resilience patterns for robust systems

LEARNING OUTCOMES:
By studying this sample, developers will understand:
- How to implement retry logic with exponential backoff
- How to design fallback mechanisms for graceful degradation
- How to isolate errors to prevent system-wide failures
- How to maintain observability through comprehensive error logging
- How to build resilient data processing pipelines that handle real-world failures";
    }
}
