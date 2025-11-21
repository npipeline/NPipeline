using NPipeline.Pipeline;
using Sample_06_AdvancedErrorHandling.Nodes;

namespace Sample_06_AdvancedErrorHandling;

/// <summary>
///     Advanced error handling pipeline demonstrating production-grade resilience patterns.
///     This pipeline implements a comprehensive error handling flow:
///     1. UnreliableDataSource generates data with intermittent failures
///     2. RetryTransform implements advanced retry strategies with Polly
///     3. CircuitBreakerTransform implements circuit breaker patterns
///     4. MonitoringTransform tracks error rates and alerts on thresholds
///     5. DeadLetterQueueSink captures failed items for later processing
/// </summary>
/// <remarks>
///     This implementation follows the IPipelineDefinition pattern and demonstrates:
///     - Circuit breaker patterns with Polly integration
///     - Dead letter queue implementation for failed items
///     - Custom error policies and recovery mechanisms
///     - Monitoring and alerting for error rates
///     - Advanced retry strategies with exponential backoff
/// </remarks>
public class AdvancedErrorHandlingPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a resilient pipeline flow with error handling:
    ///     UnreliableDataSource -> RetryTransform -> CircuitBreakerTransform -> MonitoringTransform -> DeadLetterQueueSink
    ///     The pipeline processes data through these stages:
    ///     1. Source generates data with simulated failures
    ///     2. Retry transform attempts to recover from transient failures
    ///     3. Circuit breaker prevents cascading failures
    ///     4. Monitoring tracks error rates and triggers alerts
    ///     5. Dead letter queue captures items that couldn't be processed
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add the source node that generates data with intermittent failures
        var source = builder.AddSource<UnreliableDataSource, SourceData>("unreliable-data-source");

        // Add the retry transform that implements advanced retry strategies with Polly
        var retry = builder.AddTransform<RetryTransform, SourceData, SourceData>("retry-transform");

        // Add the circuit breaker transform that implements circuit breaker patterns
        var circuitBreaker = builder.AddTransform<CircuitBreakerTransform, SourceData, SourceData>("circuit-breaker-transform");

        // Add the monitoring transform that tracks error rates and alerts on thresholds
        var monitoring = builder.AddTransform<MonitoringTransform, SourceData, SourceData>("monitoring-transform");

        // Add the dead letter queue sink that captures failed items for later processing
        var deadLetterSink = builder.AddSink<DeadLetterQueueSink, SourceData>("dead-letter-queue-sink");

        // Connect the nodes in a linear flow with error handling
        builder.Connect(source, retry);
        builder.Connect(retry, circuitBreaker);
        builder.Connect(circuitBreaker, monitoring);
        builder.Connect(monitoring, deadLetterSink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"Advanced Error Handling Pipeline Sample:

This sample demonstrates production-grade resilience patterns in NPipeline:
- Circuit breaker patterns with Polly integration
- Dead letter queue implementation for failed items
- Comprehensive retry strategies with exponential backoff
- Error recovery mechanisms and monitoring
- Custom error policies and alerting

The pipeline flow:
1. UnreliableDataSource generates data with intermittent failures
2. RetryTransform implements advanced retry strategies with Polly
3. CircuitBreakerTransform prevents cascading failures
4. MonitoringTransform tracks error rates and alerts on thresholds
5. DeadLetterQueueSink captures failed items for later processing

This implementation demonstrates:
- Polly integration for resilience patterns
- Circuit breaker configuration and monitoring
- Dead letter queue for failed item handling
- Error rate monitoring and alerting
- Advanced retry strategies with backoff policies";
    }
}
