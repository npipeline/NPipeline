# Sample 6: Advanced Error Handling

## Overview

This sample shows that a resilience library you already use still composes with NPipeline. Two transform nodes use
Polly inside `TransformAsync`: one retries with exponential backoff, and one wraps its work in a circuit breaker.
The sample also covers dead letter queues and error-rate monitoring.

NPipeline's own resilience features, such as item retry and circuit breakers, don't need Polly. For those, see
[Error handling](../../docs/error-handling/index.md). For the recommended way to retry calls to an external service,
see [`Sample_EdgeResilience`](../Sample_EdgeResilience/README.md).

## Use your own resilience library inside a node

When a node retries its own calls, turn the pipeline's item retry (L1) off for that node. Otherwise, a failure that
escapes the library's retries is retried again by the pipeline, and the two layers multiply each other's attempts.
The Default optimization profile retries transient failures three times, so this applies even when you haven't
configured retries yourself.

[`AdvancedErrorHandlingPipeline.cs`](AdvancedErrorHandlingPipeline.cs) does this for both Polly nodes:

```csharp
builder.WithResilience(retry, options => options with { ItemRetry = ItemRetryOptions.None });
builder.WithResilience(circuitBreaker, options => options with { ItemRetry = ItemRetryOptions.None });
```

In this sample, the Polly nodes also catch every exception and pass the item on, so no failure reaches the pipeline.
For more information about how the layers compose, see
[The three resilience layers](../../docs/error-handling/three-layers.md).

## Key Concepts

1. **Circuit Breaker Patterns** - Prevent cascading failures when error thresholds are exceeded
2. **Dead Letter Queues** - Capture failed items for later processing and analysis
3. **Advanced Retry Strategies** - Implement exponential backoff and intelligent retry logic
4. **Error Recovery Mechanisms** - Monitor error rates and implement recovery strategies
5. **Monitoring and Alerting** - Track pipeline health and trigger alerts on thresholds

## Quick Setup and Run

### Prerequisites

- .NET 8.0, .NET 9.0 or .NET 10.0 SDK
- JetBrains Rider, Visual Studio 2022, VS Code, or .NET CLI

### Running the Sample

```bash
cd samples/Sample_AdvancedErrorHandling
dotnet restore
dotnet run
```

## Pipeline Flow

1. **UnreliableDataSource** - Generates data with intermittent failures
2. **RetryTransform** - Implements advanced retry strategies with Polly
3. **CircuitBreakerTransform** - Prevents cascading failures
4. **MonitoringTransform** - Tracks error rates and alerts on thresholds
5. **DeadLetterQueueSink** - Captures failed items for later processing

## Key Features Demonstrated

- Polly integration for resilience patterns
- Circuit breaker configuration and monitoring
- Dead letter queue for failed item handling
- Error rate monitoring and alerting
- Advanced retry strategies with backoff policies

## Expected Output

The sample will show:

- Retry attempts with exponential backoff
- Circuit breaker state changes (Closed, Open, Half-Open)
- Error rate monitoring with alerts at different thresholds
- Dead letter queue processing and retry options
- Comprehensive error handling throughout the pipeline
