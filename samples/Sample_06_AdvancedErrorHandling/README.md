# Sample 6: Advanced Error Handling

## Overview

This sample demonstrates production-grade resilience patterns in NPipeline, including circuit breaker patterns, dead letter queues, comprehensive retry
strategies, and error recovery mechanisms using Polly.

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
cd samples/Sample_06_AdvancedErrorHandling
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
