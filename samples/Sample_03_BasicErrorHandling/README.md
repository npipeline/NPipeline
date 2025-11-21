# Sample 03: Basic Error Handling

## Overview

This sample demonstrates fundamental error handling and resilience patterns in NPipeline. It shows how to build robust pipelines that can handle failures
gracefully and continue processing data, making it an essential learning step after mastering basic pipeline concepts.

The sample introduces key error handling concepts that are critical for production-ready data processing systems, demonstrating how to maintain service
availability and data integrity even when components fail.

**Key Learning Progression:**

- Builds on concepts from Sample 01 (Basic Pipeline)
- Introduces resilience patterns for real-world scenarios
- Prepares for advanced error handling in subsequent samples

## Core Concepts

### 1. Try-Catch Patterns in Nodes

Pipeline nodes implement comprehensive exception handling to isolate failures:

```csharp
try
{
    // Primary processing logic
    await ProcessWithPrimaryOutput(message, cancellationToken);
}
catch (Exception ex)
{
    // Error handling and fallback logic
    Console.WriteLine($"Primary output failed: {ex.Message}");
    await ProcessWithFallbackOutput(message, cancellationToken);
}
```

**Benefits:**

- Prevents cascading failures across pipeline components
- Enables context-specific error handling strategies
- Maintains pipeline execution despite individual node failures

### 2. Basic Retry Logic with Exponential Backoff

The RetryTransform node demonstrates sophisticated retry patterns:

```csharp
while (attempt <= _maxRetries)
{
    try
    {
        return await ProcessWithPotentialFailure(item, cancellationToken);
    }
    catch (Exception ex) when (attempt <= _maxRetries)
    {
        // Add jitter to prevent thundering herd
        var jitter = _random.Next(0, (int)(delay.TotalMilliseconds * 0.1));
        var totalDelay = delay + TimeSpan.FromMilliseconds(jitter);

        await Task.Delay(totalDelay, cancellationToken);
        delay = TimeSpan.FromMilliseconds(delay.TotalMilliseconds * _backoffMultiplier);
    }
}
```

**Key Features:**

- Configurable retry limits and backoff strategies
- Jitter addition to prevent thundering herd problems
- Exponential delay increase to reduce system load during recovery

### 3. Error Logging and Collection

Comprehensive error tracking through the ErrorLogger utility:

```csharp
public static void LogError(ErrorSeverity severity, string component,
    string message, Exception? exception = null,
    Dictionary<string, object>? context = null)
```

**Capabilities:**

- Centralized error logging with consistent formatting
- Error severity classification (Info, Warning, Error, Critical)
- Context preservation for debugging and monitoring
- Historical error tracking with configurable retention

### 4. Graceful Degradation with Fallback Mechanisms

The FallbackSink node demonstrates service continuity during failures:

```csharp
try
{
    // Try primary output mechanism
    await ProcessWithPrimaryOutput(message, cancellationToken);
}
catch (Exception ex)
{
    // Fall back to alternative mechanism
    await ProcessWithFallbackOutput(message, cancellationToken);
}
```

**Advantages:**

- Maintains service availability during partial system failures
- Prevents data loss through alternative processing paths
- Provides configurable fallback strategies

### 5. Error Simulation Utilities

Comprehensive testing infrastructure for failure scenarios:

```csharp
// Simulate different types of failures
ErrorSimulation.SimulateNetworkFailure(resourceName, failureRate);
ErrorSimulation.SimulateDiskFailure(filePath, failureRate);
ErrorSimulation.SimulateDatabaseFailure(operation, failureRate);
```

**Features:**

- Realistic failure simulation for testing
- Configurable failure rates for different scenarios
- Multiple failure types (network, disk, database)
- Metrics collection for error handling effectiveness

## Pipeline Architecture

### Component Overview

The error handling pipeline implements a three-stage flow with comprehensive resilience:

```
UnreliableSource → RetryTransform → FallbackSink
```

### UnreliableSource

**Purpose:** Simulates intermittent data source failures with configurable failure rates.

**Key Features:**

- Random failure simulation (30% default failure rate)
- Per-message failure simulation for granular testing
- Configurable failure rates for different test scenarios
- Realistic error messages for debugging

**Error Handling:**

- Throws `InvalidOperationException` to simulate connection failures
- Provides detailed error context for troubleshooting
- Supports failure rate configuration for testing

### RetryTransform

**Purpose:** Implements retry logic with exponential backoff and jitter for transient failures.

**Key Features:**

- Configurable retry limits (3 retries by default)
- Exponential backoff with multiplier (2.0x by default)
- Jitter addition (10% of delay) to prevent thundering herd
- Configurable initial delay (100ms by default)

**Error Recovery Flow:**

1. Attempt primary processing
2. On failure, calculate delay with jitter
3. Wait for calculated delay
4. Increase delay exponentially
5. Repeat until max retries reached
6. Throw final exception if all retries fail

### FallbackSink

**Purpose:** Provides graceful degradation through primary and fallback output mechanisms.

**Key Features:**

- Primary output with configurable failure rate (30% by default)
- Automatic fallback to console output on primary failure
- Statistics tracking for success/failure rates
- Configurable fallback-only mode for testing

**Fallback Strategy:**

1. Attempt primary output (e.g., database, file system)
2. On failure, switch to fallback output (console)
3. Track statistics for monitoring
4. Report failed messages that couldn't be processed

### Data Flow and Error Recovery

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│  UnreliableSource│───▶│  RetryTransform  │───▶│   FallbackSink  │
│                 │    │                  │    │                 │
│ • Data Gen      │    │ • Retry Logic    │    │ • Primary Out   │
│ • Fail Sim      │    │ • Backoff        │    │ • Fallback Out  │
│ • Error Prop    │    │ • Jitter         │    │ • Statistics    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
        │                       │                       │
        ▼                       ▼                       ▼
   Source Failures        Retry Attempts        Output Failures
   - Connection Errors     - Transient Issues    - Primary Output
   - Data Gen Errors      - Resource Exhaustion  - Fallback Activation
   - Timeout Sim          - External Service     - Error Logging
```

## Quick Setup and Run

### Prerequisites

- .NET 8.0, .NET 9.0 or .NET 10.0 SDK
- JetBrains Rider, Visual Studio 2022, VS Code, or .NET CLI

### Running the Sample

```bash
cd samples/Sample_03_BasicErrorHandling
dotnet restore
dotnet run
```

### Expected Output

The sample demonstrates various error handling scenarios with detailed logging:

```
=== NPipeline Sample: Basic Error Handling ===

Registered NPipeline services and scanned assemblies for nodes.

Pipeline Description:
Basic Error Handling Sample:

This sample demonstrates fundamental error handling concepts and resilience patterns in NPipeline:

ERROR HANDLING CONCEPTS:
- Try-catch patterns in pipeline nodes for local error handling
- Exponential backoff with jitter for retry logic
- Error logging and collection for observability
- Graceful degradation with fallback mechanisms
- Error isolation to prevent cascading failures
- Resilience patterns for robust pipeline execution

Starting pipeline execution...

UnreliableSource: Starting to generate messages
UnreliableSource: Successfully generated 10 messages
RetryTransform: Processing item: Message #1 from unreliable source
RetryTransform: Successfully processed item: Message #1 from unreliable source
[PRIMARY OUTPUT] Processed: [RETRY_PROCESSED] Message #1 from unreliable source
RetryTransform: Processing item: Message #2 from unreliable source
RetryTransform: Attempt 1 failed for item 'Message #2 from unreliable source': Simulated processing failure for item: Message #2 from unreliable source
RetryTransform: Retrying in 110ms (attempt 1/3)
RetryTransform: Success on attempt 2 for item: Message #2 from unreliable source
[PRIMARY OUTPUT] Processed: [RETRY_PROCESSED] Message #2 from unreliable source
FallbackSink: Primary output failed for message '[RETRY_PROCESSED] Message #3 from unreliable source': Simulated primary output failure for message: [RETRY_PROCESSED] Message #3 from unreliable source
FallbackSink: Successfully used fallback output for message: [RETRY_PROCESSED] Message #3 from unreliable source
[FALLBACK OUTPUT] [RETRY_PROCESSED] Message #3 from unreliable source
...
FallbackSink: Processing complete. Total: 10, Primary Success: 7, Fallback Used: 3

Pipeline execution completed successfully!
```

## Key Learning Points

### What Developers Should Learn

1. **Error Isolation Patterns**
    - How to prevent cascading failures in pipeline systems
    - Techniques for containing errors within individual components
    - Strategies for maintaining system stability during partial failures

2. **Retry Logic Implementation**
    - Exponential backoff algorithms for transient failures
    - Jitter addition to prevent thundering herd problems
    - Configurable retry policies for different failure scenarios

3. **Graceful Degradation Strategies**
    - Fallback mechanism design for service continuity
    - Alternative processing paths for critical operations
    - Data preservation during system failures

4. **Observability and Monitoring**
    - Comprehensive error logging with context preservation
    - Metrics collection for error handling effectiveness
    - Statistical analysis of failure patterns

### Best Practices Demonstrated

1. **Structured Error Handling**
   ```csharp
   try
   {
       // Primary operation
   }
   catch (SpecificException ex)
   {
       // Handle specific error types
   }
   catch (Exception ex)
   {
       // Fallback for unexpected errors
   }
   ```

2. **Retry with Backoff**
   ```csharp
   var delay = initialDelay;
   for (int attempt = 1; attempt <= maxRetries; attempt++)
   {
       try { /* operation */ }
       catch
       {
           var jitter = random.Next(0, (int)(delay.TotalMilliseconds * 0.1));
           await Task.Delay(delay + TimeSpan.FromMilliseconds(jitter));
           delay = TimeSpan.FromMilliseconds(delay.TotalMilliseconds * backoffMultiplier);
       }
   }
   ```

3. **Fallback Mechanisms**
   ```csharp
   try
   {
       await PrimaryOperation();
   }
   catch
   {
       await FallbackOperation();
   }
   ```

### Applying These Concepts in Real Scenarios

1. **Microservice Communication**
    - Implement retry logic for service-to-service calls
    - Use circuit breakers to prevent cascading failures
    - Apply fallback mechanisms for service degradation

2. **Database Operations**
    - Handle transient connection failures with retries
    - Implement fallback to read replicas during outages
    - Use bulkhead patterns to isolate database failures

3. **External API Integration**
    - Apply exponential backoff for rate-limited APIs
    - Implement fallback responses for service unavailability
    - Cache responses to reduce dependency on external services

4. **File Processing Systems**
    - Handle file I/O failures with retry mechanisms
    - Implement alternative storage locations for critical data
    - Use dead-letter queues for failed processing attempts

By mastering these error handling patterns, developers can build robust, resilient data processing pipelines that maintain service availability and data
integrity even in the face of failures.
