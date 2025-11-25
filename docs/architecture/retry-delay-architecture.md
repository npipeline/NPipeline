---
title: Retry Delay Strategies Architecture
description: How NPipeline's retry delay system works internally, including design patterns, components, and architectural decisions.
sidebar_position: 8
---

# Retry Delay Strategies Architecture

## Overview

NPipeline's retry delay system is built on a modular architecture that separates concerns into distinct components. This guide explains the design, components, and how they work together.

## Table of Contents

- [Architecture Components](#architecture-components)
- [Design Patterns Used](#design-patterns-used)
- [Data Flow](#data-flow)
- [Validation Architecture](#validation-architecture)
- [Performance Considerations](#performance-considerations)
- [Extensibility](#extensibility)
- [Integration Points](#integration-points)
- [Testing Architecture](#testing-architecture)

## Architecture Components

### 1. Backoff Strategies

Backoff strategies determine how delays increase between retry attempts.

```mermaid
graph TD
    subgraph "Backoff Strategy Configuration"
        A[BackoffStrategyConfiguration<br>Interface] --> B[ExponentialBackoffConfiguration]
        A --> C[LinearBackoffConfiguration]
        A --> D[FixedDelayConfiguration]
    end
    
    subgraph "Backoff Strategy Implementation"
        E[IBackoffStrategy<br>Interface] --> F[ExponentialBackoffStrategy]
        E --> G[LinearBackoffStrategy]
        E --> H[FixedDelayStrategy]
    end
    
    B --> I[Validates parameters<br>Creates immutable config]
    C --> I
    D --> I
    
    F --> J[Calculates: baseDelay × multiplier^attempt<br>Capped at maxDelay]
    G --> K[Calculates: baseDelay + (increment × attempt)<br>Capped at maxDelay]
    H --> L[Returns: constant delay for all attempts]
    
    style A fill:#e1f5fe
    style E fill:#e1f5fe
    style B fill:#f3e5f5
    style C fill:#f3e5f5
    style D fill:#f3e5f5
    style F fill:#e8f5e9
    style G fill:#e8f5e9
    style H fill:#e8f5e9
    style I fill:#fff3e0
    style J fill:#e3f2fd
    style K fill:#e3f2fd
    style L fill:#e3f2fd
```

Each configuration:
- Defines parameters for delay calculation
- Implements validation to ensure valid parameters
- Can be serialized/deserialized for persistence

**Key Characteristics:**
- Immutable (uses `init` properties)
- Validated on creation
- Deterministic (same input produces same output)

### 2. Jitter Strategies

Jitter strategies add randomness to backoff delays to prevent synchronized retries.

```mermaid
graph TD
    subgraph "Jitter Strategy Configuration"
        A[JitterStrategyConfiguration<br>Interface] --> B[FullJitterConfiguration]
        A --> C[EqualJitterConfiguration]
        A --> D[DecorrelatedJitterConfiguration]
        A --> E[NoJitterConfiguration]
    end
    
    subgraph "Jitter Strategy Implementation"
        F[IJitterStrategy<br>Interface] --> G[FullJitterStrategy]
        F --> H[EqualJitterStrategy]
        F --> I[DecorrelatedJitterStrategy]
        F --> J[NoJitterStrategy]
    end
    
    subgraph "Jitter Application"
        K[Base Delay from Backoff] --> L{Jitter Strategy}
        L -->|Full Jitter|M[random(0, baseDelay)]
        L -->|Equal Jitter|N[baseDelay/2 + random(0, baseDelay/2)]
        L -->|Decorrelated Jitter|O[random(baseDelay, min(maxDelay, previousDelay × multiplier))]
        L -->|No Jitter|P[Return baseDelay unchanged]
    end
    
    style A fill:#e1f5fe
    style F fill:#e1f5fe
    style B fill:#f3e5f5
    style C fill:#f3e5f5
    style D fill:#f3e5f5
    style E fill:#f3e5f5
    style G fill:#e8f5e9
    style H fill:#e8f5e9
    style I fill:#e8f5e9
    style J fill:#e8f5e9
    style K fill:#fff3e0
    style L fill:#ffecb3
    style M fill:#e3f2fd
    style N fill:#e3f2fd
    style O fill:#e3f2fd
    style P fill:#e3f2fd
```

**Benefits of Jitter:**
- Prevents "thundering herd" problem
- Spreads retry load over time
- Improves system stability

### 3. Strategy Implementation

The actual retry delay calculation is performed by strategy implementations:

```
IBackoffStrategy
├── ExponentialBackoffStrategy
├── LinearBackoffStrategy
└── FixedDelayStrategy

IJitterStrategy
├── FullJitterStrategy
├── EqualJitterStrategy
├── DecorrelatedJitterStrategy
└── NoJitterStrategy
```

These implement the mathematical formulas for delay calculation.

### 4. Composite Strategy

```
IRetryDelayStrategy
└── CompositeRetryDelayStrategy
    ├── Combines: IBackoffStrategy
    ├── Combines: IJitterStrategy (optional)
    └── Handles: Cancellation, async execution
```

The composite strategy:
- Orchestrates backoff and jitter
- Manages async execution with `ValueTask`
- Respects cancellation tokens
- Caches state for stateful jitter (e.g., decorrelated)

### 5. Factory

```
DefaultRetryDelayStrategyFactory
├── Creates individual strategies
├── Validates configurations
└── Combines strategies appropriately
```

The factory:
- Handles configuration validation
- Creates appropriate strategy instances
- Manages strategy lifecycle
- Provides centralized creation logic

### 6. PipelineContext Integration

```
PipelineContextRetryDelayExtensions
├── GetRetryDelayStrategy() - retrieves/caches strategy
├── GetRetryDelayAsync() - gets delay for attempt
├── UseExponentialBackoffDelay() - runtime config
├── UseLinearBackoffDelay() - runtime config
└── UseExponentialBackoffWithJitter() - runtime config
```

**Integration Features:**
- Caches strategies to avoid recreation
- Supports runtime configuration
- Integrates with resilient execution strategy
- Provides convenient API for common patterns

## Design Patterns Used

### Strategy Pattern

Each backoff/jitter type implements a different strategy:

```csharp
// Strategy interface
public interface IBackoffStrategy
{
    TimeSpan CalculateDelay(int attemptNumber);
}

// Different implementations
public class ExponentialBackoffStrategy : IBackoffStrategy { }
public class LinearBackoffStrategy : IBackoffStrategy { }
public class FixedDelayStrategy : IBackoffStrategy { }
```

### Composite Pattern

Combines backoff and jitter into a single strategy:

```csharp
public class CompositeRetryDelayStrategy : IRetryDelayStrategy
{
    private readonly IBackoffStrategy _backoff;
    private readonly IJitterStrategy _jitter;
    
    public async ValueTask<TimeSpan> GetDelayAsync(int attemptNumber)
    {
        // Get base delay from backoff strategy
        var baseDelay = _backoff.CalculateDelay(attemptNumber);
        
        // Apply jitter if present
        if (_jitter != null)
            return _jitter.ApplyJitter(baseDelay, _random);
        
        return baseDelay;
    }
}
```

### Factory Pattern

Creates strategies from configurations:

```csharp
public class DefaultRetryDelayStrategyFactory
{
    public IRetryDelayStrategy CreateExponentialBackoff(
        ExponentialBackoffConfiguration config,
        IJitterStrategy jitterStrategy = null)
    {
        config.Validate();
        var backoff = new ExponentialBackoffStrategy(config);
        return new CompositeRetryDelayStrategy(backoff, jitterStrategy);
    }
}
```

### Configuration Pattern

Separates configuration from implementation:

```csharp
// Configuration - data only
public class ExponentialBackoffConfiguration
{
    public TimeSpan BaseDelay { get; init; }
    public double Multiplier { get; init; }
    public TimeSpan MaxDelay { get; init; }
}

// Implementation - uses configuration
public class ExponentialBackoffStrategy
{
    private readonly ExponentialBackoffConfiguration _config;
    
    public TimeSpan CalculateDelay(int attempt)
    {
        return TimeSpan.FromMilliseconds(
            Math.Min(
                _config.BaseDelay.TotalMilliseconds * 
                Math.Pow(_config.Multiplier, attempt),
                _config.MaxDelay.TotalMilliseconds));
    }
}
```

## Data Flow

### Configuration to Execution

```
1. PipelineRetryOptions
   └── RetryDelayStrategyConfiguration
       ├── BackoffStrategyConfiguration
       └── JitterStrategyConfiguration

2. Factory.CreateStrategy(configuration)
   ├── Validate configurations
   ├── Create backoff strategy
   ├── Create jitter strategy
   └── Return composite strategy

3. PipelineContext.GetRetryDelayStrategy()
   ├── Check cache
   ├── If not cached:
   │   └── factory.CreateStrategy()
   └── Return strategy

4. ResilientExecutionStrategy
   └── On failure:
       ├── Get strategy from context
       ├── Calculate delay for attempt
       ├── Wait for delay
       └── Retry operation
```

### Async Execution Flow

```
async Task<TimeSpan> GetDelayAsync(attemptNumber)
│
├─ BackoffStrategy.CalculateDelay(synchronous)
│  └─ Returns base delay
│
├─ JitterStrategy.ApplyJitter (may be async)
│  └─ Adds randomness
│
└─ Return combined delay
```

## Validation Architecture

Each configuration component validates independently:

```
RetryDelayStrategyConfiguration
├── Validates self
├── BackoffStrategyConfiguration.Validate()
│  └── Specific validation rules
└── JitterStrategyConfiguration.Validate()
   └── Specific validation rules
```

**Validation Timing:**
- Configuration creation: Optional (deferred)
- Factory creation: Mandatory (immediate)
- Strategy usage: Pre-validated

## Performance Considerations

### Memory Usage

- **Configurations**: Immutable, small (few properties)
- **Strategies**: Stateless except decorrelated jitter (previous delay)
- **Factory**: Singleton pattern recommended
- **PipelineContext**: Caches single strategy per context

### CPU Usage

- **Delay calculation**: O(1) - simple arithmetic
- **Validation**: One-time cost at factory creation
- **Random generation**: Minimal overhead, only if jitter enabled
- **Async overhead**: Minimal for synchronous paths with ValueTask

### Thread Safety

- **Configurations**: Immutable - fully thread-safe
- **Strategies**: Stateless - thread-safe
- **Decorrelated Jitter**: Thread-safe with proper locking
- **Factory**: Thread-safe (pure function pattern)

## Extensibility

### Adding Custom Backoff Strategy

```csharp
public class CustomBackoffConfiguration : BackoffStrategyConfiguration
{
    public override void Validate()
    {
        // Your validation logic
    }
}

public class CustomBackoffStrategy : IBackoffStrategy
{
    public TimeSpan CalculateDelay(int attemptNumber)
    {
        // Your delay calculation logic
    }
}

// Extend factory
public class ExtendedFactory : DefaultRetryDelayStrategyFactory
{
    public IRetryDelayStrategy CreateCustomBackoff(
        CustomBackoffConfiguration config)
    {
        config.Validate();
        var backoff = new CustomBackoffStrategy(config);
        return new CompositeRetryDelayStrategy(backoff, null);
    }
}
```

### Adding Custom Jitter Strategy

```csharp
public class CustomJitterConfiguration : JitterStrategyConfiguration
{
    public override void Validate() { }
}

public class CustomJitterStrategy : IJitterStrategy
{
    public TimeSpan ApplyJitter(TimeSpan baseDelay, Random random)
    {
        // Your jitter calculation logic
    }
}
```

## Integration Points

### With ResilientExecutionStrategy

```csharp
public class ResilientExecutionStrategy
{
    public async Task ExecuteAsync(/* ... */)
    {
        for (int attempt = 0; attempt <= maxRetries; attempt++)
        {
            try
            {
                return await Execute();
            }
            catch when (attempt < maxRetries)
            {
                // Integration point: Get delay from context
                var strategy = context.GetRetryDelayStrategy();
                var delay = await strategy.GetDelayAsync(attempt);
                
                // Wait respecting cancellation
                await Task.Delay(delay, cancellationToken);
            }
        }
    }
}
```

### With PipelineContext

```csharp
public class PipelineContext
{
    private Dictionary<string, object> _items;
    
    public IRetryDelayStrategy GetRetryDelayStrategy()
    {
        // Cache key: "NPipeline.RetryDelayStrategy"
        if (!_items.TryGetValue(key, out var cached))
        {
            var factory = new DefaultRetryDelayStrategyFactory();
            cached = factory.CreateStrategy(
                RetryOptions.DelayStrategyConfiguration);
            _items[key] = cached;
        }
        
        return (IRetryDelayStrategy)cached;
    }
}
```

## Testing Architecture

### Unit Testing Strategies

```csharp
[Fact]
public void ExponentialBackoff_WithValidConfig_CalculatesCorrectly()
{
    var config = new ExponentialBackoffConfiguration(...);
    var strategy = new ExponentialBackoffStrategy(config);
    
    Assert.Equal(TimeSpan.FromSeconds(1), strategy.CalculateDelay(0));
    Assert.Equal(TimeSpan.FromSeconds(2), strategy.CalculateDelay(1));
    Assert.Equal(TimeSpan.FromSeconds(4), strategy.CalculateDelay(2));
}
```

### Integration Testing

```csharp
[Fact]
public async Task CompositeStrategy_WithBackoffAndJitter_WorksTogether()
{
    var backoff = new ExponentialBackoffStrategy(...);
    var jitter = new FullJitterStrategy(...);
    var composite = new CompositeRetryDelayStrategy(backoff, jitter);
    
    var delay = await composite.GetDelayAsync(1);
    
    Assert.InRange(delay, TimeSpan.Zero, TimeSpan.FromSeconds(2));
}
```

### Factory Testing

```csharp
[Fact]
public void Factory_CreatesCorrectStrategies()
{
    var factory = new DefaultRetryDelayStrategyFactory();
    
    var strategy = factory.CreateExponentialBackoff(
        new ExponentialBackoffConfiguration(),
        new FullJitterConfiguration());
    
    Assert.IsType<CompositeRetryDelayStrategy>(strategy);
}
```

## Conclusion

NPipeline's retry delay architecture provides:

- **Separation of Concerns**: Backoff and jitter are independent
- **Flexibility**: Easy to add new strategies
- **Performance**: Minimal overhead with caching
- **Testability**: Each component can be tested independently
- **Observability**: Clear design makes debugging easier
- **Extensibility**: Custom strategies can be added

This architecture enables robust, configurable retry behavior for resilient data pipelines.

## See Also

- **[Advanced Retry Delay Strategies](../advanced-topics/retry-delay-advanced.md)**: Advanced patterns and scenarios for using retry delay strategies in production environments
- **[Retry Configuration](../core-concepts/resilience/retry-configuration.md)**: Basic retry configuration options and built-in strategies
- **[Resilience Overview](../core-concepts/resilience/index.md)**: Comprehensive guide to building fault-tolerant pipelines
- **[Component Architecture](component-architecture.md)**: Overview of major NPipeline system components and their interactions
- **[Execution Flow](execution-flow.md)**: How pipelines execute data and flow through the system
- **[Error Handling Architecture](error-handling-architecture.md)**: Error propagation and handling mechanisms in NPipeline

## Related Topics

- **[Design Principles](design-principles.md)**: Core philosophy behind NPipeline's design including separation of concerns and composability
- **[Performance Characteristics](performance-characteristics.md)**: Understanding performance implications of retry strategies and other architectural decisions
- **[Extension Points](extension-points.md)**: How to extend NPipeline functionality including custom retry strategies
- **[Optimization Principles](optimization-principles.md)**: Performance optimizations that influence retry delay system design
