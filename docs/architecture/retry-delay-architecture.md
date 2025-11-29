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
        E[BackoffStrategy<br>Delegate Type] --> F[BackoffStrategies.ExponentialBackoff()]
        E --> G[BackoffStrategies.LinearBackoff()]
        E --> H[BackoffStrategies.FixedDelay()]
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

Jitter strategies add randomness to backoff delays to prevent synchronized retries. The implementation has been simplified to use delegate-based strategies instead of interface-based classes.

```mermaid
graph TD
    subgraph "Jitter Strategy Configuration"
        A[JitterStrategyConfiguration<br>Interface] --> B[FullJitterConfiguration]
        A --> C[EqualJitterConfiguration]
        A --> D[DecorrelatedJitterConfiguration]
        A --> E[NoJitterConfiguration]
    end
    
    subgraph "Jitter Strategy Implementation"
        F[JitterStrategy<br>Delegate Type] --> G[JitterStrategies.FullJitter()]
        F --> H[JitterStrategies.EqualJitter()]
        F --> I[JitterStrategies.DecorrelatedJitter()]
        F --> J[JitterStrategies.NoJitter()]
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
BackoffStrategy (Delegate)
├── BackoffStrategies.ExponentialBackoff()
├── BackoffStrategies.LinearBackoff()
└── BackoffStrategies.FixedDelay()

JitterStrategy (Delegate)
├── JitterStrategies.FullJitter()
├── JitterStrategies.EqualJitter()
├── JitterStrategies.DecorrelatedJitter()
└── JitterStrategies.NoJitter()
```

These implement the mathematical formulas for delay calculation. Both backoff and jitter strategies are now implemented as static methods that return delegates, providing a more streamlined API while maintaining the same functionality.

### 4. Composite Strategy

```
IRetryDelayStrategy
└── CompositeRetryDelayStrategy
    ├── Combines: BackoffStrategy (delegate)
    ├── Combines: JitterStrategy (delegate)
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
// Backoff strategy delegate type
public delegate TimeSpan BackoffStrategy(int attemptNumber);

// Static factory methods for creating strategies
public static class BackoffStrategies
{
    public static BackoffStrategy ExponentialBackoff(TimeSpan baseDelay, double multiplier = 2.0, TimeSpan? maxDelay = null);
    public static BackoffStrategy LinearBackoff(TimeSpan baseDelay, TimeSpan? increment = null, TimeSpan? maxDelay = null);
    public static BackoffStrategy FixedDelay(TimeSpan delay);
}

// Jitter strategy delegate type
public delegate TimeSpan JitterStrategy(TimeSpan baseDelay, Random random);

// Static factory methods for creating jitter strategies
public static class JitterStrategies
{
    public static JitterStrategy FullJitter();
    public static JitterStrategy EqualJitter();
    public static JitterStrategy DecorrelatedJitter();
    public static JitterStrategy NoJitter();
}
```

### Composite Pattern

Combines backoff and jitter into a single strategy:

```csharp
public class CompositeRetryDelayStrategy : IRetryDelayStrategy
{
    private readonly BackoffStrategy _backoff;
    private readonly JitterStrategy _jitter;
    
    public async ValueTask<TimeSpan> GetDelayAsync(int attemptNumber)
    {
        // Get base delay from backoff strategy
        var baseDelay = _backoff(attemptNumber);
        
        // Apply jitter if present
        if (_jitter != null)
            return _jitter(baseDelay, _random);
        
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
        JitterStrategy jitterStrategy = null)
    {
        config.Validate();
        var backoff = BackoffStrategies.ExponentialBackoff(
            config.BaseDelay, 
            config.Multiplier, 
            config.MaxDelay);
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

// Implementation - uses configuration with delegate
var backoff = BackoffStrategies.ExponentialBackoff(
    config.BaseDelay, 
    config.Multiplier, 
    config.MaxDelay);
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
   ├── Create backoff strategy delegate
   ├── Create jitter strategy delegate
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
├─ BackoffStrategy(attemptNumber) (synchronous delegate)
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
│  └─ Specific validation rules
└── JitterStrategyConfiguration.Validate()
   └─ Specific validation rules
```

**Validation Timing:**
- Configuration creation: Optional (deferred)
- Factory creation: Mandatory (immediate)
- Strategy usage: Pre-validated

## Performance Considerations

### Memory Usage

- **Configurations**: Immutable, small (few properties)
- **Strategies**: Stateless delegates - minimal memory footprint
- **Factory**: Singleton pattern recommended
- **PipelineContext**: Caches single strategy per context

### CPU Usage

- **Delay calculation**: O(1) - simple arithmetic
- **Validation**: One-time cost at factory creation
- **Random generation**: Minimal overhead, only if jitter enabled
- **Async overhead**: Minimal for synchronous paths with ValueTask

### Thread Safety

- **Configurations**: Immutable - fully thread-safe
- **Strategies**: Stateless delegates - thread-safe
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

// Create a custom backoff delegate
BackoffStrategy customBackoff = (attemptNumber) =>
{
    // Your delay calculation logic
    return TimeSpan.FromSeconds(Math.Pow(2, attemptNumber));
};

// Extend factory
public class ExtendedFactory : DefaultRetryDelayStrategyFactory
{
    public IRetryDelayStrategy CreateCustomBackoff(
        CustomBackoffConfiguration config)
    {
        config.Validate();
        var backoff = BackoffStrategies.ExponentialBackoff(
            config.BaseDelay, 
            config.Multiplier, 
            config.MaxDelay);
        return new CompositeRetryDelayStrategy(backoff, null);
    }
}
```

### Adding Custom Jitter Strategy

```csharp
// Create a custom jitter delegate
JitterStrategy customJitter = (baseDelay, random) =>
{
    // Your custom jitter calculation logic
    var jitterMs = random.NextDouble() * baseDelay.TotalMilliseconds * 0.1;
    return TimeSpan.FromMilliseconds(jitterMs);
};

// Use with configuration
var config = new CustomJitterConfiguration();
var jitter = JitterStrategies.Custom(config);

// Or use directly with backoff
var backoff = BackoffStrategies.ExponentialBackoff(
    TimeSpan.FromSeconds(1), 
    2.0, 
    TimeSpan.FromMinutes(1));
var composite = new CompositeRetryDelayStrategy(backoff, customJitter);
```

For configuration-based custom jitter:

```csharp
public class CustomJitterConfiguration : JitterStrategyConfiguration
{
    public double JitterFactor { get; init; } = 0.1;
    
    public override void Validate()
    {
        if (JitterFactor < 0 || JitterFactor > 1.0)
            throw new ArgumentException("JitterFactor must be between 0 and 1.0");
    }
}

// Extend JitterStrategies with a static method
public static class JitterStrategies
{
    public static JitterStrategy Custom(CustomJitterConfiguration config)
    {
        config.Validate();
        return (baseDelay, random) =>
        {
            var jitterMs = random.NextDouble() * baseDelay.TotalMilliseconds * config.JitterFactor;
            return TimeSpan.FromMilliseconds(jitterMs);
        };
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
public void ExponentialBackoff_WithValidParameters_CalculatesCorrectly()
{
    var backoff = BackoffStrategies.ExponentialBackoff(
        TimeSpan.FromSeconds(1), 
        2.0, 
        TimeSpan.FromMinutes(1));
    
    Assert.Equal(TimeSpan.FromSeconds(1), backoff(0));
    Assert.Equal(TimeSpan.FromSeconds(2), backoff(1));
    Assert.Equal(TimeSpan.FromSeconds(4), backoff(2));
}
```

### Integration Testing

```csharp
[Fact]
public async Task CompositeStrategy_WithBackoffAndJitter_WorksTogether()
{
    var backoff = BackoffStrategies.ExponentialBackoff(
        TimeSpan.FromSeconds(1), 
        2.0, 
        TimeSpan.FromMinutes(1));
    var jitter = JitterStrategies.FullJitter();
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
        null);
    
    Assert.IsType<CompositeRetryDelayStrategy>(strategy);
}
```

## Conclusion

NPipeline's retry delay architecture provides:

- **Separation of Concerns**: Backoff and jitter are independent
- **Flexibility**: Easy to add new strategies through delegates
- **Performance**: Minimal overhead with caching and stateless delegates
- **Testability**: Each component can be tested independently
- **Observability**: Clear design makes debugging easier
- **Extensibility**: Custom strategies can be added as delegates

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
