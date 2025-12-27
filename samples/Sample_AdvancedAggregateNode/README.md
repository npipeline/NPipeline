# Sample_AdvancedAggregateNode

This sample demonstrates advanced financial risk analysis scenarios using NPipeline's `AdvancedAggregateNode`. It showcases sophisticated aggregation patterns
where accumulator and result types differ, enabling complex statistical calculations for real-time risk monitoring.

## Overview

The sample implements a comprehensive financial risk analysis pipeline that processes simulated trading data and calculates various risk metrics including
volatility, Value at Risk (VaR), and portfolio analytics. It demonstrates the power of `AdvancedAggregateNode` for scenarios requiring complex intermediate
state management.

## Key Concepts Demonstrated

### AdvancedAggregateNode Features

- **Separate Accumulator and Result Types**: Unlike `AggregateNode`, `AdvancedAggregateNode<TIn, TKey, TAccumulate, TResult>` allows different types for
  intermediate accumulation and final results
- **Complex Accumulator Patterns**: Uses tuples, dictionaries, and collections for sophisticated state management
- **Statistical Calculations**: Implements variance, standard deviation, percentiles, and weighted averages
- **Financial Risk Metrics**: Calculates volatility, VaR, Expected Shortfall, and Sharpe ratios
- **Window Strategy Flexibility**: Demonstrates both tumbling and sliding windows for different use cases

### Financial Risk Analysis

- **Volatility Calculation**: Running statistics using sum and sum of squares for variance calculation
- **Value at Risk (VaR)**: Percentile-based risk calculations with return distributions
- **Portfolio Analytics**: Weighted calculations and risk-adjusted performance metrics
- **Real-time Monitoring**: Continuous risk assessment with sliding windows
- **Risk Alerts**: Automated detection of high-risk conditions

## Pipeline Architecture

```
TradeSource → TradeValidationTransform → [Branch] → VolatilityCalculator → RiskReportSink
                                     → [Branch] → ValueAtRiskCalculator → RiskReportSink
                                     → [Branch] → PortfolioAnalyticsCalculator → RiskReportSink
                                     → [Branch] → RiskReportSink (trade statistics)
```

### Components

1. **TradeSource**: Generates realistic financial trades across multiple asset classes
2. **TradeValidationTransform**: Validates and enriches trades with risk scoring
3. **VolatilityCalculator**: Calculates price volatility using complex accumulator state
4. **ValueAtRiskCalculator**: Computes VaR using percentile-based approach
5. **PortfolioAnalyticsCalculator**: Performs comprehensive portfolio analysis
6. **RiskReportSink**: Displays formatted risk analytics dashboard

## AdvancedAggregateNode Implementations

### VolatilityCalculator

```csharp
public class VolatilityCalculator : AdvancedAggregateNode<ValidatedTrade, string, VolatilityAccumulator, VolatilityResult>
```

**Accumulator Pattern**:

- Maintains running sum and sum of squares for variance calculation
- Tracks min/max prices for range analysis
- Records timing information for temporal analysis

**Key Features**:

- 5-minute tumbling windows for discrete volatility periods
- Separates computational state (VolatilityAccumulator) from formatted results (VolatilityResult)
- Efficient incremental updates using mathematical properties of variance

### ValueAtRiskCalculator

```csharp
public class ValueAtRiskCalculator : AdvancedAggregateNode<ValidatedTrade, string, ValueAtRiskAccumulator, ValueAtRiskResult>
```

**Accumulator Pattern**:

- Collects return distribution for percentile calculations
- Maintains initial portfolio value for VaR normalization
- Builds comprehensive return history for statistical analysis

**Key Features**:

- 1-minute sliding windows every 15 seconds for real-time monitoring
- VaR calculation at 95% and 99% confidence levels
- Expected Shortfall (Conditional VaR) for tail risk assessment

### PortfolioAnalyticsCalculator

```csharp
public class PortfolioAnalyticsCalculator : AdvancedAggregateNode<ValidatedTrade, string, PortfolioAnalyticsAccumulator, PortfolioAnalyticsResult>
```

**Accumulator Pattern**:

- Dictionary-based state for asset weights and returns
- Weighted return and volatility calculations
- Trade count and timing for activity analysis

**Key Features**:

- 5-minute tumbling windows for portfolio analysis periods
- Risk-adjusted performance metrics (Sharpe ratio)
- Diversification analysis using Herfindahl-Hirschman Index

## Window Strategies

### Tumbling Windows (5 minutes)

- Used for volatility and portfolio analytics
- Non-overlapping discrete time periods
- Suitable for periodic risk reporting

### Sliding Windows (1 minute, sliding every 15 seconds)

- Used for Value at Risk calculations
- Overlapping windows for continuous monitoring
- Provides real-time risk assessment

## Data Models

### Core Models

- **FinancialTrade**: Base trade data with market information
- **ValidatedTrade**: Enriched trade with validation results and risk scores
- **VolatilityAccumulator/Result**: Complex accumulator and formatted volatility metrics
- **ValueAtRiskAccumulator/Result**: Return distribution and VaR calculations
- **PortfolioAnalyticsAccumulator/Result**: Portfolio state and comprehensive analytics

### Key Features

- **Event-Time Processing**: Proper timestamp handling for financial data
- **Watermark Support**: Handles out-of-order data gracefully
- **Type Safety**: Strongly typed models for compile-time validation
- **Extensibility**: Easy to add new risk metrics and calculations

## Running the Sample

### Prerequisites

- .NET 8.0 or later
- NPipeline framework

### Execution

```bash
dotnet run --project samples/Sample_AdvancedAggregateNode
```

### Expected Output

The sample generates a comprehensive risk dashboard showing:

1. **Summary Statistics**: Trade counts, validation rates
2. **Volatility Analysis**: Symbol-wise volatility with annualized metrics
3. **VaR Analysis**: Portfolio risk metrics at different confidence levels
4. **Portfolio Analytics**: Risk-adjusted performance and diversification metrics
5. **Risk Alerts**: Automated alerts for high-risk conditions

## AdvancedAggregateNode vs AggregateNode

| Feature          | AggregateNode          | AdvancedAggregateNode               |
|------------------|------------------------|-------------------------------------|
| Type Parameters  | `<TIn, TKey, TResult>` | `<TIn, TKey, TAccumulate, TResult>` |
| Accumulator Type | Same as Result         | Separate from Result                |
| Use Case         | Simple aggregations    | Complex state management            |
| State Management | Limited                | Sophisticated patterns              |
| Performance      | Higher                 | Optimal for complex calculations    |

## Performance Considerations

### Memory Efficiency

- Accumulator patterns minimize memory allocation
- Incremental updates avoid full dataset recalculation
- Window-based processing limits data retention

### Computational Efficiency

- Mathematical properties used for efficient variance calculation
- Percentile calculations with linear interpolation
- Weighted calculations using incremental updates

### Scalability

- Parallel processing across different risk calculations
- Window-based isolation for independent processing
- Efficient state management for high-frequency data

## Extending the Sample

### Adding New Risk Metrics

1. Create accumulator and result models
2. Implement `AdvancedAggregateNode<TIn, TKey, TAccumulate, TResult>`
3. Define `CreateAccumulator()`, `Accumulate()`, and `GetResult()` methods
4. Add to pipeline definition

### Custom Window Strategies

```csharp
// Custom window assigner for specific business requirements
var customWindow = WindowAssigner.Sliding(
    TimeSpan.FromMinutes(10),
    TimeSpan.FromMinutes(2)
);
```

### Additional Data Sources

- Real market data feeds
- Historical data replay
- Multiple exchange integration
- Custom trade formats

## Best Practices

### Accumulator Design

- Keep accumulators minimal and focused
- Use efficient data structures for state
- Consider memory implications of large collections
- Implement proper equality for custom types

### Window Selection

- Choose window sizes based on business requirements
- Consider data volume and processing latency
- Balance between responsiveness and statistical significance
- Align with regulatory reporting periods

### Error Handling

- Validate input data in accumulation
- Handle edge cases (empty windows, single values)
- Implement proper logging for debugging
- Consider circuit breakers for extreme conditions

## Real-World Applications

This sample provides a foundation for:

- **Real-time trading risk monitoring**
- **Regulatory reporting systems**
- **Portfolio management platforms**
- **Risk management dashboards**
- **Compliance and audit systems**
- **Algorithmic trading risk controls**

## Conclusion

The `AdvancedAggregateNode` sample demonstrates sophisticated patterns for financial risk analysis, showcasing the power and flexibility of NPipeline for
complex quantitative applications. The separation of accumulator and result types enables efficient state management while providing clean, formatted outputs
for business consumption.

This implementation serves as a comprehensive reference for building production-grade risk analysis systems using NPipeline's advanced aggregation capabilities.
