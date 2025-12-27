# Intent-Driven Grouping API Sample

This sample demonstrates the **intent-driven grouping API** that helps developers choose the correct grouping strategy by requiring explicit intent declaration.

## The Problem

Traditionally, developers struggle to understand the critical distinction between:

- **Batching**: Grouping items for operational efficiency (e.g., bulk database inserts)
- **Aggregation**: Grouping items for temporal correctness (e.g., hourly analytics with late data)

Choosing the wrong approach leads to subtle data corruption or over-engineered solutions.

## The Solution

The intent-driven grouping API makes your intent explicit:

```csharp
// Crystal clear: This is for operational efficiency
var batcher = builder.GroupItems<Order>()
    .ForOperationalEfficiency(batchSize: 100, maxWait: TimeSpan.FromSeconds(5));

// Crystal clear: This is for temporal correctness
var aggregator = builder.GroupItems<Sale>()
    .ForTemporalCorrectness(
        windowSize: TimeSpan.FromHours(1),
        keySelector: s => s.Category,
        initialValue: () => 0m,
        accumulator: (sum, s) => sum + s.Amount);
```

## Scenarios Demonstrated

### Scenario 1: Operational Efficiency (Batching)

**Use case**: Bulk database inserts to reduce I/O overhead.

- Generate 250 orders
- Batch them into groups of 100
- Simulate bulk database insert
- **Key insight**: Order timing doesn't affect correctness

### Scenario 2: Temporal Correctness (Aggregation)

**Use case**: Calculate sales totals by category with late-arriving data.

- Generate sales events with timestamps
- 10% of events arrive "late" (2 minutes old)
- Aggregate into 5-minute windows by category
- **Key insight**: Late data is handled correctly using event time

### Scenario 3: Rolling Windows (Sliding Aggregation)

**Use case**: Calculate 15-minute rolling temperature averages.

- Generate sensor readings over time
- Use sliding windows (15-min window, 5-min slide)
- Calculate continuous moving averages
- **Key insight**: Overlapping windows for continuous monitoring

## Running the Sample

```bash
dotnet run --project samples/Sample_IntentDrivenGrouping
```

## Expected Output

```text
=== Intent-Driven Grouping API Demo ===

Scenario 1: Batching for Operational Efficiency
Goal: Reduce database load by batching inserts

  💾 Bulk inserting 100 orders to database
  💾 Bulk inserting 100 orders to database
  💾 Bulk inserting 50 orders to database

------------------------------------------------------------

Scenario 2: Aggregation for Temporal Correctness
Goal: Calculate hourly sales totals, handling late data

  ⏰ Late event: SALE-00042 (2 minutes old)
  📊 12 sales, Total: $3,456.00
  📊 8 sales, Total: $2,134.50
  📊 15 sales, Total: $4,567.80
  📊 10 sales, Total: $1,890.25

------------------------------------------------------------

Scenario 3: Rolling Window Aggregation
Goal: Calculate 15-minute rolling averages

  🌡️  Rolling 15-min avg: 18.34°C
  🌡️  Rolling 15-min avg: 19.12°C
  🌡️  Rolling 15-min avg: 20.45°C

=== Demo Complete ===
```

## Key Takeaways

1. **ForOperationalEfficiency**: Use when external system constraints drive batching
2. **ForTemporalCorrectness**: Use when data timing matters for correctness
3. **ForRollingWindow**: Use when you need continuous, overlapping windows
4. **Self-documenting code**: Intent is clear from method names
5. **Prevents mistakes**: Can't accidentally use batching for time-windowed analytics

## Related Documentation

- [Grouping Strategies Guide](../../docs/core-concepts/grouping-strategies.md)
- [Batching Nodes](../../docs/core-concepts/nodes/batching.md)
- [Aggregation Nodes](../../docs/core-concepts/nodes/aggregation.md)

## What's Next?

Try modifying the sample:

- Change batch sizes and timeouts
- Increase the percentage of late events
- Experiment with different window sizes
- Add more complex accumulator logic
