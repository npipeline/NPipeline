---
title: Advanced Samples
description: Production-grade NPipeline samples for advanced developers (Samples 11-23)
sidebar_position: 4
---

# Advanced Samples (11-23)

These samples demonstrate production-grade patterns, complex scenarios, and advanced techniques for building sophisticated data processing systems.

---

## Foundational Advanced Concepts

### Sample 11: Custom Node Implementation

**Concepts demonstrated:**

- Creating custom node implementations
- Lifecycle management
- Performance optimization through caching and batching
- Observability patterns

**What it does:** Shows how to implement custom node types including a sensor data source, a cached transform for performance optimization, and a batching sink for improved throughput.

**Key takeaways:** Extending NPipeline to meet specific application requirements. Essential foundation for advanced pipeline development.

---

### Sample 12: Performance Optimization

**Concepts demonstrated:**

- ValueTask optimization
- Synchronous fast paths
- Memory allocation reduction
- Performance measurement

**What it does:** Demonstrates advanced performance optimization techniques including ValueTask usage, synchronous fast paths for simple operations, memory allocation reduction with ArrayPool, and comprehensive performance measurement.

**Key takeaways:** Advanced techniques for optimizing pipeline performance. Best practices for high-throughput systems.

---

## Data Processing Patterns

### Sample 13: Batching Node

**Concepts demonstrated:**

- `BatchingNode<T>` for collecting individual items into batches
- Size-based, time-based, and hybrid batching strategies
- BatchProcessingTransform for efficient batch operations
- Performance optimization through batch processing
- Bulk database operations with batched results
- BatchingExecutionStrategy for optimal performance
- Comprehensive testing patterns for batched pipelines

**What it does:** Demonstrates the `BatchingNode<T>` functionality for efficient batch processing of individual items. The sample simulates an IoT sensor data processing pipeline that collects individual sensor readings into batches, processes them with aggregations and calculations, and performs bulk database operations. It shows different batching strategies (size-based, time-based, and hybrid) and provides detailed performance analysis comparing batch processing to individual item processing.

**Key takeaways:** Using batching to significantly improve performance for computational operations, database operations, and network operations while maintaining flexibility and reliability in data processing pipelines.

---

### Sample 14: UnbatchingNode - Stream Conversion Patterns

**Concepts demonstrated:**

- UnbatchingNode for converting batched data back to individual item streams
- Batch-to-stream conversion patterns with ordering guarantee preservation
- Backpressure handling mechanisms for downstream consumers
- Performance optimization techniques for stream conversion
- Integration with existing batching nodes
- Hybrid batch/stream processing architecture
- Real-world financial trading system scenario

**What it does:** Implements a sophisticated financial trading system that demonstrates the unbatching pattern for converting batched analytics results back to individual item streams. The pipeline processes individual market data events, batches them for efficient analytics processing, then unbatches the results to generate individual alert events for real-time monitoring. This showcases how to combine the efficiency of batch processing with the responsiveness of individual event processing in a real-world trading scenario.

The pipeline flow demonstrates the complete unbatching pattern:
1. MarketDataSource generates individual market data events from multiple exchanges
2. BatchingNode collects events into batches based on size and time thresholds
3. BatchAnalyticsTransform processes batches efficiently with comprehensive analytics calculations
4. BatchEventExtractor extracts original events from batch analytics results
5. UnbatchingNode converts batched events back to individual market data events (UNBATCHING)
6. AlertGeneratorTransform converts individual events to alerts based on batch analytics insights
7. RealTimeAlertingSink processes individual alert events for real-time monitoring

**Key takeaways:** Understanding how to implement stream conversion patterns that enable hybrid batch/stream processing architectures. Learning how to maintain ordering guarantees when converting between batched and individual data flows, implementing backpressure handling for downstream consumers, and optimizing performance for high-frequency trading scenarios. This pattern is essential when you need both efficient batch processing for analytics and individual event processing for real-time operations like alerting.

---

## Stream Processing and Windowing

### Sample 15: Streaming Analytics

**Concepts demonstrated:**

- Time-based windowing (tumbling and sliding windows)
- Watermarks and late data handling
- Real-time aggregations
- Branching pipelines

**What it does:** Implements a time-windowed aggregation pipeline that processes sensor data in real-time, handles late-arriving data, and performs statistical analysis with trend and anomaly detection.

**Key takeaways:** Building stream processing applications with correct temporal semantics and handling real-world challenges like out-of-order and late-arriving data.

---

### Sample 16: WindowingStrategies - Advanced Windowing Strategies

**Concepts demonstrated:**

- Session-based windowing with custom timeout management and session splitting
- Dynamic windowing with adaptive sizing based on activity patterns and diversity metrics
- Custom trigger windowing with multiple trigger conditions (conversion, high-value, time-based)
- Multi-strategy parallel processing for comprehensive analytics comparison
- Advanced pattern detection across different windowing approaches
- User behavior analytics with engagement, retention, and churn metrics
- Sophisticated temporal pattern analysis with confidence scoring
- Performance optimization for complex windowing scenarios

**What it does:** Implements a comprehensive user analytics platform that demonstrates three advanced windowing strategies working in parallel to provide deep insights into user behavior patterns. The pipeline processes individual user events through session-based windowing, then applies three different windowing strategies simultaneously: direct session analytics, dynamic windowing that adapts to data characteristics, and custom trigger windowing driven by business rules. Each strategy provides unique insights, and the results are combined with advanced pattern detection to identify behavioral, temporal, and navigation patterns.

The pipeline architecture demonstrates sophisticated windowing techniques:
1. **SessionWindowAssigner** groups events into sessions based on activity timeouts
2. **Three parallel processing paths** apply different windowing strategies:
   - Direct session analytics for traditional metrics
   - **DynamicWindowAssigner** that adapts window size based on activity levels and device/geographic diversity
   - **CustomTriggerWindowAssigner** that uses conversion thresholds, high-value events, and time intervals as triggers
3. **SessionAnalyticsCalculator** processes each windowing strategy for comprehensive metrics
4. **PatternDetectionCalculator** identifies sophisticated user behavior patterns across all strategies
5. **UserBehaviorSink** provides formatted analytics results with executive summaries

**Key takeaways:** Building sophisticated temporal analytics systems with adaptive windowing that goes beyond basic tumbling and sliding windows. Understanding how to implement session-based, dynamic, and custom trigger windowing strategies for different analytical needs. Learning how to process multiple windowing strategies in parallel to gain comprehensive insights from the same data. Mastering advanced pattern detection with confidence scoring and business impact assessment. This sample is essential when standard time-based windows are insufficient for capturing complex user behavior patterns or when business requirements demand sophisticated, adaptive windowing logic.

---

### Sample 17: AdvancedAggregateNode

**Concepts demonstrated:**

- `AdvancedAggregateNode<TIn, TKey, TAccumulate, TResult>` for complex aggregations
- Separate accumulator and result types for sophisticated state management
- Financial risk calculations (volatility, Value at Risk, portfolio analytics)
- Statistical calculations using complex accumulator patterns
- Weighted metrics and risk-adjusted performance calculations
- Real-time risk monitoring with alerting

**What it does:** Implements a comprehensive financial risk analysis pipeline that processes simulated trading data and calculates various risk metrics including volatility, Value at Risk (VaR), and portfolio analytics. Demonstrates sophisticated accumulator patterns using tuples, dictionaries, and collections for complex state management.

**Key takeaways:** Building advanced analytics systems where accumulator and result types differ, enabling complex statistical calculations and sophisticated state management. Understanding when to use AdvancedAggregateNode over AggregateNode for complex scenarios.

---

## Advanced Join Operations

### Sample 18: Keyed Join Node

**Concepts demonstrated:**

- KeyedJoinNode for joining data streams based on common keys
- Inner, Left Outer, Right Outer, and Full Outer join strategies
- Multi-stream correlation and data enrichment patterns
- Real-time aggregation on joined data
- Memory management for unmatched items
- Performance considerations for key distribution

**What it does:** Demonstrates how to use NPipeline's KeyedJoinNode to join data streams based on common keys in an e-commerce scenario. It shows how to join orders with customer data using different join strategies, enrich the results with product information, and generate business intelligence through real-time aggregations by customer tier and product category.

**Key takeaways:** Building sophisticated data pipelines that can correlate and enrich data from multiple streams using various join strategies while managing performance and memory efficiently.

---

### Sample 19: Time Windowed Join Node

**Concepts demonstrated:**

- TimeWindowedJoinNode for temporal correlation of events
- Tumbling and sliding window strategies with configurable sizes
- Timestamp extraction and watermark management for event-time processing
- Out-of-order data handling with configurable tolerance
- Memory-efficient state management with automatic cleanup
- Performance optimization for time-based joins

**What it does:** Demonstrates time-windowed joins for correlating IoT sensor readings with maintenance events that occur within specific time windows. The sample shows how to use tumbling and sliding windows to analyze maintenance effectiveness, handle out-of-order events, and perform temporal analysis on streaming data with proper watermark management.

**Key takeaways:** Implementing sophisticated temporal data processing patterns that can correlate events based on both keys and time proximity while handling real-world challenges like out-of-order data and managing memory efficiently.

---

### Sample 20: CustomMergeNode

**Concepts demonstrated:**

- CustomMergeNode for advanced stream merging strategies
- Priority-based merging with conflict resolution (NYSE > NASDAQ > International)
- Temporal alignment with configurable delay tolerance
- Data quality scoring and assessment (Completeness, Timeliness, Accuracy, Consistency)
- High-performance concurrent processing using `Channel<T>`
- Backpressure handling with intelligent buffering and drop strategies
- Real-world financial trading system scenario
- Multi-exchange market data processing
- Custom merge strategies extending `IMergeStrategy<T>`
- Performance optimization for high-frequency trading scenarios

**What it does:** Implements a sophisticated financial trading data processing pipeline that merges market data from multiple exchanges (NYSE, NASDAQ, International) using CustomMergeNode with priority-based conflict resolution and temporal alignment strategies. The pipeline demonstrates how to handle real-world challenges in financial data processing, including conflicting data from different exchanges, timing variations, and data quality assessment. It showcases advanced merging patterns where NYSE data takes precedence over NASDAQ and International exchanges, with temporal alignment to handle out-of-order data and configurable delay tolerance windows.

**Key takeaways:** Building complex stream merging systems with custom conflict resolution logic and business rule integration. Understanding how to implement priority-based merging strategies, temporal alignment for handling timing discrepancies, and data quality assessment for financial data processing. Learning advanced performance optimization techniques using `Channel<T>` for high-throughput scenarios and backpressure handling for reliable processing in high-frequency trading environments.

---

## Event-Time Processing

### Sample 21: WatermarkHandling - Advanced Event-Time Processing

**Concepts demonstrated:**

- WatermarkHandling for advanced event-time processing
- Custom watermark generators with adaptive strategies
- Handling of late data with configurable lateness tolerance
- Watermark alignment across multiple streams
- Dynamic watermark adjustment based on system conditions
- Comprehensive monitoring and alerting for watermark issues
- Real-world IoT manufacturing platform scenario
- Multi-network sensor processing (WiFi, LoRaWAN, Ethernet)
- Clock synchronization handling (GPS, NTP, internal clocks)
- Network-aware processing strategies
- Time-windowed aggregation with watermark-based advancement

**What it does:** Implements a sophisticated IoT manufacturing platform that processes sensor data from heterogeneous networks with different timing characteristics. The sample demonstrates advanced watermark handling capabilities for coordinating multiple sensor networks: Production Line A (WiFi sensors with GPS-disciplined clocks), Production Line B (LoRaWAN sensors with NTP synchronization), and Environmental sensors (Ethernet sensors with internal clocks). It showcases adaptive watermark generation that adjusts based on network conditions, configurable late data handling with tolerance policies, and comprehensive monitoring and alerting for watermark issues. The pipeline uses network-aware processing strategies to handle the unique challenges of each network type while maintaining accurate temporal semantics across the entire system.

**Key takeaways:** Building complex event-time processing systems that can handle heterogeneous data sources with different timing characteristics. Understanding how to implement adaptive watermark strategies, manage late data effectively, and coordinate multiple streams with varying synchronization capabilities. Learning how to design resilient IoT data processing pipelines that can maintain accurate temporal semantics despite network variations, clock drift, and system load changes.

---

## Observability and Monitoring

### Sample 22: TapNode

**Concepts demonstrated:**

- `TapNode<T>` for non-intrusive monitoring of data streams
- Multiple tap points for comprehensive pipeline observability
- Audit logging for compliance and regulatory requirements
- Real-time metrics collection and performance monitoring
- Alert generation for suspicious activities and operational issues
- Error isolation between monitoring and main processing flows
- Side-effect processing without modifying core business logic

**What it does:** Demonstrates how to use TapNode to add comprehensive monitoring capabilities to a financial transaction processing pipeline without affecting the main data flow. The sample shows multiple tap points at different pipeline stages (source, validation, processing) that generate audit trails, collect performance metrics, and create alerts for suspicious activities. It showcases how TapNode enables observability, compliance, and operational intelligence while maintaining separation between monitoring concerns and core business logic.

**Key takeaways:** Implementing non-intrusive monitoring patterns that provide comprehensive observability without modifying core pipeline logic or impacting performance.

---

## Complex End-to-End Scenarios

### Sample 23: Complex Data Transformations

**Concepts demonstrated:**

- Multi-stream joins
- External data lookups
- Complex aggregations
- Data lineage tracking

**What it does:** Implements sophisticated data processing scenarios including joining orders with customer data, product enrichment, complex aggregations, and complete data lineage tracking for auditability.

**Key takeaways:** Building complex data pipelines with joins, lookups, and lineage tracking for production systems. Integrates many concepts from earlier samples.

---

## Next Steps

- Want to review intermediate patterns? → [Intermediate Samples (6-10)](./intermediate.md)
- Looking for a specific topic? → [Samples by Topic](./by-topic.md)
- Ready to build your own? Check [Core Concepts](../core-concepts/index.md) and [Advanced Topics](../advanced-topics/index.md)
