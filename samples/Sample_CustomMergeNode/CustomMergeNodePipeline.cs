using System;
using NPipeline.Pipeline;
using Sample_CustomMergeNode.Models;
using Sample_CustomMergeNode.Nodes;
using Sample_CustomMergeNode.Strategies;

namespace Sample_CustomMergeNode;

/// <summary>
///     Pipeline definition for the CustomMergeNode sample demonstrating advanced stream merging patterns
///     for financial trading systems with priority-based merging, temporal alignment, and quality assessment.
/// </summary>
/// <remarks>
///     This implementation follows the IPipelineDefinition pattern and demonstrates:
///     1. Real-time market data generation from multiple exchanges
///     2. Priority-based merging with conflict resolution (NYSE > NASDAQ > International)
///     3. Temporal alignment with configurable delay tolerance
///     4. Data quality scoring and validation
///     5. High-performance concurrent processing using Channel
///     <T>
///         6. Backpressure handling with intelligent buffering
///         7. Comprehensive error handling and logging
///         8. Performance metrics and monitoring
///         The pipeline flow:
///         NYSE Source → PriorityBasedMergeNode → DataQualityValidator → MarketDataSink
///         NASDAQ Source → PriorityBasedMergeNode → DataQualityValidator → MarketDataSink
///         International Source → PriorityBasedMergeNode → DataQualityValidator → MarketDataSink
/// </remarks>
public class CustomMergeNodePipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a merging pipeline flow that demonstrates advanced stream merging:
    ///     1. Multiple market data sources generate realistic trading data
    ///     2. PriorityBasedMergeNode merges streams with priority handling
    ///     3. DataQualityValidator scores and validates merged data
    ///     4. MarketDataSink provides formatted output with metrics
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Create merge strategies with null loggers for now
        var priorityStrategy = new PriorityMergeStrategy(
            null,
            TimeSpan.FromMilliseconds(50),
            1000);

        // Create the priority-based merge node with backpressure handling
        var mergeNode = new PriorityBasedMergeNode(
            priorityStrategy,
            null);

        // Add market data sources
        var nyseSource = builder.AddSource<NyseMarketDataSource, MarketDataTick>("nyse-source");
        var nasdaqSource = builder.AddSource<NasdaqMarketDataSource, MarketDataTick>("nasdaq-source");
        var internationalSource = builder.AddSource<InternationalMarketDataSource, MarketDataTick>("international-source");

        // Add the priority-based merge node as a transform node
        var mergeNodeDefinition = builder.AddTransform<PriorityBasedMergeNode, MarketDataTick, MarketDataTick>("priority-merge");

        // Add data quality validation
        var qualityValidator = builder.AddTransform<DataQualityValidator, MarketDataTick, MarketDataTick>("quality-validator");

        // Add market data sink for output
        var marketDataSink = builder.AddSink<MarketDataSink, MarketDataTick>("market-data-sink");

        // Register pre-configured merge node instance
        builder.AddPreconfiguredNodeInstance("priority-merge", mergeNode);

        // Connect all sources to the merge node
        builder.Connect(nyseSource, mergeNodeDefinition);
        builder.Connect(nasdaqSource, mergeNodeDefinition);
        builder.Connect(internationalSource, mergeNodeDefinition);

        // Connect merge node to quality validator
        builder.Connect(mergeNodeDefinition, qualityValidator);

        // Connect quality validator to sink
        builder.Connect(qualityValidator, marketDataSink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"CustomMergeNode Pipeline Sample:

This sample demonstrates advanced stream merging patterns for financial trading systems using NPipeline's CustomMergeNode:

Key Features:
- Priority-based Merging: NYSE > NASDAQ > International exchange priority handling
- Temporal Alignment: Configurable delay tolerance for out-of-order data
- Data Quality Scoring: Multi-dimensional quality assessment (Completeness, Timeliness, Accuracy, Consistency)
- High-performance Processing: Channel<T> for concurrent data flow
- Backpressure Handling: Intelligent buffering with drop strategies
- Error Isolation: Comprehensive error handling and logging
- Performance Metrics: Real-time monitoring and observability

Pipeline Architecture:
1. Market Data Sources generate realistic trading data:
   - NYSE Source: High priority, lower latency, more reliable data
   - NASDAQ Source: Medium priority, medium latency
   - International Source: Lower priority, higher latency, occasional quality issues
   - Realistic market data simulation with proper timestamps and price movements

2. PriorityBasedMergeNode performs advanced merging:
   - Priority-based conflict resolution based on exchange priorities
   - Temporal alignment with configurable delay tolerance
   - Channel<T> for high-performance concurrent processing
   - Backpressure handling with intelligent buffering
   - Comprehensive error handling and performance metrics

3. DataQualityValidator assesses data quality:
   - Multi-dimensional quality scoring (Completeness, Timeliness, Accuracy, Consistency)
   - Quality thresholds and validation rules
   - Quality metrics and reporting
   - Error detection and handling

4. MarketDataSink provides formatted output:
   - Formatted display of merged market data
   - Quality metrics and statistics
   - Performance indicators and timing information
   - Clear visualization of merge results

CustomMergeNode Concepts Demonstrated:
- Custom Merge Logic: Priority-based merging with conflict resolution
- Temporal Processing: Time-based synchronization and alignment
- Quality Assessment: Multi-dimensional data quality scoring
- Performance Optimization: Channel<T> for high-throughput scenarios
- Backpressure Management: Intelligent buffering strategies
- Error Handling: Comprehensive error isolation and recovery
- Monitoring: Real-time metrics and observability

This implementation provides a foundation for building high-frequency trading systems
with NPipeline, demonstrating how CustomMergeNode enables advanced stream merging
while maintaining data quality and performance in demanding financial scenarios.";
    }
}
