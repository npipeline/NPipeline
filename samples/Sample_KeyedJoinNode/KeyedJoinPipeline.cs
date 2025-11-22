using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_KeyedJoinNode.Nodes;

namespace Sample_KeyedJoinNode;

/// <summary>
///     Keyed Join pipeline demonstrating NPipeline's KeyedJoinNode functionality.
///     This pipeline showcases different join strategies and data enrichment patterns.
/// </summary>
/// <remarks>
///     This implementation demonstrates advanced NPipeline concepts including:
///     - Multi-stream joins using KeyedJoinNode with different join types
///     - Data enrichment through lookups and transformations
///     - Complex aggregations for business intelligence
///     - Real-time data processing with multiple output streams
/// </remarks>
public class KeyedJoinPipeline : IPipelineDefinition
{
    private JoinType _joinType;

    /// <summary>
    ///     Initializes a new instance of the <see cref="KeyedJoinPipeline" /> class.
    /// </summary>
    /// <param name="joinType">The type of join to demonstrate (defaults to Inner).</param>
    public KeyedJoinPipeline(JoinType joinType = JoinType.Inner)
    {
        _joinType = joinType;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="KeyedJoinPipeline" /> class.
    /// </summary>
    public KeyedJoinPipeline()
    {
        _joinType = JoinType.Inner;
    }

    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a comprehensive keyed join pipeline flow:
    ///     1. OrderSource and CustomerSource generate separate data streams
    ///     2. OrderCustomerJoin joins the streams based on CustomerId using specified join type
    ///     3. ProductLookup enriches joined data with product information
    ///     4. Sales aggregations generate business insights by different dimensions
    ///     5. Multiple sink nodes output different views of the processed data
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Read JoinType from context if available
        if (context.Parameters.TryGetValue("JoinType", out var joinTypeObj) && joinTypeObj is JoinType joinType)
            _joinType = joinType;

        // Add source nodes for different data streams
        var orderSource = builder.AddSource<OrderSource, Order>("order-source");
        var customerSource = builder.AddSource<CustomerSource, Customer>("customer-source");

        // Add join node to combine orders with customers using the specified join type
        var orderCustomerJoin = builder.AddJoin<OrderCustomerJoinNode, Order, Customer, OrderCustomerJoin>("order-customer-join");

        // Configure join node with specified join type
        // Note: Join type configuration would be handled by the join node itself
        // This line is just a placeholder to show how services could be accessed if needed
        // var logger = context.ServiceProvider?.GetService(typeof(ILogger<OrderCustomerJoinNode>)) as ILogger<OrderCustomerJoinNode>;

        // Add transform node to enrich orders with product information
        var productLookup = builder.AddTransform<ProductLookupNode, OrderCustomerJoin, EnrichedOrder>("product-lookup");

        // Add aggregation nodes for business intelligence
        var salesByTierAggregation =
            builder.AddAggregate<SalesByCustomerTierAggregator, EnrichedOrder, string, SalesByCustomerTier>("sales-by-tier-aggregation");

        var salesByCategoryAggregation =
            builder.AddAggregate<SalesByCategoryAggregator, EnrichedOrder, string, SalesByCategory>("sales-by-category-aggregation");

        // Add sink nodes for different output views
        var joinSink = builder.AddSink<ConsoleSink<OrderCustomerJoin>, OrderCustomerJoin>("join-sink");
        var enrichedSink = builder.AddSink<ConsoleSink<EnrichedOrder>, EnrichedOrder>("enriched-sink");
        var tierSalesSink = builder.AddSink<ConsoleSink<SalesByCustomerTier>, SalesByCustomerTier>("tier-sales-sink");
        var categorySalesSink = builder.AddSink<ConsoleSink<SalesByCategory>, SalesByCategory>("category-sales-sink");

        // Connect the nodes in the pipeline flow

        // Connect sources to join node inputs
        // OrderSource connects to first input (Order) of the join
        builder.Connect(orderSource, orderCustomerJoin);

        // CustomerSource connects to second input (Customer) of the join
        builder.Connect(customerSource, orderCustomerJoin);

        // Connect join output to product lookup
        builder.Connect<OrderCustomerJoin>(orderCustomerJoin, productLookup);

        // Connect product lookup to aggregations
        builder.Connect<EnrichedOrder>(productLookup, salesByTierAggregation);
        builder.Connect<EnrichedOrder>(productLookup, salesByCategoryAggregation);

        // Connect aggregations to their respective sinks
        builder.Connect<SalesByCustomerTier>(salesByTierAggregation, tierSalesSink);
        builder.Connect<SalesByCategory>(salesByCategoryAggregation, categorySalesSink);

        // Connect join output to join sink (for debugging/join visibility)
        builder.Connect<OrderCustomerJoin>(orderCustomerJoin, joinSink);

        // Connect enriched orders to enriched sink (for debugging/enrichment visibility)
        builder.Connect<EnrichedOrder>(productLookup, enrichedSink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"Keyed Join Node Sample:

This sample demonstrates NPipeline's KeyedJoinNode functionality for joining data streams based on common keys:

Key Concepts Demonstrated:
- KeyedJoinNode with different join strategies (Inner, LeftOuter, RightOuter, FullOuter)
- Multi-stream data processing and correlation
- Data enrichment through lookups and transformations
- Real-time aggregations for business intelligence
- Handling of unmatched items in join operations

Pipeline Flow:
1. OrderSource generates order data with CustomerId foreign keys
2. CustomerSource generates customer master data
3. OrderCustomerJoin joins streams using CustomerId as the join key
4. ProductLookup enriches joined data with product information
5. Sales aggregations generate insights by customer tier and product category
6. Multiple sink nodes output different views of processed data

Join Types Demonstrated:
- Inner Join: Only orders with matching customers are processed
- Left Outer Join: All orders are processed, with placeholder data for unmatched customers
- Right Outer Join: All customers are processed, with placeholder data for unmatched orders
- Full Outer Join: All orders and customers are processed, with appropriate handling for unmatched items

Performance Considerations:
- KeyedJoinNode maintains in-memory buffers for unmatched items
- Consider memory usage with large datasets or high cardinality keys
- Join performance depends on key distribution and stream ordering
- Use appropriate join types based on business requirements

Real-World Scenarios:
- Order processing with customer enrichment
- Event correlation with metadata
- Financial transaction matching with account details
- Sensor data fusion with calibration information
- Log analysis with user profile enrichment

This implementation showcases production-ready patterns for:
- Real-time data correlation and enrichment
- Business intelligence generation from joined streams
- Handling of data quality issues in stream processing
- Multi-dimensional analytics on joined data";
    }

    /// <summary>
    ///     Gets a description of the specific join type being demonstrated.
    /// </summary>
    /// <returns>A description of the current join type configuration.</returns>
    public string GetJoinTypeDescription()
    {
        return _joinType switch
        {
            JoinType.Inner => "Inner Join: Only orders with matching customers will be processed.",
            JoinType.LeftOuter => "Left Outer Join: All orders will be processed, with placeholder data for unmatched customers.",
            JoinType.RightOuter => "Right Outer Join: All customers will be processed, with placeholder data for unmatched orders.",
            JoinType.FullOuter => "Full Outer Join: All orders and customers will be processed, with appropriate handling for unmatched items.",
            _ => $"Unknown Join Type: {_joinType}",
        };
    }
}
