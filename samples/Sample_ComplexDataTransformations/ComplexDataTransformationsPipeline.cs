using NPipeline.Pipeline;
using Sample_ComplexDataTransformations.Nodes;

namespace Sample_ComplexDataTransformations;

/// <summary>
///     Complex data transformations pipeline demonstrating advanced NPipeline concepts.
///     This pipeline implements a sophisticated data processing flow with multiple streams:
///     1. OrderSource and CustomerSource generate separate data streams
///     2. OrderCustomerJoin joins the streams based on CustomerId
///     3. ProductLookup enriches order data with product information
///     4. SalesAggregation performs complex aggregations by category
///     5. CustomerBehaviorAggregator analyzes customer purchase patterns
///     6. LineageTracker tracks data transformations throughout the pipeline
/// </summary>
/// <remarks>
///     This implementation demonstrates advanced NPipeline capabilities including:
///     - Multi-stream joins using KeyedJoinNode
///     - External data lookups using LookupNode
///     - Complex aggregations using AggregateNode
///     - Data lineage tracking for auditability
///     - Time-windowed processing for real-time analytics
/// </remarks>
public class ComplexDataTransformationsPipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a complex multi-stream pipeline flow:
    ///     OrderSource -> OrderCustomerJoin -> ProductLookup -> SalesAggregation -> ConsoleSink
    ///     CustomerSource -> OrderCustomerJoin
    ///     ProductSource -> ProductLookup
    ///     OrderCustomerJoin -> CustomerBehaviorAggregator -> ConsoleSink
    ///     The pipeline processes e-commerce data through these stages:
    ///     1. Sources generate orders, customers, and products data
    ///     2. Join combines orders with customer information
    ///     3. Lookup enriches orders with product details
    ///     4. Aggregations generate business insights
    ///     5. Lineage tracking maintains data provenance
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add source nodes for different data streams
        var orderSource = builder.AddSource<OrderSource, Order>("order-source");
        var customerSource = builder.AddSource<CustomerSource, Customer>("customer-source");
        var productSource = builder.AddSource<ProductSource, Product>("product-source");

        // Add join node to combine orders with customers
        var orderCustomerJoin = builder.AddJoin<OrderCustomerJoinNode, Order, Customer, OrderCustomerJoin>("order-customer-join");

        // Add lookup node to enrich orders with product information
        var productLookup = builder.AddTransform<ProductLookupNode, OrderCustomerJoin, EnrichedOrder>("product-lookup");

        // Add aggregation nodes for business insights
        var salesAggregation = builder.AddAggregate<SalesByCategoryAggregator, EnrichedOrder, string, SalesByCategory>("sales-aggregation");

        var customerBehaviorAggregation =
            builder.AddAggregate<CustomerBehaviorAggregator, OrderCustomerJoin, int, CustomerPurchaseBehavior>("customer-behavior-aggregation");

        // Add lineage tracking node
        var lineageTracker = builder.AddTransform<LineageTrackingNode, object, LineageTrackedItem<object>>("lineage-tracker");

        // Add sink nodes for output
        var consoleSink = builder.AddSink<ConsoleSink, object>("console-sink");
        var lineageSink = builder.AddSink<LineageSink, LineageTrackedItem<object>>("lineage-sink");

        // Connect the nodes in the pipeline flow
        // Order processing branch - connect to join node inputs
        // OrderSource connects to first input (Order) of the join
        builder.Connect(orderSource, orderCustomerJoin);

        // CustomerSource connects to second input (Customer) of the join
        builder.Connect(customerSource, orderCustomerJoin);

        // Main processing flow - join output to product lookup
        builder.Connect<OrderCustomerJoin>(orderCustomerJoin, productLookup);

        // Product lookup to sales aggregation
        builder.Connect<EnrichedOrder>(productLookup, salesAggregation);

        // Sales aggregation to console sink
        builder.Connect<SalesByCategory>(salesAggregation, consoleSink);

        // Customer behavior analysis branch
        builder.Connect<OrderCustomerJoin>(orderCustomerJoin, customerBehaviorAggregation);
        builder.Connect<CustomerPurchaseBehavior>(customerBehaviorAggregation, consoleSink);

        // Connect ProductSource to console sink to prevent isolation
        // ProductSource generates product data for reference/debugging
        builder.Connect<Product>(productSource, consoleSink);

        // Lineage tracking (applied to main flow)
        builder.Connect<EnrichedOrder>(productLookup, lineageTracker);
        builder.Connect<LineageTrackedItem<object>>(lineageTracker, lineageSink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"Complex Data Transformations Sample:

This sample demonstrates advanced NPipeline concepts for sophisticated data processing:
- Multi-stream joins using KeyedJoinNode
- External data lookups using LookupNode
- Complex aggregations using AggregateNode
- Data lineage tracking for auditability
- Time-windowed processing for real-time analytics

The pipeline flow:
1. OrderSource, CustomerSource, and ProductSource generate separate data streams
2. OrderCustomerJoin combines orders with customer information using CustomerId
3. ProductLookup enriches order data with product information using ProductId
4. SalesAggregation performs complex aggregations by product category
5. CustomerBehaviorAggregator analyzes customer purchase patterns
6. LineageTracker tracks data transformations throughout the pipeline

Key concepts demonstrated:
- Joining multiple data streams with different schemas
- Enriching data through external lookups
- Performing complex aggregations with multiple dimensions
- Maintaining data lineage for audit and debugging
- Processing e-commerce data to generate business insights

This implementation showcases production-ready patterns for:
- Real-time data processing and analytics
- Data enrichment and transformation
- Business intelligence generation
- Data provenance and governance";
    }
}
