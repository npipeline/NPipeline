using NPipeline.Nodes;
using NPipeline.Pipeline;
using Sample_BranchNode.Models;
using Sample_BranchNode.Nodes;

namespace Sample_BranchNode;

/// <summary>
///     E-commerce order processing pipeline demonstrating BranchNode usage for parallel processing.
///     This pipeline showcases how to branch data flow to multiple processing paths simultaneously.
/// </summary>
/// <remarks>
///     This implementation follows the IPipelineDefinition pattern and demonstrates:
///     1. Real-time order event generation with realistic e-commerce data
///     2. Branching of order events to multiple parallel processing paths
///     3. Inventory management processing for stock tracking
///     4. Analytics processing for business intelligence
///     5. Customer notification processing for communication
///     6. Main order flow continuation alongside branch processing
///     7. Error isolation between parallel branches
///     8. Type preservation across different processing paths
///     The pipeline flow:
///     OrderSource → BranchNode → (3 parallel branches + main flow)
///     Branch 1: InventoryProcessor → InventorySink
///     Branch 2: AnalyticsProcessor → AnalyticsSink
///     Branch 3: NotificationProcessor → NotificationSink
///     Main flow: BranchNode → OrderProcessor → OrderSink
/// </remarks>
public class BranchNodePipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a branching pipeline flow that demonstrates parallel processing:
    ///     1. OrderSource generates realistic e-commerce order events
    ///     2. BranchNode fans out order events to multiple paths
    ///     3. Each branch processes the same order data differently for different business concerns
    ///     4. All branches execute in parallel without blocking each other
    ///     5. Main order flow continues alongside branch processing
    ///     6. Different sink nodes handle the output from each processing path
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        Console.WriteLine("BranchNodePipeline: Defining e-commerce order processing pipeline...");
        Console.WriteLine("BranchNodePipeline: Setting up parallel processing branches...");

        // Add the source node that generates order events
        var orderSource = builder.AddSource<OrderSource, OrderEvent>("order-source");

        // Add transform processors for each branch
        var inventoryProcessor = builder.AddTransform<InventoryProcessor, OrderEvent, InventoryUpdate>("inventory-processor");
        var analyticsProcessor = builder.AddTransform<AnalyticsProcessor, OrderEvent, AnalyticsEvent>("analytics-processor");
        var notificationProcessor = builder.AddTransform<NotificationProcessor, OrderEvent, NotificationEvent>("notification-processor");

        // Add a simple order processor for the main flow (passthrough transform)
        var orderProcessor = builder.AddTransform<OrderProcessor, OrderEvent, OrderEvent>("order-processor");

        // Add sink nodes for each processing path
        var inventorySink = builder.AddSink<ConsoleSink, InventoryUpdate>("inventory-sink");
        var analyticsSink = builder.AddSink<ConsoleSink, AnalyticsEvent>("analytics-sink");
        var notificationSink = builder.AddSink<ConsoleSink, NotificationEvent>("notification-sink");
        var orderSink = builder.AddSink<ConsoleSink, OrderEvent>("order-sink");

        // Connect main flow: source -> order processor -> order sink
        builder.Connect(orderSource, orderProcessor);
        builder.Connect(orderProcessor, orderSink);

        // Create three separate branches for parallel processing from the source
        // Branch 1: Inventory processing
        builder.Connect(orderSource, inventoryProcessor);
        builder.Connect(inventoryProcessor, inventorySink);

        // Branch 2: Analytics processing
        builder.Connect(orderSource, analyticsProcessor);
        builder.Connect(analyticsProcessor, analyticsSink);

        // Branch 3: Notification processing
        builder.Connect(orderSource, notificationProcessor);
        builder.Connect(notificationProcessor, notificationSink);

        Console.WriteLine("BranchNodePipeline: Pipeline definition completed");
        Console.WriteLine("BranchNodePipeline: Ready for execution with parallel processing branches");
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"BranchNode Pipeline Sample:

This sample demonstrates e-commerce order processing scenarios using NPipeline's BranchNode for parallel data flow:

Key Features:
- Parallel Processing: Single order event fanned out to multiple processing paths simultaneously
- Error Isolation: Failures in one branch don't affect other branches or main flow
- Type Preservation: Each branch maintains type safety for its specific processing needs
- Realistic Business Logic: Inventory, analytics, and notification processing with domain-specific rules
- Observable Output: Color-coded console output showing parallel execution timing

Pipeline Architecture:
1. OrderSource generates realistic e-commerce orders with:
   - Multiple product types (laptops, phones, tablets, accessories, furniture)
   - Various customer segments (premium, VIP, regular, new, corporate)
   - Different order statuses (pending, confirmed, shipped, delivered, cancelled)
   - Realistic pricing and timing patterns
   - 10-15 sample orders with 1-3 second intervals

2. BranchNode performs parallel data distribution:
   - Receives OrderEvent and duplicates it to multiple processing paths
   - Maintains original order flow while enabling parallel processing
   - Provides error isolation between branches
   - Executes all branches concurrently without blocking

3. InventoryProcessor handles inventory management:
   - Tracks inventory levels by product and warehouse location
   - Calculates priority based on stock levels (critical, high, medium, low)
   - Generates inventory updates with quantity changes and location assignments
   - Demonstrates warehouse management logic and reorder alerts

4. AnalyticsProcessor processes business intelligence:
   - Categorizes products (computers, mobile devices, accessories, etc.)
   - Segments customers (premium, VIP, regular, new, corporate)
   - Generates analytics events with metadata for BI processing
   - Includes price ranges, order sizes, and regional data

5. NotificationProcessor manages customer communications:
   - Determines notification type based on customer segment (email, SMS, push)
   - Generates appropriate messages for each order status
   - Handles scheduling and priority for different notification types
   - Demonstrates multi-channel communication strategies

6. ConsoleSink displays formatted output with:
   - Color-coded event types for easy identification
   - Detailed information for each event type
   - Timing information to demonstrate concurrent processing
   - Clear separation between different processing streams

BranchNode Concepts Demonstrated:
- Data Duplication: One event sent to multiple processing paths
- Parallel Execution: All branches run simultaneously
- Error Isolation: Branch failures don't impact other branches
- Type Safety: Each branch maintains its own output type
- Flow Continuation: Main pipeline flow continues alongside branches
- Resource Efficiency: Shared processing with specialized concerns

This implementation provides a foundation for building complex e-commerce systems
with NPipeline, demonstrating how BranchNode enables parallel processing while
maintaining data consistency and error isolation across different business domains.";
    }
}

/// <summary>
///     Simple passthrough transform for the main order flow.
///     This demonstrates that the main flow continues alongside branch processing.
/// </summary>
public class OrderProcessor : TransformNode<OrderEvent, OrderEvent>
{
    /// <summary>
    ///     Initializes a new instance of the OrderProcessor.
    /// </summary>
    public OrderProcessor()
    {
        Console.WriteLine("OrderProcessor: Initialized for main order flow processing");
    }

    /// <summary>
    ///     Processes an order event by passing it through unchanged.
    ///     This represents the main order processing flow that continues alongside branches.
    /// </summary>
    /// <param name="orderEvent">The order event to process.</param>
    /// <param name="context">The pipeline context.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The unchanged order event.</returns>
    public override async Task<OrderEvent> ExecuteAsync(
        OrderEvent orderEvent,
        PipelineContext context,
        CancellationToken cancellationToken)
    {
        // Simulate minimal processing delay for main flow
        await Task.Delay(50, cancellationToken);

        Console.WriteLine($"OrderProcessor: Processing order {orderEvent.OrderId} in main flow");

        // Return the order event unchanged (passthrough)
        return orderEvent;
    }
}
