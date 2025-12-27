using NPipeline.Nodes;
using NPipeline.Pipeline;
using NPipeline.SelfJoinExtensions;
using Sample_SelfJoinNode.Nodes;

namespace Sample_SelfJoinNode;

/// <summary>
///     Self Join pipeline demonstrating NPipeline's self-join functionality.
///     This pipeline showcases year-over-year sales comparison using the AddSelfJoin extension method.
/// </summary>
/// <remarks>
///     This implementation demonstrates advanced NPipeline concepts including:
///     - Self-join operations using AddSelfJoin extension method
///     - Joining a data stream with itself based on different criteria
///     - Type-safe wrapper types to handle the "type erasure" issue in joins
///     - Data enrichment and aggregation patterns
///     - Real-time business intelligence generation
/// </remarks>
public class SelfJoinPipeline : IPipelineDefinition
{
    private JoinType _joinType;
    private int _comparisonYear;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SelfJoinPipeline" /> class.
    /// </summary>
    /// <param name="joinType">The type of join to demonstrate (defaults to Inner).</param>
    /// <param name="comparisonYear">The year to compare against the previous year.</param>
    public SelfJoinPipeline(JoinType joinType = JoinType.Inner, int comparisonYear = 2024)
    {
        _joinType = joinType;
        _comparisonYear = comparisonYear;
    }

    /// <summary>
    ///     Initializes a new instance of the <see cref="SelfJoinPipeline" /> class.
    /// </summary>
    public SelfJoinPipeline()
    {
        _joinType = JoinType.Inner;
        _comparisonYear = 2024;
    }

    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a self-join pipeline flow:
    ///     1. SalesDataSource generates sales data for multiple years
    ///     2. Transform nodes split the stream into current year and previous year streams
    ///     3. Self-join combines the streams based on ProductId
    ///     4. Aggregation generates category-level summaries
    ///     5. Sink nodes output the results
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Read parameters from context if available
        if (context.Parameters.TryGetValue("JoinType", out var joinTypeObj) && joinTypeObj is JoinType joinType)
            _joinType = joinType;

        if (context.Parameters.TryGetValue("ComparisonYear", out var yearObj) && yearObj is int year)
            _comparisonYear = year;

        // Add source node for sales data
        var salesSource = builder.AddSource<SalesDataSource, SalesData>("sales-source");

        // Add transform nodes to split data by year
        // Current year data (left side of join)
        var currentYearFilter = builder.AddTransform<CurrentYearFilter, SalesData, SalesData>("current-year-filter");
        
        // Previous year data (right side of join)
        var previousYearFilter = builder.AddTransform<PreviousYearFilter, SalesData, SalesData>("previous-year-filter");

        // Add self-join node using the AddSelfJoin extension method
        // This joins the same type (SalesData) with itself based on ProductId
        var selfJoin = builder.AddSelfJoin<
            SalesData,
            SalesData,
            int,
            YearOverYearComparison
        >(
            "self-join",
            _joinType,
            (left, right) => new YearOverYearComparison(left, right)
        );

        // Add aggregation node for category summaries
        var categoryAggregator = builder.AddAggregate<CategoryAggregator, YearOverYearComparison, string, CategorySummary>("category-aggregator");

        // Add sink nodes for output
        var comparisonSink = builder.AddSink<ConsoleSink<YearOverYearComparison>, YearOverYearComparison>("comparison-sink");
        var categorySink = builder.AddSink<ConsoleSink<CategorySummary>, CategorySummary>("category-sink");

        // Connect the nodes in the pipeline flow

        // Connect source to both filter transforms
        builder.Connect(salesSource, currentYearFilter);
        builder.Connect(salesSource, previousYearFilter);

        // Connect filters to self-join inputs
        // Current year data connects to first input (left side)
        builder.Connect(currentYearFilter, selfJoin);

        // Previous year data connects to second input (right side)
        builder.Connect(previousYearFilter, selfJoin);

        // Connect self-join output to aggregation
        builder.Connect<YearOverYearComparison>(selfJoin, categoryAggregator);

        // Connect outputs to sinks
        builder.Connect<YearOverYearComparison>(selfJoin, comparisonSink);
        builder.Connect<CategorySummary>(categoryAggregator, categorySink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"Self Join Node Sample:

This sample demonstrates NPipeline's self-join functionality using the AddSelfJoin extension method:

Key Concepts Demonstrated:
- Self-join operations: Joining a data stream with itself
- AddSelfJoin extension method: Convenient API for self-joins
- Type-safe wrapper types: Handling type erasure in join nodes
- Year-over-year analysis: Comparing data across time periods
- Data enrichment and aggregation: Building insights from joined data

Pipeline Flow:
1. SalesDataSource generates sales data for multiple years (2022-2024)
2. CurrentYearFilter filters data for the comparison year (e.g., 2024)
3. PreviousYearFilter filters data for the previous year (e.g., 2023)
4. Self-join combines streams using ProductId as the join key
5. CategoryAggregator generates growth statistics by category
6. Sink nodes output year-over-year comparisons and category summaries

Self-Join Mechanics:
- The same data type (SalesData) is used for both join inputs
- LeftWrapper<T> and RightWrapper<T> distinguish the inputs internally
- The join key (ProductId) is extracted from both inputs
- Results are combined into YearOverYearComparison objects

Join Types Demonstrated:
- Inner Join: Only products with data in both years are compared
- LeftOuter Join: All products in the current year are included, with null for missing previous year data
- RightOuter Join: All products in the previous year are included, with null for missing current year data
- FullOuter Join: All products from both years are included, with appropriate null handling

Real-World Scenarios:
- Year-over-year sales analysis
- Price comparison across catalogs
- Event correlation between systems
- Order matching from different sources
- Performance comparison over time
- Inventory reconciliation

This implementation showcases production-ready patterns for:
- Temporal data analysis
- Business intelligence generation
- Growth trend identification
- Category-level performance tracking
- Handling of new and discontinued products";
    }

    /// <summary>
    ///     Gets a description of the specific join type being demonstrated.
    /// </summary>
    /// <returns>A description of the current join type configuration.</returns>
    public string GetJoinTypeDescription()
    {
        return _joinType switch
        {
            JoinType.Inner => "Inner Join: Only products with data in both years will be compared.",
            JoinType.LeftOuter => "Left Outer Join: All products in the current year will be included. New products (no previous year data) will show 'New Product' status.",
            JoinType.RightOuter => "Right Outer Join: All products in the previous year will be included. Discontinued products (no current year data) will be included.",
            JoinType.FullOuter => "Full Outer Join: All products from both years will be included, with appropriate handling for new and discontinued products.",
            _ => $"Unknown Join Type: {_joinType}",
        };
    }

    /// <summary>
    ///     Gets the comparison year being used.
    /// </summary>
    /// <returns>The comparison year.</returns>
    public int GetComparisonYear() => _comparisonYear;
}

/// <summary>
///     Transform node that filters sales data for the current (comparison) year.
///     This provides the left side of the self-join.
/// </summary>
public class CurrentYearFilter : TransformNode<SalesData, SalesData>
{
    private int _comparisonYear;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CurrentYearFilter" /> class.
    /// </summary>
    public CurrentYearFilter()
    {
        _comparisonYear = 2024;
    }

    /// <inheritdoc />
    public override IDataPipe<SalesData> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        // Read comparison year from context if available
        if (context.Parameters.TryGetValue("ComparisonYear", out var yearObj) && yearObj is int year)
            _comparisonYear = year;

        return base.Initialize(context, cancellationToken);
    }

    /// <inheritdoc />
    protected override SalesData Transform(SalesData input, CancellationToken cancellationToken)
    {
        // Only pass through data for the comparison year
        if (input.Year == _comparisonYear)
            return input;

        // Return null to filter out other years (handled by the pipeline)
        throw new InvalidOperationException($"Filtering out data from year {input.Year}");
    }
}

/// <summary>
///     Transform node that filters sales data for the previous year.
///     This provides the right side of the self-join.
/// </summary>
public class PreviousYearFilter : TransformNode<SalesData, SalesData>
{
    private int _comparisonYear;

    /// <summary>
    ///     Initializes a new instance of the <see cref="PreviousYearFilter" /> class.
    /// </summary>
    public PreviousYearFilter()
    {
        _comparisonYear = 2024;
    }

    /// <inheritdoc />
    public override IDataPipe<SalesData> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        // Read comparison year from context if available
        if (context.Parameters.TryGetValue("ComparisonYear", out var yearObj) && yearObj is int year)
            _comparisonYear = year;

        return base.Initialize(context, cancellationToken);
    }

    /// <inheritdoc />
    protected override SalesData Transform(SalesData input, CancellationToken cancellationToken)
    {
        // Only pass through data for the previous year
        if (input.Year == _comparisonYear - 1)
            return input;

        // Return null to filter out other years (handled by the pipeline)
        throw new InvalidOperationException($"Filtering out data from year {input.Year}");
    }
}
