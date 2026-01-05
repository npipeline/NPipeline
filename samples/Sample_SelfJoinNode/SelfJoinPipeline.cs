using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using NPipeline;
using NPipeline.DataFlow;
using NPipeline.Nodes;
using NPipeline.Pipeline;
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
    private int _comparisonYear;
    private JoinType _joinType;

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

        // Add source node for current year data (left side of join)
        var currentYearSource = builder.AddSource<CurrentYearDataSource, SalesData>("current-year-source");

        // Add source node for previous year data (right side of join)
        var previousYearSource = builder.AddSource<PreviousYearDataSource, SalesData>("previous-year-source");

        // Add self-join node using the AddSelfJoin extension method
        // This joins the same type (SalesData) with itself based on ProductId
        var selfJoin = builder.AddSelfJoin(
            currentYearSource,
            previousYearSource,
            "self-join",
            (current, previous) => new YearOverYearComparison(current, previous),
            sales => sales.ProductId,
            sales => sales.ProductId,
            _joinType
        );

        // Add aggregation node for category summaries
        var categoryAggregator = builder.AddAggregate<CategoryAggregator, YearOverYearComparison, string, CategorySummary>("category-aggregator");

        // Add sink nodes for output
        var comparisonSink = builder.AddSink<ConsoleSink<YearOverYearComparison>, YearOverYearComparison>("comparison-sink");
        var categorySink = builder.AddSink<ConsoleSink<CategorySummary>, CategorySummary>("category-sink");

        // Connect the nodes in the pipeline flow

        // Connect self-join output to aggregation
        builder.Connect(selfJoin, categoryAggregator);

        // Connect outputs to sinks
        builder.Connect(selfJoin, comparisonSink);
        builder.Connect(categoryAggregator, categorySink);
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
            JoinType.LeftOuter =>
                "Left Outer Join: All products in the current year will be included. New products (no previous year data) will show 'New Product' status.",
            JoinType.RightOuter =>
                "Right Outer Join: All products in the previous year will be included. Discontinued products (no current year data) will be included.",
            JoinType.FullOuter =>
                "Full Outer Join: All products from both years will be included, with appropriate handling for new and discontinued products.",
            _ => $"Unknown Join Type: {_joinType}",
        };
    }

    /// <summary>
    ///     Gets the comparison year being used.
    /// </summary>
    /// <returns>The comparison year.</returns>
    public int GetComparisonYear()
    {
        return _comparisonYear;
    }
}

/// <summary>
///     Source node that generates sales data for the current (comparison) year.
///     This provides the left side of the self-join.
/// </summary>
public class CurrentYearDataSource : SourceNode<SalesData>
{
    private int _comparisonYear = 2024;

    /// <summary>
    ///     Initializes a new instance of the <see cref="CurrentYearDataSource" /> class.
    /// </summary>
    public CurrentYearDataSource()
    {
    }

    /// <summary>
    ///     Generates sales data for the comparison year.
    /// </summary>
    public override IDataPipe<SalesData> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        // Read comparison year from context if available
        if (context.Parameters.TryGetValue("ComparisonYear", out var yearObj) && yearObj is int year)
            _comparisonYear = year;

        // Generate data from the underlying SalesDataSource and filter to current year
        var salesSource = new SalesDataSource();
        var allSalesData = salesSource.Initialize(context, cancellationToken);

        // Create filtered pipe that only returns current year data
        return new YearFilteredDataPipe(allSalesData, _comparisonYear);
    }
}

/// <summary>
///     Source node that generates sales data for the previous year.
///     This provides the right side of the self-join.
/// </summary>
public class PreviousYearDataSource : SourceNode<SalesData>
{
    private int _comparisonYear = 2024;

    /// <summary>
    ///     Initializes a new instance of the <see cref="PreviousYearDataSource" /> class.
    /// </summary>
    public PreviousYearDataSource()
    {
    }

    /// <summary>
    ///     Generates sales data for the previous year.
    /// </summary>
    public override IDataPipe<SalesData> Initialize(PipelineContext context, CancellationToken cancellationToken)
    {
        // Read comparison year from context if available
        if (context.Parameters.TryGetValue("ComparisonYear", out var yearObj) && yearObj is int year)
            _comparisonYear = year;

        // Generate data from the underlying SalesDataSource and filter to previous year
        var salesSource = new SalesDataSource();
        var allSalesData = salesSource.Initialize(context, cancellationToken);

        // Create filtered pipe that only returns previous year data
        return new YearFilteredDataPipe(allSalesData, _comparisonYear - 1);
    }
}

/// <summary>
///     Data pipe that filters sales data by year.
/// </summary>
public class YearFilteredDataPipe : IDataPipe<SalesData>
{
    private readonly IDataPipe<SalesData> _source;
    private readonly int _targetYear;

    public YearFilteredDataPipe(IDataPipe<SalesData> source, int targetYear)
    {
        _source = source;
        _targetYear = targetYear;
    }

    public string StreamName => $"{_source.StreamName}_filtered_{_targetYear}";

    public Type GetDataType()
    {
        return typeof(SalesData);
    }

    public IAsyncEnumerable<object?> ToAsyncEnumerable(CancellationToken cancellationToken = default)
    {
        return ReadAsyncInternal(cancellationToken);

        async IAsyncEnumerable<object?> ReadAsyncInternal([EnumeratorCancellation] CancellationToken ct)
        {
            await foreach (var item in _source.WithCancellation(ct))
            {
                if (item.Year == _targetYear)
                    yield return item;
            }
        }
    }

    public IAsyncEnumerator<SalesData> GetAsyncEnumerator(CancellationToken cancellationToken = default)
    {
        return ReadAsyncInternal(cancellationToken).GetAsyncEnumerator(cancellationToken);

        IAsyncEnumerable<SalesData> ReadAsyncInternal(CancellationToken ct)
        {
            return IterateAsync(ct);
        }

        async IAsyncEnumerable<SalesData> IterateAsync([EnumeratorCancellation] CancellationToken ct)
        {
            await foreach (var item in _source.WithCancellation(ct))
            {
                if (item.Year == _targetYear)
                    yield return item;
            }
        }
    }

    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return _source.DisposeAsync();
    }

    public async IAsyncEnumerable<SalesData> ReadAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var item in _source.WithCancellation(cancellationToken))
        {
            if (item.Year == _targetYear)
                yield return item;
        }
    }
}
