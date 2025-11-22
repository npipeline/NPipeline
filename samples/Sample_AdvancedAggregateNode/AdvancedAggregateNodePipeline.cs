using NPipeline.Pipeline;
using Sample_AdvancedAggregateNode.Models;
using Sample_AdvancedAggregateNode.Nodes;

namespace Sample_AdvancedAggregateNode;

/// <summary>
///     Financial risk analysis pipeline demonstrating AdvancedAggregateNode usage for complex risk calculations.
///     This pipeline showcases sophisticated aggregation patterns with different accumulator and result types.
/// </summary>
/// <remarks>
///     This implementation follows the IPipelineDefinition pattern and demonstrates:
///     1. Real-time financial trade generation with realistic market data
///     2. Trade validation and enrichment for quality assurance
///     3. Advanced volatility calculation using running statistics (sum, sum of squares)
///     4. Value at Risk (VaR) calculation using percentile-based approach
///     5. Portfolio analytics with weighted calculations and risk metrics
///     6. Comprehensive risk reporting with formatted dashboard output
///     7. Event-time processing with watermarks and late data handling
///     8. Complex accumulator patterns separating intermediate state from final results
///     The pipeline flow:
///     TradeSource -> TradeValidationTransform -> [Branch] -> VolatilityCalculator -> RiskReportSink
///     -> [Branch] -> ValueAtRiskCalculator -> RiskReportSink
///     -> [Branch] -> PortfolioAnalyticsCalculator -> RiskReportSink
///     -> [Branch] -> RiskReportSink (for trade statistics)
/// </remarks>
public class AdvancedAggregateNodePipeline : IPipelineDefinition
{
    /// <summary>
    ///     Defines the pipeline structure by adding and connecting nodes.
    /// </summary>
    /// <param name="builder">The PipelineBuilder used to add and connect nodes.</param>
    /// <param name="context">The PipelineContext containing execution configuration and services.</param>
    /// <remarks>
    ///     This method creates a multi-branch pipeline flow that demonstrates different AdvancedAggregateNode patterns:
    ///     1. TradeSource generates realistic financial trades with various asset classes and market conditions
    ///     2. TradeValidationTransform validates trades and adds enrichment data for risk calculations
    ///     3. VolatilityCalculator calculates price volatility using complex accumulator with running statistics
    ///     4. ValueAtRiskCalculator calculates VaR using percentile-based accumulator with return distribution
    ///     5. PortfolioAnalyticsCalculator performs weighted portfolio analysis with complex state management
    ///     6. RiskReportSink displays comprehensive risk analytics in a formatted dashboard
    /// </remarks>
    public void Define(PipelineBuilder builder, PipelineContext context)
    {
        // Add the source node that generates financial trades
        var tradeSource = builder.AddSource<TradeSource, FinancialTrade>("financial-trade-source");

        // Add the validation transform to process and enrich trades
        var validationTransform = builder.AddTransform<TradeValidationTransform, FinancialTrade, ValidatedTrade>(
            "trade-validation-transform"
        );

        // Add AdvancedAggregateNode implementations for different risk calculations
        var volatilityCalculator = builder.AddAggregate<VolatilityCalculator, ValidatedTrade, string, VolatilityResult>(
            "volatility-calculator"
        );

        var valueAtRiskCalculator = builder.AddAggregate<ValueAtRiskCalculator, ValidatedTrade, string, ValueAtRiskResult>(
            "value-at-risk-calculator"
        );

        var portfolioAnalyticsCalculator = builder.AddAggregate<PortfolioAnalyticsCalculator, ValidatedTrade, string, PortfolioAnalyticsResult>(
            "portfolio-analytics-calculator"
        );

        // Add the sink for displaying risk reports (handles multiple result types)
        var riskReportSink = builder.AddSink<RiskReportSink, object>("risk-report-sink");

        // Connect the nodes in a branching flow:
        // source -> validation -> [branch] -> volatility calculator -> sink
        //                      -> [branch] -> VaR calculator -> sink
        //                      -> [branch] -> portfolio calculator -> sink
        //                      -> [branch] -> sink (for trade statistics)
        builder.Connect(tradeSource, validationTransform);

        // Branch 1: Volatility calculation by symbol
        builder.Connect(validationTransform, volatilityCalculator);
        builder.Connect(volatilityCalculator, riskReportSink);

        // Branch 2: Value at Risk calculation by portfolio
        builder.Connect(validationTransform, valueAtRiskCalculator);
        builder.Connect(valueAtRiskCalculator, riskReportSink);

        // Branch 3: Portfolio analytics by portfolio
        builder.Connect(validationTransform, portfolioAnalyticsCalculator);
        builder.Connect(portfolioAnalyticsCalculator, riskReportSink);

        // Branch 4: Direct trade statistics to sink
        builder.Connect(validationTransform, riskReportSink);
    }

    /// <summary>
    ///     Gets a description of what this pipeline demonstrates.
    /// </summary>
    /// <returns>A detailed description of the pipeline's purpose and flow.</returns>
    public static string GetDescription()
    {
        return @"AdvancedAggregateNode Pipeline Sample:

This sample demonstrates advanced financial risk analysis scenarios using NPipeline's AdvancedAggregateNode:

Key Features:
- AdvancedAggregateNode: Complex accumulator patterns with separate accumulator/result types
- Volatility Calculation: Running statistics using sum and sum of squares for variance
- Value at Risk (VaR): Percentile-based risk calculations with return distributions
- Portfolio Analytics: Weighted calculations and risk-adjusted performance metrics
- Event-Time Processing: Watermark handling for real-time financial data
- Multi-Branch Pipeline: Parallel processing of different risk calculations
- Complex State Management: Sophisticated accumulator patterns for financial analytics

Pipeline Architecture:
1. TradeSource generates realistic financial trades with:
   - Multiple asset classes (Equity, Fixed Income, Derivatives, Commodities, Forex, Crypto)
   - Various symbols with realistic pricing and market conditions
   - Trade metadata including trader, portfolio, and execution details
   - Variable frequency (2-8 trades/second) simulating market activity

2. TradeValidationTransform processes trades to:
   - Validate trade data against business rules and market regulations
   - Filter out invalid or suspicious trades
   - Enrich trades with volatility scores and risk weights
   - Provide market-adjusted pricing for accurate risk calculations

3. VolatilityCalculator performs tumbling window aggregation:
   - Groups trades by symbol using 5-minute tumbling windows
   - Maintains running sum and sum of squares for variance calculation
   - Tracks min/max prices for range analysis
   - Transforms accumulator state into formatted volatility results
   - Demonstrates complex accumulator with intermediate computational state

4. ValueAtRiskCalculator performs sliding window aggregation:
   - Groups trades by portfolio using 1-minute sliding windows every 15 seconds
   - Builds return distribution for percentile-based VaR calculations
   - Calculates VaR at 95% and 99% confidence levels
   - Computes Expected Shortfall (Conditional VaR) for tail risk
   - Demonstrates collection-based accumulator with sorting and percentile logic

5. PortfolioAnalyticsCalculator performs tumbling window aggregation:
   - Groups trades by portfolio using 5-minute tumbling windows
   - Maintains asset weights and returns for portfolio composition
   - Calculates weighted portfolio returns and volatility
   - Computes risk-adjusted performance metrics (Sharpe ratio)
   - Demonstrates complex accumulator with dictionary-based state management

6. RiskReportSink displays comprehensive risk dashboard with:
   - Formatted volatility analysis by symbol with annualized metrics
   - Value at Risk analysis by portfolio with confidence intervals
   - Portfolio analytics with risk level classification
   - Real-time risk alerts for high volatility, VaR, and low Sharpe ratios
   - Summary statistics and processing metrics

AdvancedAggregateNode Concepts Demonstrated:
- Accumulator/Result Type Separation: Different types for intermediate state vs final results
- Complex Accumulator Patterns: Tuples, dictionaries, collections for sophisticated state
- Statistical Calculations: Variance, standard deviation, percentiles, weighted averages
- Financial Risk Metrics: Volatility, VaR, Expected Shortfall, Sharpe ratio
- Window Strategy Differences: Tumbling vs sliding windows for different use cases
- Performance Considerations: Efficient state management for high-frequency trading data
- Event-Time Processing: Watermark handling for out-of-order financial data

This implementation provides a comprehensive foundation for building real-time financial risk
analysis systems with NPipeline, demonstrating the power and flexibility of AdvancedAggregateNode
for complex quantitative finance applications.";
    }
}
